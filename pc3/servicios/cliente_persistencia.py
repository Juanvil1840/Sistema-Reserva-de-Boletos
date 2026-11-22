"""Cliente ZeroMQ REQ hacia el Servicio de Persistencia (puerto 5558)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import zmq


class ErrorPersistencia(Exception):
    """Fallo de comunicación o error reportado por el servicio de persistencia."""


class ClientePersistencia:
    """Encapsula un socket REQ resistente a timeouts.

    Ante ``zmq.Again`` o error de red el socket se recrea para no quedar en
    estado inválido. No es seguro entre hilos: use una instancia por hilo.
    """

    def __init__(self, host: str, puerto: int, timeout_ms: int = 5000,
                 contexto: Optional[zmq.Context] = None) -> None:
        """Crea el cliente y conecta el socket.

        Args:
            host: Dirección del servicio de persistencia.
            puerto: Puerto REP del servicio.
            timeout_ms: Timeout de envío y recepción en milisegundos.
            contexto: Contexto ZeroMQ compartido (por defecto el global).
        """
        self.endpoint = f"tcp://{host}:{puerto}"
        self.timeout_ms = timeout_ms
        self.contexto = contexto or zmq.Context.instance()
        self.socket: Optional[zmq.Socket] = None
        self._crear_socket()

    def _crear_socket(self) -> None:
        """Cierra el socket anterior (si existe) y crea uno nuevo."""
        if self.socket is not None:
            self.socket.close(linger=0)
        self.socket = self.contexto.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        self.socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.endpoint)

    def _enviar(self, peticion: Dict[str, Any]) -> Dict[str, Any]:
        """Envía una petición JSON; recrea el socket y lanza ErrorPersistencia si falla."""
        try:
            self.socket.send_json(peticion)
            return self.socket.recv_json()
        except zmq.ZMQError as e:
            self._crear_socket()
            raise ErrorPersistencia(f"Sin respuesta de persistencia ({self.endpoint}): {e}") from e

    def _exigir(self, resp: Dict[str, Any]) -> Any:
        """Retorna ``resultado`` o lanza ErrorPersistencia si ``exito`` es falso."""
        if not resp.get("exito"):
            raise ErrorPersistencia(resp.get("error", "Error desconocido en persistencia"))
        return resp.get("resultado")

    def consultar(self, sql: str, parametros: Sequence[Any] = ()) -> List[List[Any]]:
        """Ejecuta un SELECT y retorna las filas."""
        return self._exigir(self._enviar({"operacion": "ejecutarConsulta", "sql": sql,
                                          "parametros": list(parametros)}))

    def ejecutar_transaccion(self, operaciones: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Ejecuta una transacción atómica; ``exito=False`` indica rollback."""
        return self._enviar({"operacion": "ejecutarTransaccion", "operaciones": operaciones})

    def registrar_checkpoint(self, id_transaccion: str, tipo_operacion: str, id_cliente: str,
                             payload_json: str, estado: str = "RECIBIDA") -> bool:
        """Registra (o reemplaza) un checkpoint en la bitácora."""
        return bool(self._enviar({
            "operacion": "registrarCheckpoint", "idTransaccion": id_transaccion,
            "tipoOperacion": tipo_operacion, "idCliente": id_cliente,
            "payloadJson": payload_json, "estado": estado}).get("exito"))

    def actualizar_checkpoint(self, id_transaccion: Optional[str], estado: str,
                              resultado_json: Optional[str] = None) -> bool:
        """Actualiza estado/resultado de un checkpoint (sin efecto si no hay id).

        Returns:
            True si fue confirmado; False en cualquier otro caso (no lanza).
        """
        if not id_transaccion:
            return False
        try:
            return bool(self._enviar({"operacion": "actualizarCheckpoint",
                                      "idTransaccion": id_transaccion, "estado": estado,
                                      "resultadoJson": resultado_json}).get("exito"))
        except ErrorPersistencia:
            return False

    def consultar_checkpoint(self, id_transaccion: str) -> Optional[Dict[str, Any]]:
        """Retorna el checkpoint o None si no existe."""
        return self._exigir(self._enviar({"operacion": "consultarCheckpoint",
                                          "idTransaccion": id_transaccion}))

    def listar_pendientes(self) -> List[Dict[str, Any]]:
        """Lista checkpoints en estado RECIBIDA o EN_PROCESO."""
        return self._exigir(self._enviar({"operacion": "listarPendientes"}))

    def ping(self) -> Dict[str, Any]:
        """Consulta la salud del servicio de persistencia."""
        return self._exigir(self._enviar({"operacion": "ping"}))

    def cerrar(self) -> None:
        """Cierra el socket."""
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None
