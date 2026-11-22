"""Cliente ZeroMQ REQ hacia el Servicio de Persistencia (PC3, puerto 5558)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import zmq


class ErrorPersistencia(Exception):
    """Fallo de comunicación o error reportado por PC3."""


class ClientePersistencia:
    """Encapsula un socket REQ resistente a timeouts hacia PC3.

    Un socket REQ queda inutilizable si no se recibe respuesta; por eso, ante
    ``zmq.Again`` o cualquier error de red el socket se cierra y se recrea.
    No es seguro entre hilos: use una instancia por hilo/proceso.
    """

    def __init__(self, host: str, puerto: int, timeout_ms: int = 5000,
                 contexto: Optional[zmq.Context] = None) -> None:
        """Crea el cliente y conecta el socket.

        Args:
            host: Dirección de PC3.
            puerto: Puerto REP del servicio de persistencia.
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
        """Envía una petición JSON y retorna la respuesta.

        Raises:
            ErrorPersistencia: Si hay timeout o error de red (el socket se recrea).
        """
        try:
            self.socket.send_json(peticion)
            return self.socket.recv_json()
        except zmq.ZMQError as e:
            self._crear_socket()
            raise ErrorPersistencia(f"Sin respuesta de PC3 ({self.endpoint}): {e}") from e

    def consultar(self, sql: str, parametros: Sequence[Any] = ()) -> List[List[Any]]:
        """Ejecuta un SELECT en PC3 y retorna las filas.

        Raises:
            ErrorPersistencia: Si PC3 no responde o reporta error.
        """
        resp = self._enviar({"operacion": "ejecutarConsulta", "sql": sql,
                             "parametros": list(parametros)})
        if not resp.get("exito"):
            raise ErrorPersistencia(resp.get("error", "Error desconocido en PC3"))
        return resp["resultado"]

    def ejecutar_transaccion(self, operaciones: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Ejecuta una transacción atómica en PC3.

        Args:
            operaciones: Lista de ``{"sql", "parametros", "minimoFilas"?}``.

        Returns:
            La respuesta de PC3; ``exito=False`` indica rollback (p. ej. sin cupo).

        Raises:
            ErrorPersistencia: Solo ante fallos de comunicación.
        """
        return self._enviar({"operacion": "ejecutarTransaccion", "operaciones": operaciones})

    def actualizar_checkpoint(self, id_transaccion: Optional[str], estado: str,
                              resultado_json: Optional[str] = None) -> bool:
        """Actualiza el checkpoint de una transacción (no hace nada sin id).

        Returns:
            True si PC3 confirmó la actualización; False en cualquier otro caso.
        """
        if not id_transaccion:
            return False
        try:
            resp = self._enviar({"operacion": "actualizarCheckpoint",
                                 "idTransaccion": id_transaccion, "estado": estado,
                                 "resultadoJson": resultado_json})
        except ErrorPersistencia:
            return False
        return bool(resp.get("exito"))

    def registrar_checkpoint(self, id_transaccion: str, tipo_operacion: str, id_cliente: str,
                             payload_json: str, estado: str = "RECIBIDA") -> bool:
        """Registra (o reemplaza) un checkpoint en la bitácora de PC3.

        Returns:
            True si PC3 confirmó el registro.

        Raises:
            ErrorPersistencia: Si hay fallo de comunicación.
        """
        resp = self._enviar({"operacion": "registrarCheckpoint", "idTransaccion": id_transaccion,
                             "tipoOperacion": tipo_operacion, "idCliente": id_cliente,
                             "payloadJson": payload_json, "estado": estado})
        return bool(resp.get("exito"))

    def consultar_checkpoint(self, id_transaccion: str) -> Optional[Dict[str, Any]]:
        """Consulta un checkpoint; retorna None si no existe.

        Raises:
            ErrorPersistencia: Si PC3 no responde o reporta error.
        """
        resp = self._enviar({"operacion": "consultarCheckpoint", "idTransaccion": id_transaccion})
        if not resp.get("exito"):
            raise ErrorPersistencia(resp.get("error", "Error desconocido en PC3"))
        return resp.get("resultado")

    def cerrar(self) -> None:
        """Cierra el socket."""
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None
