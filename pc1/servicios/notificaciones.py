"""Receptor de notificaciones PUB/SUB (puerto 5557) para los clientes de PC1.

Escucha los mensajes multiparte ``[idCliente, payload_json]`` publicados por el
servicio de cancelaciones (principal o de respaldo) y despierta a los hilos
clientes que esperan confirmar su cancelación para medir la latencia total.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional, Tuple

import zmq


class ReceptorNotificaciones(threading.Thread):
    """Hilo receptor ZeroMQ SUB con soporte de conmutación (failover) en caliente."""

    def __init__(
        self,
        contexto: zmq.Context,
        host_inicial: str,
        puerto_inicial: int,
        stop_event: threading.Event,
    ) -> None:
        """Inicializa el receptor de notificaciones.

        Args:
            contexto: Contexto ZeroMQ compartido de PC1.
            host_inicial: Host del servidor principal (PC2).
            puerto_inicial: Puerto PUB de notificaciones (5557).
            stop_event: Evento de parada global de la simulación.
        """
        super().__init__(name="ReceptorNotificaciones", daemon=True)
        self.contexto = contexto
        self.host_activo = host_inicial
        self.puerto_activo = puerto_inicial
        self.stop_event = stop_event

        self._lock = threading.Lock()
        self._pendientes_reconectar: Optional[Tuple[str, int]] = None
        self._forzar_reconexion = False

        # Resultados recibidos indexados por (idCliente, idReserva)
        self._resultados: Dict[Tuple[str, str], Dict[str, Any]] = {}
        # Eventos de espera indexados por (idCliente, idReserva)
        self._eventos_espera: Dict[Tuple[str, str], threading.Event] = {}

    def registrar_espera(self, id_cliente: str, id_reserva: str) -> threading.Event:
        """Registra un ``threading.Event`` antes de enviar una cancelación.

        Si la notificación ya llegó antes de llamar a este método, el evento se
        retorna activado inmediatamente.
        """
        clave = (str(id_cliente), str(id_reserva))
        with self._lock:
            ev = self._eventos_espera.get(clave)
            if ev is None:
                ev = threading.Event()
                self._eventos_espera[clave] = ev
            if clave in self._resultados:
                ev.set()
            return ev

    def esperar_notificacion(
        self, id_cliente: str, id_reserva: str, timeout_seg: float = 5.0
    ) -> Optional[Dict[str, Any]]:
        """Espera hasta ``timeout_seg`` la notificación de cancelación y la retorna."""
        clave = (str(id_cliente), str(id_reserva))
        ev = self.registrar_espera(id_cliente, id_reserva)
        ev.wait(timeout=timeout_seg)
        with self._lock:
            self._eventos_espera.pop(clave, None)
            return self._resultados.get(clave)

    def conmutar_servidor(self, nuevo_host: str, nuevo_puerto: int) -> None:
        """Solicita de forma thread-safe reconectar el socket SUB al servidor de respaldo."""
        with self._lock:
            self._pendientes_reconectar = (nuevo_host, int(nuevo_puerto))
            self._forzar_reconexion = True

    def _crear_socket(self, host: str, puerto: int) -> zmq.Socket:
        """Crea y conecta un socket SUB suscrito a todos los tópicos."""
        sock = self.contexto.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVTIMEO, 500)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt_string(zmq.SUBSCRIBE, "")
        sock.connect(f"tcp://{host}:{puerto}")
        return sock

    def run(self) -> None:
        """Bucle principal de recepción de notificaciones PUB/SUB."""
        sock = self._crear_socket(self.host_activo, self.puerto_activo)
        try:
            while not self.stop_event.is_set():
                # Verificar si se solicitó failover o reconexión del socket SUB
                reconectar: Optional[Tuple[str, int]] = None
                with self._lock:
                    if self._forzar_reconexion and self._pendientes_reconectar is not None:
                        reconectar = self._pendientes_reconectar
                        self._pendientes_reconectar = None
                        self._forzar_reconexion = False

                if reconectar is not None:
                    nuevo_host, nuevo_puerto = reconectar
                    sock.close(linger=0)
                    self.host_activo, self.puerto_activo = nuevo_host, nuevo_puerto
                    sock = self._crear_socket(self.host_activo, self.puerto_activo)

                try:
                    partes = sock.recv_multipart()
                except zmq.Again:
                    continue
                except zmq.ZMQError:
                    break

                t_llegada = time.perf_counter()
                if len(partes) < 2:
                    continue

                try:
                    id_cli_topico = partes[0].decode("utf-8", errors="replace")
                    datos = json.loads(partes[-1].decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue

                if not isinstance(datos, dict):
                    continue

                id_cliente = str(datos.get("idCliente") or id_cli_topico)
                id_reserva = str(datos.get("idReserva") or "")
                if not id_reserva:
                    continue

                registro = {
                    "t_llegada": t_llegada,
                    "idCliente": id_cliente,
                    "idReserva": id_reserva,
                    "estado": datos.get("estado", ""),
                    "exito": bool(datos.get("exito", False)),
                    "motivo": datos.get("motivo", ""),
                }
                clave = (id_cliente, id_reserva)
                with self._lock:
                    self._resultados[clave] = registro
                    ev = self._eventos_espera.get(clave)
                    if ev is not None:
                        ev.set()
        finally:
            sock.close(linger=0)
