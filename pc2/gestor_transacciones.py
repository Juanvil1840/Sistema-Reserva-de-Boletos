"""Gestor de Transacciones (coordinador y balanceador) de la Capa de Negocio PC2.

Hilos (cada uno crea y cierra sus propios sockets sobre un único contexto):
  1. Balanceador síncrono: ROUTER 5555 <-> DEALER 5560 (réplicas RMC), con
     checkpointing e idempotencia para ``reservar`` y ``modificar``.
  2. Canal asíncrono: ROUTER 5556 -> PUSH 5559 (worker de cancelaciones).
  3. Heartbeat: REP 5569.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import zmq

from servicios.cliente_persistencia import ClientePersistencia, ErrorPersistencia
from servicios.configuracion import cargar_seccion, instalar_senales, log

ETIQUETA = "GESTOR"
ALIAS_ACCIONES = {
    "consultar": "consultar", "consultareventos": "consultar",
    "reservar": "reservar", "solicitarreserva": "reservar",
    "modificar": "modificar", "modificarreserva": "modificar",
}
TIPO_CHECKPOINT = {"reservar": "RESERVAR", "modificar": "MODIFICAR"}


def _json(obj: Dict[str, Any]) -> bytes:
    """Serializa un diccionario a bytes JSON UTF-8."""
    return json.dumps(obj).encode("utf-8")


class GestorTransacciones:
    """Coordinador multihilo: balancea peticiones, hace checkpoint y responde heartbeats."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Guarda la configuración y crea el contexto ZeroMQ compartido.

        Args:
            config: Sección ``gestor`` de ``pc2_config.json``.
        """
        self.cfg = config
        self.contexto = zmq.Context()
        self.stop_event = threading.Event()
        self.inicio = time.time()

    def _nuevo_cliente_bd(self) -> ClientePersistencia:
        """Crea un cliente de persistencia propio (no es seguro entre hilos)."""
        return ClientePersistencia(self.cfg["host_persistencia"], self.cfg["puerto_persistencia"],
                                   self.cfg.get("timeout_persistencia_ms", 5000), self.contexto)

    # ------------------------------------------------------------------
    # Hilo 1: balanceador síncrono
    # ------------------------------------------------------------------
    def _preparar_escritura(self, bd: ClientePersistencia, peticion: Dict[str, Any],
                            accion: str) -> Optional[Dict[str, Any]]:
        """Aplica idempotencia y registra el checkpoint EN_PROCESO.

        Returns:
            Respuesta guardada (con ``reintentoIdempotente``) si la transacción ya
            estaba COMPLETADA; None si debe reenviarse a una réplica RMC.
        """
        id_tx = peticion["idTransaccion"]
        cp = bd.consultar_checkpoint(id_tx)
        if cp and cp.get("estado") == "COMPLETADA" and cp.get("resultadoJson"):
            guardado = json.loads(cp["resultadoJson"])
            guardado["reintentoIdempotente"] = True
            log(ETIQUETA, f"Reintento idempotente {id_tx}: se devuelve resultado guardado")
            return guardado
        bd.registrar_checkpoint(id_tx, TIPO_CHECKPOINT[accion], str(peticion.get("idCliente", "")),
                                json.dumps(peticion), "EN_PROCESO")
        return None

    def _hilo_balanceador(self) -> None:
        """Proxy ROUTER(5555) <-> DEALER(5560) con checkpointing de escrituras."""
        frontend = self.contexto.socket(zmq.ROUTER)
        backend = self.contexto.socket(zmq.DEALER)
        bd: Optional[ClientePersistencia] = None
        try:
            frontend.setsockopt(zmq.LINGER, 0)
            backend.setsockopt(zmq.LINGER, 0)
            frontend.bind(f"tcp://*:{self.cfg['puerto_sync']}")
            backend.bind(f"tcp://*:{self.cfg['puerto_backend_rmc']}")
            bd = self._nuevo_cliente_bd()
            poller = zmq.Poller()
            poller.register(frontend, zmq.POLLIN)
            poller.register(backend, zmq.POLLIN)
            log(ETIQUETA, f"Balanceador: ROUTER {self.cfg['puerto_sync']} <-> "
                          f"DEALER {self.cfg['puerto_backend_rmc']}")
            while not self.stop_event.is_set():
                eventos = dict(poller.poll(500))
                if backend in eventos:
                    # Respuesta de una réplica: se reenvía intacta al cliente
                    frontend.send_multipart(backend.recv_multipart())
                if frontend in eventos:
                    self._atender_frontend(frontend, backend, bd)
        except zmq.ZMQError as e:
            log(ETIQUETA, f"Error fatal en balanceador: {e}")
            self.stop_event.set()
        finally:
            frontend.close(linger=0)
            backend.close(linger=0)
            if bd:
                bd.cerrar()

    def _atender_frontend(self, frontend: zmq.Socket, backend: zmq.Socket,
                          bd: ClientePersistencia) -> None:
        """Procesa una petición recibida por el ROUTER síncrono."""
        frames: List[bytes] = frontend.recv_multipart()
        ruta, cuerpo = frames[:-1], frames[-1]

        def responder(obj: Dict[str, Any]) -> None:
            frontend.send_multipart(ruta + [_json(obj)])

        try:
            peticion = json.loads(cuerpo.decode("utf-8"))
            if not isinstance(peticion, dict):
                raise ValueError("no es un objeto JSON")
        except (ValueError, UnicodeDecodeError) as e:
            responder({"exito": False, "error": f"JSON inválido: {e}"})
            return
        accion = ALIAS_ACCIONES.get(str(peticion.get("accion") or peticion.get("operacion")
                                        or "").strip().lower())
        if accion is None:
            responder({"exito": False, "error": "Acción desconocida"})
            return
        peticion["accion"] = accion
        try:
            if accion in TIPO_CHECKPOINT:
                peticion["idTransaccion"] = peticion.get("idTransaccion") or str(uuid.uuid4())
                guardado = self._preparar_escritura(bd, peticion, accion)
                if guardado is not None:
                    responder(guardado)
                    return
            # Un DEALER sin delimitador vacío necesita uno para que el REP de la réplica lo procese
            ruta_backend = ruta if b"" in ruta else ruta + [b""]
            backend.send_multipart(ruta_backend + [_json(peticion)], zmq.NOBLOCK)
            log(ETIQUETA, f"{accion} reenviado a RMC (tx={peticion.get('idTransaccion', '-')})")
        except zmq.Again:
            responder({"exito": False, "error": "Sin réplicas RMC disponibles"})
        except ErrorPersistencia as e:
            responder({"exito": False, "error": f"Persistencia no disponible: {e}"})

    # ------------------------------------------------------------------
    # Hilo 2: canal asíncrono de cancelaciones
    # ------------------------------------------------------------------
    def _hilo_async(self) -> None:
        """ROUTER(5556) -> PUSH(5559): encola cancelaciones y acusa recibo."""
        router = self.contexto.socket(zmq.ROUTER)
        push = self.contexto.socket(zmq.PUSH)
        bd: Optional[ClientePersistencia] = None
        try:
            router.setsockopt(zmq.RCVTIMEO, 1000)
            router.setsockopt(zmq.LINGER, 0)
            push.setsockopt(zmq.SNDTIMEO, 2000)
            push.setsockopt(zmq.LINGER, 0)
            router.bind(f"tcp://*:{self.cfg['puerto_async']}")
            push.connect(f"tcp://{self.cfg['host_cancelaciones']}:"
                         f"{self.cfg['puerto_cancelaciones_push']}")
            bd = self._nuevo_cliente_bd()
            log(ETIQUETA, f"Canal asíncrono: ROUTER {self.cfg['puerto_async']} -> PUSH "
                          f"{self.cfg['host_cancelaciones']}:{self.cfg['puerto_cancelaciones_push']}")
            while not self.stop_event.is_set():
                try:
                    frames = router.recv_multipart()
                except zmq.Again:
                    continue
                ruta = frames[:-1]
                responder = lambda obj: router.send_multipart(ruta + [_json(obj)])  # noqa: E731
                try:
                    peticion = json.loads(frames[-1].decode("utf-8"))
                    if not isinstance(peticion, dict) or not peticion.get("idReserva"):
                        raise ValueError("se requiere un objeto JSON con idReserva")
                except (ValueError, UnicodeDecodeError) as e:
                    responder({"exito": False, "error": f"Solicitud inválida: {e}"})
                    continue
                id_tx = peticion.get("idTransaccion") or str(uuid.uuid4())
                tarea = {"accion": "cancelar", "idReserva": peticion["idReserva"],
                         "idCliente": peticion.get("idCliente"), "idTransaccion": id_tx}
                try:
                    bd.registrar_checkpoint(id_tx, "CANCELAR", str(peticion.get("idCliente", "")),
                                            json.dumps(tarea), "RECIBIDA")
                    push.send_json(tarea)
                except ErrorPersistencia as e:
                    responder({"exito": False, "error": f"Persistencia no disponible: {e}"})
                    continue
                except zmq.Again:
                    bd.actualizar_checkpoint(id_tx, "FALLIDA", json.dumps({"error": "PUSH sin destino"}))
                    responder({"exito": False, "error": "Worker de cancelaciones no disponible"})
                    continue
                log(ETIQUETA, f"Cancelación encolada (tx={id_tx}, reserva={peticion['idReserva']})")
                responder({"exito": True, "estado": "EN_PROCESO", "idTransaccion": id_tx,
                           "mensaje": "Solicitud de cancelación aceptada y encolada"})
        except zmq.ZMQError as e:
            log(ETIQUETA, f"Error fatal en canal asíncrono: {e}")
            self.stop_event.set()
        finally:
            router.close(linger=0)
            push.close(linger=0)
            if bd:
                bd.cerrar()

    # ------------------------------------------------------------------
    # Hilo 3: heartbeat
    # ------------------------------------------------------------------
    def _hilo_heartbeat(self) -> None:
        """REP(5569): responde ``ping`` con ``pong`` y datos de estado."""
        rep = self.contexto.socket(zmq.REP)
        try:
            rep.setsockopt(zmq.RCVTIMEO, 1000)
            rep.setsockopt(zmq.LINGER, 0)
            rep.bind(f"tcp://*:{self.cfg['puerto_heartbeat']}")
            log(ETIQUETA, f"Heartbeat en tcp://*:{self.cfg['puerto_heartbeat']}")
            while not self.stop_event.is_set():
                try:
                    msg = rep.recv_json()
                except zmq.Again:
                    continue
                except ValueError:
                    rep.send_json({"exito": False, "error": "JSON inválido"})
                    continue
                if isinstance(msg, dict) and msg.get("tipo") == "ping":
                    rep.send_json({"tipo": "pong", "rol": "PRINCIPAL", "estado": "ACTIVO",
                                   "uptime_s": round(time.time() - self.inicio, 1),
                                   "timestamp": datetime.now().isoformat(timespec="seconds")})
                else:
                    rep.send_json({"exito": False, "error": "Mensaje no soportado"})
        except zmq.ZMQError as e:
            log(ETIQUETA, f"Error fatal en heartbeat: {e}")
            self.stop_event.set()
        finally:
            rep.close(linger=0)

    # ------------------------------------------------------------------
    def run(self) -> None:
        """Lanza los tres hilos y espera hasta ``stop_event``; cierra el contexto."""
        hilos = [threading.Thread(target=t, name=n, daemon=True) for n, t in (
            ("balanceador", self._hilo_balanceador), ("async", self._hilo_async),
            ("heartbeat", self._hilo_heartbeat))]
        for h in hilos:
            h.start()
        try:
            while not self.stop_event.wait(0.5):
                pass
        finally:
            self.stop_event.set()
            for h in hilos:
                h.join(timeout=5)
            self.contexto.term()
            log(ETIQUETA, "Detenido limpiamente.")


def main() -> None:
    """Punto de entrada del Gestor de Transacciones."""
    gestor = GestorTransacciones(cargar_seccion("gestor"))
    instalar_senales(gestor.stop_event)
    gestor.run()


if __name__ == "__main__":
    main()
