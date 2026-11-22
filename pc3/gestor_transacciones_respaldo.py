"""Gestor de Transacciones de respaldo (PC3) con failover Active-Passive.

Estados: PASIVO -> ACTIVANDO -> ACTIVO.

* PASIVO: solo vigila el heartbeat del GT principal (REQ -> 5569). No hace bind
  en 5555/5556/5557/5559/5560, por lo que puede convivir con PC2 en localhost.
* ACTIVANDO: lanza las réplicas RMC y el worker de cancelaciones de respaldo,
  abre los coordinadores (ROUTER 5555 <-> DEALER 5560 con idempotencia por
  checkpoint y ROUTER 5556 -> PUSH 5559, con las mismas reglas de frames que
  pc2/gestor_transacciones.py) y recupera los checkpoints pendientes.
* ACTIVO: atiende tráfico hasta recibir SIGINT/SIGTERM.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import zmq

from servicios import procesos
from servicios.cliente_persistencia import ClientePersistencia, ErrorPersistencia
from servicios.configuracion import cargar_respaldo, instalar_senales, log, usar_utf8

ETIQUETA = "GT-RESPALDO"
ALIAS_ACCIONES = {
    "consultar": "consultar", "consultareventos": "consultar",
    "reservar": "reservar", "solicitarreserva": "reservar",
    "modificar": "modificar", "modificarreserva": "modificar",
}
TIPO_CHECKPOINT = {"reservar": "RESERVAR", "modificar": "MODIFICAR"}


def _json(obj: Dict[str, Any]) -> bytes:
    """Serializa un diccionario a bytes JSON UTF-8."""
    return json.dumps(obj).encode("utf-8")


class GestorRespaldo:
    """Nodo coordinador de respaldo con monitoreo de heartbeat y failover."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Guarda la configuración y crea el contexto ZeroMQ compartido.

        Args:
            config: Sección ``respaldo`` de ``pc3_config.json``.
        """
        self.cfg = config
        self.contexto = zmq.Context()
        self.stop_event = threading.Event()
        self.estado = "PASIVO"
        self.subprocesos: List[Any] = []
        self.hilos: List[threading.Thread] = []

    def _nuevo_cliente_bd(self) -> ClientePersistencia:
        """Crea un cliente de persistencia propio (no es seguro entre hilos)."""
        return ClientePersistencia(self.cfg["host_persistencia"], self.cfg["puerto_persistencia"],
                                   self.cfg.get("timeout_persistencia_ms", 5000), self.contexto)

    # ------------------------------------------------------------------
    # Estado PASIVO: monitoreo de heartbeat
    # ------------------------------------------------------------------
    def _ping_principal(self, sock: Optional[zmq.Socket]) -> tuple[bool, Optional[zmq.Socket]]:
        """Envía un ping al GT principal.

        Returns:
            (respondió, socket): el socket se recrea (None) si hubo timeout o error.
        """
        timeout = int(self.cfg["timeout_heartbeat_ms"])
        try:
            if sock is None:
                sock = self.contexto.socket(zmq.REQ)
                sock.setsockopt(zmq.RCVTIMEO, timeout)
                sock.setsockopt(zmq.SNDTIMEO, timeout)
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(f"tcp://{self.cfg['host_gt_principal']}:"
                             f"{self.cfg['puerto_heartbeat_principal']}")
            sock.send_json({"tipo": "ping"})
            return sock.recv_json().get("tipo") == "pong", sock
        except (zmq.ZMQError, ValueError):
            if sock is not None:
                sock.close(linger=0)
            return False, None

    def _vigilar_principal(self) -> bool:
        """Bucle de heartbeat. Retorna True si debe producirse el failover."""
        umbral = int(self.cfg["fallos_consecutivos_para_failover"])
        intervalo = float(self.cfg["intervalo_heartbeat_seg"])
        gracia = float(self.cfg.get("periodo_gracia_seg", 15))
        inicio = time.monotonic()
        fallos, visto, sock = 0, False, None
        log(ETIQUETA, f"Estado PASIVO: vigilando GT principal "
                      f"{self.cfg['host_gt_principal']}:{self.cfg['puerto_heartbeat_principal']}")
        try:
            while not self.stop_event.is_set():
                t0 = time.monotonic()
                ok, sock = self._ping_principal(sock)
                if ok:
                    if fallos:
                        log(ETIQUETA, "GT Principal volvió a responder; contador reiniciado")
                    fallos, visto = 0, True
                elif visto or (time.monotonic() - inicio) > gracia:
                    fallos += 1
                    log(ETIQUETA, f"Heartbeat sin respuesta ({fallos}/{umbral})")
                    if fallos >= umbral:
                        return True
                self.stop_event.wait(max(0.0, intervalo - (time.monotonic() - t0)))
        finally:
            if sock is not None:
                sock.close(linger=0)
        return False

    # ------------------------------------------------------------------
    # Coordinador síncrono (ROUTER 5555 <-> DEALER 5560)
    # ------------------------------------------------------------------
    def _preparar_escritura(self, bd: ClientePersistencia, peticion: Dict[str, Any],
                            accion: str) -> Optional[Dict[str, Any]]:
        """Aplica idempotencia y registra EN_PROCESO.

        Returns:
            Resultado guardado (con ``reintentoIdempotente``) si ya estaba COMPLETADA;
            None si debe reenviarse a una réplica RMC.
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
            ruta_backend = ruta if b"" in ruta else ruta + [b""]
            backend.send_multipart(ruta_backend + [_json(peticion)], zmq.NOBLOCK)
            log(ETIQUETA, f"{accion} reenviado a RMC (tx={peticion.get('idTransaccion', '-')})")
        except zmq.Again:
            responder({"exito": False, "error": "Sin réplicas RMC disponibles"})
        except ErrorPersistencia as e:
            responder({"exito": False, "error": f"Persistencia no disponible: {e}"})

    # ------------------------------------------------------------------
    # Canal asíncrono (ROUTER 5556 -> PUSH 5559)
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
            push.connect(f"tcp://localhost:{self.cfg['puerto_cancelaciones_push']}")
            bd = self._nuevo_cliente_bd()
            log(ETIQUETA, f"Canal asíncrono: ROUTER {self.cfg['puerto_async']} -> PUSH "
                          f"{self.cfg['puerto_cancelaciones_push']}")
            while not self.stop_event.is_set():
                try:
                    frames = router.recv_multipart()
                except zmq.Again:
                    continue
                ruta = frames[:-1]

                def responder(obj: Dict[str, Any], ruta: List[bytes] = ruta) -> None:
                    router.send_multipart(ruta + [_json(obj)])

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
    # Activación y recuperación de checkpoints
    # ------------------------------------------------------------------
    def _pedir_interno(self, puerto: int, mensaje: Dict[str, Any],
                       reintentos: int = 40) -> Optional[Dict[str, Any]]:
        """Envía una petición a los coordinadores propios (REQ a localhost).

        Reintenta mientras las réplicas RMC aún no estén conectadas.
        """
        for _ in range(reintentos):
            if self.stop_event.is_set():
                return None
            sock = self.contexto.socket(zmq.REQ)
            sock.setsockopt(zmq.RCVTIMEO, 10000)
            sock.setsockopt(zmq.SNDTIMEO, 5000)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(f"tcp://localhost:{puerto}")
            try:
                sock.send_json(mensaje)
                resp = sock.recv_json()
                if "Sin réplicas" not in str(resp.get("error", "")):
                    return resp
            except zmq.ZMQError:
                pass
            finally:
                sock.close(linger=0)
            self.stop_event.wait(0.5)
        return None

    def recuperar_pendientes(self) -> None:
        """Reprocesa los checkpoints RECIBIDA/EN_PROCESO dejados por el GT principal."""
        bd = self._nuevo_cliente_bd()
        try:
            pendientes = bd.listar_pendientes()
            log(ETIQUETA, f"Recuperación: {len(pendientes)} checkpoint(s) pendiente(s)")
            for cp in pendientes:
                id_tx, tipo = cp["idTransaccion"], str(cp.get("tipoOperacion", "")).upper()
                try:
                    payload = json.loads(cp.get("payloadJson") or "{}")
                    payload["idTransaccion"] = id_tx
                    if tipo == "CANCELAR":
                        # Re-encolar por el canal asíncrono para liberar cupos y notificar
                        resp = self._pedir_interno(self.cfg["puerto_async"], payload)
                    elif tipo in ("RESERVAR", "MODIFICAR"):
                        # Reprocesar por el flujo síncrono; deja el checkpoint COMPLETADA/FALLIDA
                        payload["accion"] = tipo.lower()
                        resp = self._pedir_interno(self.cfg["puerto_sync"], payload)
                    else:
                        log(ETIQUETA, f"Checkpoint {id_tx}: tipo '{tipo}' no recuperable")
                        continue
                    log(ETIQUETA, f"Recuperado {id_tx} ({tipo}): {resp}")
                except Exception as e:  # una falla no debe frenar las demás
                    log(ETIQUETA, f"Error recuperando {id_tx}: {type(e).__name__}: {e}")
        except ErrorPersistencia as e:
            log(ETIQUETA, f"No se pudieron listar pendientes: {e}")
        finally:
            bd.cerrar()

    def _activar(self) -> None:
        """Transición ACTIVANDO -> ACTIVO."""
        self.estado = "ACTIVANDO"
        log(ETIQUETA, "⚠ GT Principal no responde. Asumiendo rol ACTIVO...")
        n = int(self.cfg.get("num_replicas_rmc", 2))
        for i in range(1, n + 1):
            self.subprocesos.append(procesos.lanzar("servicio_rmc_respaldo.py",
                                                    "--worker-id", f"RMC-RESPALDO-{i}"))
        self.subprocesos.append(procesos.lanzar("servicio_cancelaciones_respaldo.py"))
        for nombre, destino in (("balanceador", self._hilo_balanceador), ("async", self._hilo_async)):
            h = threading.Thread(target=destino, name=nombre, daemon=True)
            h.start()
            self.hilos.append(h)
        self.recuperar_pendientes()
        self.estado = "ACTIVO"
        log(ETIQUETA, "Estado ACTIVO: atendiendo tráfico de clientes")

    def run(self) -> None:
        """Ciclo de vida completo: vigilar, conmutar y atender hasta la parada."""
        try:
            if self._vigilar_principal():
                self._activar()
                while not self.stop_event.wait(0.5):
                    pass
        finally:
            self.stop_event.set()
            for h in self.hilos:
                h.join(timeout=5)
            procesos.detener(self.subprocesos)
            self.contexto.term()
            log(ETIQUETA, "Detenido limpiamente.")


def main() -> None:
    """Punto de entrada del gestor de respaldo."""
    usar_utf8()
    config = cargar_respaldo()
    if os.environ.get("GT_PRINCIPAL_HOST"):  # permite apuntar al contenedor de PC2
        config["host_gt_principal"] = os.environ["GT_PRINCIPAL_HOST"]
    gestor = GestorRespaldo(config)
    instalar_senales(gestor.stop_event)
    gestor.run()


if __name__ == "__main__":
    main()
