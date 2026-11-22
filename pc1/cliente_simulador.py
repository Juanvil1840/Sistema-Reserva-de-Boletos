"""Simulador multihilo de consumidores (PC1) del Sistema de Reserva de Boletos.

Ejecuta clientes concurrentes (``CLI-01..CLI-N``) que generan carga sobre las 4
operaciones del sistema (Consulta, Reserva y Modificación síncronas por ``5555``
y Cancelación asíncrona por ``5556`` con confirmación PUB/SUB por ``5557``).
Incluye conmutación automática (failover) hacia el Gestor de Respaldo en PC3
conservando el ``idTransaccion`` para idempotencia, y exportación de métricas a CSV.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import zmq

from servicios.metricas import RecolectorMetricas
from servicios.notificaciones import ReceptorNotificaciones

DIRECTORIO_PC1 = os.path.dirname(os.path.abspath(__file__))
CONFIG_POR_DEFECTO = os.path.join(DIRECTORIO_PC1, "config", "pc1_config.json")


def usar_utf8() -> None:
    """Configura la salida estándar en UTF-8 para evitar errores de codificación en Windows."""
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def log(etiqueta: str, mensaje: str) -> None:
    """Imprime un mensaje con marca de tiempo."""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] [{etiqueta}] {mensaje}", flush=True)


def cargar_config(ruta: Optional[str] = None) -> Dict[str, Any]:
    """Carga el JSON de configuración de PC1."""
    ruta_final = ruta or os.environ.get("PC1_CONFIG", CONFIG_POR_DEFECTO)
    with open(ruta_final, encoding="utf-8") as f:
        return json.load(f)


class ControladorFailover:
    """Coordina de forma thread-safe el nodo activo (PRINCIPAL vs. RESPALDO) entre clientes."""

    def __init__(
        self,
        servidor_principal: Dict[str, Any],
        servidor_respaldo: Dict[str, Any],
        receptor_notif: ReceptorNotificaciones,
    ) -> None:
        """Inicializa apuntando al servidor principal (PC2)."""
        self._lock = threading.Lock()
        self.principal = dict(servidor_principal)
        self.respaldo = dict(servidor_respaldo)
        self.receptor_notif = receptor_notif
        self._nodo = "PRINCIPAL"
        self._epoch = 0

    def obtener_destino(self) -> Tuple[str, Dict[str, Any], int]:
        """Retorna ``(nodo_actual, dict_servidor, epoch_failover)``."""
        with self._lock:
            srv = self.principal if self._nodo == "PRINCIPAL" else self.respaldo
            return self._nodo, dict(srv), self._epoch

    def conmutar_a_respaldo(self) -> Tuple[str, Dict[str, Any], int]:
        """Conmuta el tráfico hacia ``servidor_respaldo`` (PC3) y reconecta el SUB 5557."""
        with self._lock:
            self._epoch += 1
            ya_en_respaldo = self._nodo == "RESPALDO"
            self._nodo = "RESPALDO"
            srv = dict(self.respaldo)
            epoch = self._epoch

        self.receptor_notif.conmutar_servidor(srv["host"], int(srv["puerto_pub"]))
        if not ya_en_respaldo:
            log(
                "FAILOVER",
                f"Conmutando tráfico de clientes hacia servidor de respaldo "
                f"({srv['host']}:{srv['puerto_sync']})",
            )
        return "RESPALDO", srv, epoch


class HiloCliente(threading.Thread):
    """Hilo que simula un consumidor individual ejecutando las 4 operaciones del SRB."""

    def __init__(
        self,
        id_cliente: str,
        contexto: zmq.Context,
        controlador: ControladorFailover,
        receptor: ReceptorNotificaciones,
        metricas: RecolectorMetricas,
        cfg_sim: Dict[str, Any],
        stop_event: threading.Event,
    ) -> None:
        """Inicializa el estado local del cliente."""
        super().__init__(name=id_cliente, daemon=True)
        self.id_cliente = id_cliente
        self.contexto = contexto
        self.controlador = controlador
        self.receptor = receptor
        self.metricas = metricas
        self.cfg_sim = cfg_sim
        self.stop_event = stop_event

        self.timeout_ms = int(cfg_sim.get("timeout_ms", 3000))
        self.reintentos_max = int(cfg_sim.get("reintentos_max", 2))
        self.mes_consulta = int(cfg_sim.get("mes_consulta", 10))
        self.rpm = max(1, int(cfg_sim.get("solicitudes_por_minuto_por_cliente", 24)))
        self.duracion_seg = float(cfg_sim.get("duracion_segundos", 30))

        porc = cfg_sim.get("porcentajes", {})
        self.pesos = [
            float(porc.get("consultar", 40)),
            float(porc.get("reservar", 30)),
            float(porc.get("modificar", 15)),
            float(porc.get("cancelar", 15)),
        ]

        self.sock_req: Optional[zmq.Socket] = None
        self.sock_dealer: Optional[zmq.Socket] = None
        self._epoch_conectado = -1
        self._servidor_actual: Dict[str, Any] = {}

        # Lista local de reservas activas: [{"idReserva", "idEvento", "cantidadPuestos"}]
        self.reservas_activas: List[Dict[str, Any]] = []
        self.eventos_disponibles: List[str] = [f"E{i:03d}" for i in range(1, 51)]

    # ------------------------------------------------------------------
    # Gestión de sockets ZeroMQ
    # ------------------------------------------------------------------
    def _cerrar_sockets(self) -> None:
        """Cierra los sockets REQ y DEALER del hilo."""
        for s in (self.sock_req, self.sock_dealer):
            if s is not None:
                s.close(linger=0)
        self.sock_req = None
        self.sock_dealer = None

    def _asegurar_sockets(self, forzar: bool = False) -> Tuple[str, Dict[str, Any]]:
        """Verifica si hubo failover y abre/recrea los sockets REQ (5555) y DEALER (5556)."""
        nodo, srv, epoch = self.controlador.obtener_destino()
        if forzar or self.sock_req is None or self.sock_dealer is None or epoch != self._epoch_conectado:
            self._cerrar_sockets()
            self.sock_req = self.contexto.socket(zmq.REQ)
            self.sock_req.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            self.sock_req.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            self.sock_req.setsockopt(zmq.LINGER, 0)
            self.sock_req.connect(f"tcp://{srv['host']}:{srv['puerto_sync']}")

            self.sock_dealer = self.contexto.socket(zmq.DEALER)
            self.sock_dealer.setsockopt(zmq.IDENTITY, f"{self.id_cliente}-{uuid.uuid4().hex[:6]}".encode("utf-8"))
            self.sock_dealer.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            self.sock_dealer.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            self.sock_dealer.setsockopt(zmq.LINGER, 0)
            self.sock_dealer.connect(f"tcp://{srv['host']}:{srv['puerto_async']}")

            self._epoch_conectado = epoch
            self._servidor_actual = srv
        return nodo, srv

    # ------------------------------------------------------------------
    # Envío con reintento y failover conservando idTransaccion
    # ------------------------------------------------------------------
    def _enviar_sync(self, peticion: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, bool, float]:
        """Envía una petición por REQ (5555) con reintentos y failover a PC3.

        Returns:
            ``(respuesta_dict, nodo_destino, hubo_failover, latencia_ms)``
        """
        t0 = time.perf_counter()
        hubo_failover = False
        nodo, _ = self._asegurar_sockets()
        intentos_totales = self.reintentos_max + 2

        for intento in range(intentos_totales):
            if self.stop_event.is_set():
                break
            nodo, _ = self._asegurar_sockets()
            try:
                assert self.sock_req is not None
                self.sock_req.send_json(peticion)
                resp = self.sock_req.recv_json()
                # Si el respaldo aún está levantando sus réplicas RMC, reintentar brevemente
                if isinstance(resp, dict) and "Sin réplicas" in str(resp.get("error", "")):
                    time.sleep(0.4)
                    continue
                lat_ms = (time.perf_counter() - t0) * 1000.0
                if isinstance(resp, dict) and str(resp.get("procesadoPor", "")).startswith("RMC-RESPALDO"):
                    nodo = "RESPALDO"
                return resp, nodo, hubo_failover, lat_ms
            except zmq.ZMQError:
                hubo_failover = True
                if intento + 1 >= self.reintentos_max:
                    self.controlador.conmutar_a_respaldo()
                self._asegurar_sockets(forzar=True)
                time.sleep(0.3)

        lat_ms = (time.perf_counter() - t0) * 1000.0
        return None, nodo, hubo_failover, lat_ms

    def _enviar_async(self, peticion: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str, bool, float]:
        """Envía una solicitud de cancelación por DEALER (5556) con reintentos y failover."""
        t0 = time.perf_counter()
        hubo_failover = False
        nodo, _ = self._asegurar_sockets()
        intentos_totales = self.reintentos_max + 2

        for intento in range(intentos_totales):
            if self.stop_event.is_set():
                break
            nodo, _ = self._asegurar_sockets()
            try:
                assert self.sock_dealer is not None
                self.sock_dealer.send_json(peticion)
                partes = self.sock_dealer.recv_multipart()
                resp = json.loads(partes[-1].decode("utf-8"))
                lat_ms = (time.perf_counter() - t0) * 1000.0
                return resp, nodo, hubo_failover, lat_ms
            except (zmq.ZMQError, ValueError):
                hubo_failover = True
                if intento + 1 >= self.reintentos_max:
                    self.controlador.conmutar_a_respaldo()
                self._asegurar_sockets(forzar=True)
                time.sleep(0.3)

        lat_ms = (time.perf_counter() - t0) * 1000.0
        return None, nodo, hubo_failover, lat_ms

    # ------------------------------------------------------------------
    # Las 4 operaciones de negocio del cliente
    # ------------------------------------------------------------------
    def op_consultar(self) -> None:
        """Ejecuta una consulta de eventos por mes."""
        id_tx = f"CON-{uuid.uuid4().hex[:10]}"
        peticion = {"accion": "consultar", "mes": self.mes_consulta}
        resp, nodo, fo, lat_ms = self._enviar_sync(peticion)
        exito = bool(resp and resp.get("exito"))
        eventos = resp.get("eventos", []) if isinstance(resp, dict) else []
        if exito and eventos:
            con_cupo = [e[0] for e in eventos if isinstance(e, list) and len(e) >= 6 and int(e[5]) > 5]
            if con_cupo:
                self.eventos_disponibles = con_cupo
        detalle = f"eventos={len(eventos)}" if exito else str((resp or {}).get("error", "timeout"))
        self.metricas.registrar(self.id_cliente, id_tx, "consultar", nodo, fo, exito, lat_ms, detalle)

    def op_reservar(self) -> bool:
        """Ejecuta una reserva síncrona con ``idTransaccion`` único."""
        id_tx = str(uuid.uuid4())
        id_evento = random.choice(self.eventos_disponibles)
        puestos = random.randint(1, 3)
        peticion = {
            "accion": "reservar",
            "idCliente": self.id_cliente,
            "idEvento": id_evento,
            "numPuestos": puestos,
            "idTransaccion": id_tx,
        }
        resp, nodo, fo, lat_ms = self._enviar_sync(peticion)
        exito = bool(resp and resp.get("exito") and resp.get("idReserva"))
        if exito and isinstance(resp, dict):
            self.reservas_activas.append(
                {
                    "idReserva": resp["idReserva"],
                    "idEvento": id_evento,
                    "cantidadPuestos": puestos,
                }
            )
            idem = " (idempotente)" if resp.get("reintentoIdempotente") else ""
            detalle = f"idReserva={resp['idReserva']} ev={id_evento} puestos={puestos}{idem}"
        else:
            detalle = str((resp or {}).get("motivo") or (resp or {}).get("error") or "timeout")
        self.metricas.registrar(self.id_cliente, id_tx, "reservar", nodo, fo, exito, lat_ms, detalle)
        return exito

    def op_modificar(self) -> None:
        """Modifica una reserva activa (si no tiene ninguna, crea una reserva primero)."""
        if not self.reservas_activas:
            if not self.op_reservar():
                return
        reserva = random.choice(self.reservas_activas)
        id_tx = str(uuid.uuid4())
        # Alternar entre cambio de cantidad en el mismo evento o cambio de evento
        if random.random() < 0.5:
            nuevo_ev = reserva["idEvento"]
        else:
            candidatos = [e for e in self.eventos_disponibles if e != reserva["idEvento"]]
            nuevo_ev = random.choice(candidatos) if candidatos else reserva["idEvento"]
        nuevos_puestos = random.choice([1, 2, 3, 4])
        if nuevo_ev == reserva["idEvento"] and nuevos_puestos == reserva["cantidadPuestos"]:
            nuevos_puestos = (nuevos_puestos % 4) + 1

        peticion = {
            "accion": "modificar",
            "idReserva": reserva["idReserva"],
            "idCliente": self.id_cliente,
            "nuevoIdEvento": nuevo_ev,
            "nuevosPuestos": nuevos_puestos,
            "idTransaccion": id_tx,
        }
        resp, nodo, fo, lat_ms = self._enviar_sync(peticion)
        exito = bool(resp and resp.get("exito"))
        if exito:
            reserva["idEvento"] = nuevo_ev
            reserva["cantidadPuestos"] = nuevos_puestos
            detalle = f"idReserva={reserva['idReserva']} -> ev={nuevo_ev} puestos={nuevos_puestos}"
        else:
            detalle = str((resp or {}).get("motivo") or (resp or {}).get("error") or "timeout")
        self.metricas.registrar(self.id_cliente, id_tx, "modificar", nodo, fo, exito, lat_ms, detalle)

    def op_cancelar(self) -> None:
        """Cancela asíncronamente una reserva (5556) y espera su notificación PUB/SUB (5557)."""
        if not self.reservas_activas:
            if not self.op_reservar():
                return
        reserva = self.reservas_activas.pop(0)
        id_reserva = reserva["idReserva"]
        id_tx = str(uuid.uuid4())

        # Registrar espera antes de enviar la solicitud para no perder una notificación rápida
        self.receptor.registrar_espera(self.id_cliente, id_reserva)
        t_inicio_total = time.perf_counter()

        peticion = {
            "accion": "cancelar",
            "idReserva": id_reserva,
            "idCliente": self.id_cliente,
            "idTransaccion": id_tx,
        }
        ack, nodo, fo, lat_acuse_ms = self._enviar_async(peticion)
        exito_acuse = bool(ack and ack.get("exito"))
        detalle_acuse = (
            f"idReserva={id_reserva} estado={ack.get('estado')}"
            if isinstance(ack, dict) and exito_acuse
            else str((ack or {}).get("error", "timeout"))
        )
        self.metricas.registrar(
            self.id_cliente, id_tx, "cancelar_acuse", nodo, fo, exito_acuse, lat_acuse_ms, detalle_acuse
        )

        if not exito_acuse:
            return

        # Esperar la confirmación asíncrona por PUB/SUB (5557)
        timeout_notif = max(6.0, (self.timeout_ms / 1000.0) * 2.5)
        notif = self.receptor.esperar_notificacion(self.id_cliente, id_reserva, timeout_seg=timeout_notif)
        if notif is not None:
            lat_total_ms = max(lat_acuse_ms, (notif["t_llegada"] - t_inicio_total) * 1000.0)
            exito_total = bool(notif.get("exito")) and notif.get("estado") == "CANCELADA"
            detalle_total = f"idReserva={id_reserva} notif={notif.get('estado')}"
        else:
            # Si hubo failover durante la espera, reconectar SUB y reenviar la cancelación
            nodo, _, _ = self.controlador.conmutar_a_respaldo()
            fo = True
            self.receptor.registrar_espera(self.id_cliente, id_reserva)
            ack2, nodo, _, _ = self._enviar_async(peticion)
            notif = self.receptor.esperar_notificacion(self.id_cliente, id_reserva, timeout_seg=6.0)
            lat_total_ms = (time.perf_counter() - t_inicio_total) * 1000.0
            exito_total = bool(notif and notif.get("exito") and notif.get("estado") == "CANCELADA")
            detalle_total = (
                f"idReserva={id_reserva} notif_tras_failover={notif.get('estado')}"
                if notif
                else f"idReserva={id_reserva} sin_notificacion_sub"
            )

        nodo_final, _, _ = self.controlador.obtener_destino()
        self.metricas.registrar(
            self.id_cliente,
            id_tx,
            "cancelar_total",
            nodo_final,
            fo,
            exito_total,
            lat_total_ms,
            detalle_total,
        )

    # ------------------------------------------------------------------
    # Ciclo de generación de carga
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Ejecuta operaciones según los porcentajes y RPM hasta cumplir ``duracion_seg``."""
        intervalo = 60.0 / float(self.rpm)
        t_fin = time.monotonic() + self.duracion_seg
        operaciones = ("consultar", "reservar", "modificar", "cancelar")

        try:
            while not self.stop_event.is_set() and time.monotonic() < t_fin:
                t_ciclo = time.monotonic()
                eleccion = random.choices(operaciones, weights=self.pesos, k=1)[0]
                if eleccion == "consultar":
                    self.op_consultar()
                elif eleccion == "reservar":
                    self.op_reservar()
                elif eleccion == "modificar":
                    self.op_modificar()
                else:
                    self.op_cancelar()

                restante = t_fin - time.monotonic()
                espera = min(max(0.0, intervalo - (time.monotonic() - t_ciclo)), max(0.0, restante))
                if espera > 0:
                    self.stop_event.wait(espera)
        finally:
            self._cerrar_sockets()


def construir_parser() -> argparse.ArgumentParser:
    """Construye el parser de argumentos CLI."""
    parser = argparse.ArgumentParser(description="Simulador de Clientes Concurrentes (PC1)")
    parser.add_argument("--config", default=None, help="Ruta a pc1_config.json")
    parser.add_argument("--clientes", type=int, default=None, help="Número de hilos clientes")
    parser.add_argument("--duracion", type=float, default=None, help="Duración en segundos")
    parser.add_argument("--rpm", type=int, default=None, help="Solicitudes por minuto por cliente")
    parser.add_argument("--host-principal", default=None, help="Host del GT Principal (PC2)")
    parser.add_argument("--host-respaldo", default=None, help="Host del GT Respaldo (PC3)")
    parser.add_argument("--escenario", default=None, help="Etiqueta del escenario")
    parser.add_argument("--carpeta-resultados", default=None, help="Directorio de salida de los CSV")
    return parser


def main(argv: Optional[List[str]] = None) -> Tuple[str, str]:
    """Ejecuta la simulación completa y retorna las rutas de los dos CSV generados."""
    usar_utf8()
    args = construir_parser().parse_args(argv)
    config = cargar_config(args.config)

    srv_principal = dict(config["servidor_principal"])
    srv_respaldo = dict(config["servidor_respaldo"])
    cfg_sim = dict(config["simulacion"])

    # 1. Sobrescritura por variables de entorno (Docker)
    if os.environ.get("HOST_PRINCIPAL"):
        srv_principal["host"] = os.environ["HOST_PRINCIPAL"]
    if os.environ.get("HOST_RESPALDO"):
        srv_respaldo["host"] = os.environ["HOST_RESPALDO"]
    if os.environ.get("NUM_CLIENTES"):
        cfg_sim["num_clientes"] = int(os.environ["NUM_CLIENTES"])
    if os.environ.get("DURACION_SEG"):
        cfg_sim["duracion_segundos"] = float(os.environ["DURACION_SEG"])
    if os.environ.get("RPM"):
        cfg_sim["solicitudes_por_minuto_por_cliente"] = int(os.environ["RPM"])

    # 2. Sobrescritura por argumentos CLI (tienen prioridad sobre variables de entorno)
    if args.host_principal:
        srv_principal["host"] = args.host_principal
    if args.host_respaldo:
        srv_respaldo["host"] = args.host_respaldo
    if args.clientes is not None:
        cfg_sim["num_clientes"] = args.clientes
    if args.duracion is not None:
        cfg_sim["duracion_segundos"] = args.duracion
    if args.rpm is not None:
        cfg_sim["solicitudes_por_minuto_por_cliente"] = args.rpm
    if args.carpeta_resultados:
        cfg_sim["carpeta_resultados"] = args.carpeta_resultados

    escenario = args.escenario or os.environ.get("ESCENARIO") or "base_3_clientes"

    carpeta_res = cfg_sim.get("carpeta_resultados", "resultados")
    if not os.path.isabs(carpeta_res):
        carpeta_res = os.path.join(DIRECTORIO_PC1, carpeta_res)

    stop_event = threading.Event()
    for nombre in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, nombre, None)
        if sig is not None:
            signal.signal(sig, lambda *_: stop_event.set())

    contexto = zmq.Context()
    receptor = ReceptorNotificaciones(
        contexto, srv_principal["host"], int(srv_principal["puerto_pub"]), stop_event
    )
    receptor.start()
    # Breve pausa para estabilizar la suscripción SUB inicial
    time.sleep(0.35)

    controlador = ControladorFailover(srv_principal, srv_respaldo, receptor)
    metricas = RecolectorMetricas(escenario, carpeta_res)

    num_clientes = int(cfg_sim["num_clientes"])
    log(
        "PC1",
        f"Iniciando escenario '{escenario}' con {num_clientes} clientes | "
        f"duración={cfg_sim['duracion_segundos']}s | RPM/cliente={cfg_sim['solicitudes_por_minuto_por_cliente']}",
    )

    t_inicio = time.perf_counter()
    hilos: List[HiloCliente] = []
    for i in range(1, num_clientes + 1):
        h = HiloCliente(
            id_cliente=f"CLI-{i:02d}",
            contexto=contexto,
            controlador=controlador,
            receptor=receptor,
            metricas=metricas,
            cfg_sim=cfg_sim,
            stop_event=stop_event,
        )
        hilos.append(h)
        h.start()

    try:
        for h in hilos:
            while h.is_alive():
                h.join(timeout=0.5)
                if stop_event.is_set():
                    break
    finally:
        duracion_real = max(0.001, time.perf_counter() - t_inicio)
        stop_event.set()
        for h in hilos:
            h.join(timeout=3.0)
        receptor.join(timeout=2.0)
        contexto.term()

    return metricas.imprimir_y_exportar(duracion_real)


if __name__ == "__main__":
    main()
