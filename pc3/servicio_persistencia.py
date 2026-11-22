"""Servicio de Persistencia (PC3): servidor ZeroMQ REP sobre SQLite.

Atiende en ``tcp://*:5558`` (por defecto) las operaciones JSON del protocolo
del SRB. La configuración se lee de ``config/pc3_config.json`` o de la ruta
indicada en la variable de entorno ``PC3_CONFIG`` / argumento ``--config``.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
from datetime import datetime
from typing import Any, Dict

import zmq

from servicios.database import BaseDatosSRB

DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_POR_DEFECTO = os.path.join(DIRECTORIO_BASE, "config", "pc3_config.json")


def log(mensaje: str) -> None:
    """Imprime un mensaje con marca de tiempo."""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] [PC3] {mensaje}", flush=True)


def cargar_configuracion(ruta: str) -> Dict[str, Any]:
    """Carga el JSON de configuración y resuelve rutas relativas contra pc3/."""
    with open(ruta, encoding="utf-8") as f:
        cfg = json.load(f)
    for clave in ("ruta_base_datos", "ruta_csv"):
        if not os.path.isabs(cfg[clave]):
            cfg[clave] = os.path.join(DIRECTORIO_BASE, cfg[clave])
    cfg["puerto_escucha"] = int(cfg["puerto_escucha"])
    return cfg


class ServicioPersistencia:
    """Servidor REP que enruta peticiones JSON hacia :class:`BaseDatosSRB`."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Inicializa la base de datos y el evento de parada."""
        self.puerto = config["puerto_escucha"]
        self.bd = BaseDatosSRB(config["ruta_base_datos"], config["ruta_csv"])
        self.stop_event = threading.Event()

    def procesar(self, peticion: Dict[str, Any]) -> Dict[str, Any]:
        """Despacha una petición según su campo ``operacion``.

        Returns:
            Diccionario respuesta con ``exito`` y ``resultado`` o ``error``.
        """
        op = peticion.get("operacion")
        params = peticion.get("parametros", [])
        if op == "ejecutarConsulta":
            return {"exito": True, "resultado": self.bd.ejecutar_consulta(peticion["sql"], params)}
        if op == "ejecutarActualizacion":
            filas = self.bd.ejecutar_actualizacion(peticion["sql"], params)
            return {"exito": True, "resultado": True, "filasAfectadas": filas}
        if op == "ejecutarTransaccion":
            filas = self.bd.ejecutar_transaccion_atomica(peticion["operaciones"])
            return {"exito": True, "resultado": True, "filasAfectadas": filas}
        if op == "registrarCheckpoint":
            self.bd.registrar_checkpoint(
                peticion["idTransaccion"], peticion["tipoOperacion"], peticion["idCliente"],
                peticion.get("payloadJson", ""), peticion.get("estado", "RECIBIDA"))
            return {"exito": True, "resultado": True}
        if op == "actualizarCheckpoint":
            ok = self.bd.actualizar_checkpoint(
                peticion["idTransaccion"], peticion["estado"], peticion.get("resultadoJson"))
            if not ok:
                return {"exito": False, "error": "Transacción no encontrada."}
            return {"exito": True, "resultado": True}
        if op == "consultarCheckpoint":
            return {"exito": True, "resultado": self.bd.consultar_checkpoint(peticion["idTransaccion"])}
        if op == "listarPendientes":
            return {"exito": True, "resultado": self.bd.listar_checkpoints_pendientes()}
        if op == "ping":
            return {"exito": True, "resultado": self.bd.estado_salud()}
        return {"exito": False, "error": f"Operación '{op}' no reconocida."}

    def iniciar(self) -> None:
        """Abre el socket REP y atiende peticiones hasta ``stop_event``."""
        contexto = zmq.Context()
        sock = contexto.socket(zmq.REP)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(f"tcp://*:{self.puerto}")
        log(f"Escuchando en tcp://*:{self.puerto} (REP)")
        try:
            while not self.stop_event.is_set():
                try:
                    peticion = sock.recv_json()
                except zmq.Again:
                    continue
                except ValueError as e:
                    sock.send_json({"exito": False, "error": f"JSON inválido: {e}"})
                    continue
                op = peticion.get("operacion") if isinstance(peticion, dict) else None
                try:
                    if not isinstance(peticion, dict):
                        raise ValueError("La petición debe ser un objeto JSON.")
                    respuesta = self.procesar(peticion)
                except Exception as e:  # el servidor nunca debe caerse
                    respuesta = {"exito": False, "error": f"{type(e).__name__}: {e}"}
                if op != "ping":
                    log(f"{op} -> exito={respuesta['exito']}"
                        + (f" error={respuesta['error']}" if not respuesta["exito"] else ""))
                sock.send_json(respuesta)
        finally:
            sock.close()
            contexto.term()
            self.bd.cerrar()
            log("Servicio detenido limpiamente.")

    def detener(self) -> None:
        """Solicita la parada del bucle principal."""
        self.stop_event.set()


def main() -> None:
    """Punto de entrada: carga configuración, registra señales e inicia."""
    parser = argparse.ArgumentParser(description="Servicio de Persistencia PC3")
    parser.add_argument("--config", default=os.environ.get("PC3_CONFIG", CONFIG_POR_DEFECTO))
    args = parser.parse_args()
    servicio = ServicioPersistencia(cargar_configuracion(args.config))
    for nombre in ("SIGINT", "SIGTERM", "SIGBREAK"):  # SIGBREAK: parada limpia en Windows
        senal = getattr(signal, nombre, None)
        if senal is not None:
            signal.signal(senal, lambda *_: servicio.detener())
    servicio.iniciar()


if __name__ == "__main__":
    main()
