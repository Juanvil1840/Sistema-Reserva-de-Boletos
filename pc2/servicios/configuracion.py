"""Utilidades comunes de PC2: configuración, logging y señales de parada."""

from __future__ import annotations

import json
import os
import signal
import threading
from datetime import datetime
from typing import Any, Dict

DIRECTORIO_PC2 = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_POR_DEFECTO = os.path.join(DIRECTORIO_PC2, "config", "pc2_config.json")


def cargar_seccion(seccion: str, ruta: str | None = None) -> Dict[str, Any]:
    """Carga una sección de ``pc2_config.json`` aplicando variables de entorno.

    La ruta se toma del argumento, de la variable de entorno ``PC2_CONFIG``
    o del archivo ``config/pc2_config.json`` (en ese orden de prioridad).
    Sobrescribe ``host_persistencia`` si existe ``HOST_PERSISTENCIA`` y
    ``num_replicas_rmc`` si existe ``NUM_REPLICAS_RMC``.
    """
    ruta = ruta or os.environ.get("PC2_CONFIG", CONFIG_POR_DEFECTO)
    with open(ruta, encoding="utf-8") as f:
        cfg = dict(json.load(f)[seccion])
    if os.environ.get("HOST_PERSISTENCIA") and "host_persistencia" in cfg:
        cfg["host_persistencia"] = os.environ["HOST_PERSISTENCIA"]
    if os.environ.get("NUM_REPLICAS_RMC") and "num_replicas_rmc" in cfg:
        cfg["num_replicas_rmc"] = int(os.environ["NUM_REPLICAS_RMC"])
    if os.environ.get("HOST_GESTOR") and "host_gestor" in cfg:
        cfg["host_gestor"] = os.environ["HOST_GESTOR"]
    if os.environ.get("HOST_CANCELACIONES") and "host_cancelaciones" in cfg:
        cfg["host_cancelaciones"] = os.environ["HOST_CANCELACIONES"]
    return cfg


def log(etiqueta: str, mensaje: str) -> None:
    """Imprime un mensaje con marca de tiempo y etiqueta del servicio."""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] [{etiqueta}] {mensaje}", flush=True)


def instalar_senales(stop_event: threading.Event) -> None:
    """Registra SIGINT/SIGTERM (y SIGBREAK en Windows) para activar ``stop_event``."""
    def manejador(*_: Any) -> None:
        stop_event.set()

    for nombre in ("SIGINT", "SIGTERM", "SIGBREAK"):
        senal = getattr(signal, nombre, None)
        if senal is not None:
            signal.signal(senal, manejador)
