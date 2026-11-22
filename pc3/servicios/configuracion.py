"""Utilidades comunes de los componentes de respaldo de PC3."""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
from datetime import datetime
from typing import Any, Dict

DIRECTORIO_PC3 = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_POR_DEFECTO = os.path.join(DIRECTORIO_PC3, "config", "pc3_config.json")


def cargar_config(ruta: str | None = None) -> Dict[str, Any]:
    """Carga el JSON completo de PC3 (ruta, ``PC3_CONFIG`` o archivo por defecto)."""
    ruta = ruta or os.environ.get("PC3_CONFIG", CONFIG_POR_DEFECTO)
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def cargar_respaldo(ruta: str | None = None) -> Dict[str, Any]:
    """Retorna la sección ``respaldo`` aplicando sobrescrituras por variables de entorno."""
    respaldo = dict(cargar_config(ruta)["respaldo"])
    host_gt = os.environ.get("HOST_GT_PRINCIPAL") or os.environ.get("GT_PRINCIPAL_HOST")
    if host_gt:
        respaldo["host_gt_principal"] = host_gt
    if os.environ.get("NUM_REPLICAS_RMC"):
        respaldo["num_replicas_rmc"] = int(os.environ["NUM_REPLICAS_RMC"])
    if os.environ.get("HOST_PERSISTENCIA"):
        respaldo["host_persistencia"] = os.environ["HOST_PERSISTENCIA"]
    return respaldo


def usar_utf8() -> None:
    """Fuerza UTF-8 en stdout/stderr (evita fallos con símbolos en consolas cp1252)."""
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def log(etiqueta: str, mensaje: str) -> None:
    """Imprime un mensaje con marca de tiempo y etiqueta."""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] [{etiqueta}] {mensaje}", flush=True)


def instalar_senales(stop_event: threading.Event) -> None:
    """Registra SIGINT/SIGTERM (y SIGBREAK en Windows) para activar ``stop_event``."""
    def manejador(*_: Any) -> None:
        stop_event.set()

    for nombre in ("SIGINT", "SIGTERM", "SIGBREAK"):
        senal = getattr(signal, nombre, None)
        if senal is not None:
            signal.signal(senal, manejador)
