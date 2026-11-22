"""Orquestador de procesos de PC3 para Docker.

Lanza ``servicio_persistencia.py``, verifica con un ping al puerto 5558 que la
base de datos esté lista y, si ``respaldo.habilitado`` es true, lanza
``gestor_transacciones_respaldo.py``. Propaga SIGINT/SIGTERM para un apagado limpio.
"""

from __future__ import annotations

import sys
import threading

import zmq

from servicios import procesos
from servicios.cliente_persistencia import ClientePersistencia, ErrorPersistencia
from servicios.configuracion import cargar_config, instalar_senales, log, usar_utf8

ETIQUETA = "PC3"


def esperar_persistencia(puerto: int, stop: threading.Event, limite_s: float = 30.0) -> bool:
    """Hace ping a la persistencia hasta que responda o venza ``limite_s``."""
    cliente = ClientePersistencia("localhost", puerto, 1000)
    try:
        for _ in range(int(limite_s)):
            if stop.is_set():
                return False
            try:
                info = cliente.ping()
                log(ETIQUETA, f"Persistencia lista: {info}")
                return True
            except ErrorPersistencia:
                stop.wait(1.0)
        return False
    finally:
        cliente.cerrar()


def main() -> int:
    """Arranca persistencia y respaldo, y los supervisa hasta la parada."""
    usar_utf8()
    stop = threading.Event()
    instalar_senales(stop)
    config = cargar_config()
    lanzados = [procesos.lanzar("servicio_persistencia.py")]
    codigo = 0
    try:
        if not esperar_persistencia(int(config["puerto_escucha"]), stop):
            log(ETIQUETA, "La persistencia no estuvo disponible a tiempo.")
            return 1
        if config.get("respaldo", {}).get("habilitado", False):
            lanzados.append(procesos.lanzar("gestor_transacciones_respaldo.py"))
        else:
            log(ETIQUETA, "Respaldo deshabilitado: solo se ejecuta la persistencia.")
        while not stop.wait(1.0):
            if any(p.poll() is not None for p in lanzados):
                log(ETIQUETA, "Un subproceso terminó inesperadamente; deteniendo PC3.")
                codigo = 1
                break
    finally:
        log(ETIQUETA, "Deteniendo subprocesos...")
        procesos.detener(list(reversed(lanzados)))
        zmq.Context.instance().term()
        log(ETIQUETA, "PC3 detenido.")
    return codigo


if __name__ == "__main__":
    sys.exit(main())
