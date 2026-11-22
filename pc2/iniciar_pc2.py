"""Orquestador de procesos de PC2 para el contenedor Docker.

Lanza el Gestor de Transacciones, N réplicas de ``servicio_rmc.py`` y el
servicio de cancelaciones, y reenvía SIGINT/SIGTERM para detenerlos en orden.
N se toma de la variable ``NUM_REPLICAS_RMC`` o de ``num_replicas_rmc`` en
``config/pc2_config.json``.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
from typing import List

from servicios.configuracion import DIRECTORIO_PC2, cargar_seccion, instalar_senales, log

ETIQUETA = "PC2"
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def lanzar(script: str, *args: str) -> subprocess.Popen:
    """Inicia un script de PC2 como subproceso."""
    ruta = os.path.join(DIRECTORIO_PC2, script)
    log(ETIQUETA, f"Lanzando {script} {' '.join(args)}".strip())
    return subprocess.Popen([sys.executable, "-u", ruta, *args], cwd=DIRECTORIO_PC2,
                            creationflags=FLAGS)


def detener(procesos: List[subprocess.Popen]) -> None:
    """Envía la señal de parada a todos los subprocesos y espera su salida."""
    for p in procesos:
        if p.poll() is None:
            if os.name == "nt":
                p.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                p.terminate()
    for p in procesos:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def main() -> int:
    """Arranca todos los componentes y los supervisa hasta la parada."""
    stop = threading.Event()
    instalar_senales(stop)
    n = int(os.environ.get("NUM_REPLICAS_RMC") or cargar_seccion("gestor").get("num_replicas_rmc", 2))
    procesos = [lanzar("gestor_transacciones.py")]
    procesos += [lanzar("servicio_rmc.py", "--worker-id", f"RMC-{i}") for i in range(1, n + 1)]
    procesos.append(lanzar("servicio_cancelaciones.py"))
    codigo = 0
    try:
        while not stop.wait(1):
            if any(p.poll() is not None for p in procesos):
                log(ETIQUETA, "Un subproceso terminó inesperadamente; deteniendo PC2.")
                codigo = 1
                break
    finally:
        log(ETIQUETA, "Deteniendo subprocesos...")
        detener(procesos)
        log(ETIQUETA, "PC2 detenido.")
    return codigo


if __name__ == "__main__":
    sys.exit(main())
