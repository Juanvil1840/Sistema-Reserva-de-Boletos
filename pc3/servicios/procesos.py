"""Lanzamiento y parada ordenada de subprocesos Python (multiplataforma)."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from typing import List

from servicios.configuracion import DIRECTORIO_PC3

FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def lanzar(script: str, *args: str) -> subprocess.Popen:
    """Inicia un script de pc3/ como subproceso (grupo propio en Windows)."""
    ruta = os.path.join(DIRECTORIO_PC3, script)
    return subprocess.Popen([sys.executable, "-u", ruta, *args], cwd=DIRECTORIO_PC3,
                            creationflags=FLAGS)


def detener(procesos: List[subprocess.Popen], espera_s: float = 10.0) -> None:
    """Pide la parada limpia a cada subproceso y lo mata si no responde a tiempo."""
    for p in procesos:
        if p.poll() is None:
            p.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    for p in procesos:
        try:
            p.wait(timeout=espera_s)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
