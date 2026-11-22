"""Verificación local de los workers de PC2 (RMC x2 + Cancelaciones) contra PC3."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List

import zmq

DIR = os.path.dirname(os.path.abspath(__file__))
PC3 = os.path.join(os.path.dirname(DIR), "pc3")
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def lanzar(script: str, args: List[str], cwd: str, env: Dict[str, str]) -> subprocess.Popen:
    """Levanta un script Python en un subproceso (grupo propio en Windows)."""
    return subprocess.Popen([sys.executable, "-u", script, *args], cwd=cwd, env=env,
                            creationflags=FLAGS)


def detener(proc: subprocess.Popen) -> None:
    """Pide parada limpia (CTRL_BREAK en Windows, SIGTERM en Linux) o mata al proceso."""
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            import signal
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()


def main() -> None:
    """Ejecuta el flujo consulta -> reserva -> modificación -> cancelación."""
    tmp = tempfile.mkdtemp(prefix="pc2_test_")
    cfg3 = os.path.join(tmp, "cfg3.json")
    with open(cfg3, "w", encoding="utf-8") as f:
        json.dump({"puerto_escucha": 5558, "ruta_base_datos": os.path.join(tmp, "t.db"),
                   "ruta_csv": os.path.join(PC3, "datos", "eventos_prueba.csv")}, f)
    procs: List[subprocess.Popen] = []
    ctx = zmq.Context()
    backend = ctx.socket(zmq.DEALER)  # simula el backend del balanceador (5560)
    push = ctx.socket(zmq.PUSH)
    sub = ctx.socket(zmq.SUB)
    ok = False
    try:
        backend.setsockopt(zmq.LINGER, 0)
        backend.setsockopt(zmq.RCVTIMEO, 10000)
        backend.setsockopt(zmq.SNDTIMEO, 10000)
        backend.bind("tcp://*:5560")
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVTIMEO, 10000)
        sub.setsockopt_string(zmq.SUBSCRIBE, "C001")
        sub.connect("tcp://localhost:5557")
        push.setsockopt(zmq.LINGER, 0)
        push.connect("tcp://localhost:5559")

        env3 = dict(os.environ, PC3_CONFIG=cfg3)
        procs.append(lanzar(os.path.join(PC3, "servicio_persistencia.py"), [], PC3, env3))
        env2 = dict(os.environ)
        for wid in ("RMC-1", "RMC-2"):
            procs.append(lanzar(os.path.join(DIR, "servicio_rmc.py"), ["--worker-id", wid], DIR, env2))
        procs.append(lanzar(os.path.join(DIR, "servicio_cancelaciones.py"), [], DIR, env2))

        def pedir(msg: Dict[str, Any]) -> Dict[str, Any]:
            backend.send_multipart([b"", json.dumps(msg).encode()])
            return json.loads(backend.recv_multipart()[-1])

        def cupos(id_evento: str) -> int:
            r = pedir({"accion": "consultar", "mes": 10})
            return next(e[5] for e in r["eventos"] if e[0] == id_evento)

        # Esperar a que PC3 y las 2 réplicas estén listas (consulta mes 10)
        vistos = set()
        fin = time.time() + 40
        while len(vistos) < 2 and time.time() < fin:
            try:
                r = pedir({"accion": "consultar", "mes": 10})
                vistos.add(r["procesadoPor"])
            except zmq.Again:
                pass
        assert vistos == {"RMC-1", "RMC-2"}, f"Réplicas detectadas: {vistos}"
        assert r["exito"] and len(r["eventos"]) == 50 and r["RespuestaConsulta"] == r["eventos"]
        assert cupos("E001") == 200 and cupos("E002") == 200

        # Cliente REQ directo a PC3 para registrar checkpoint y verificar la BD
        pc3 = ctx.socket(zmq.REQ)
        pc3.setsockopt(zmq.LINGER, 0)
        pc3.setsockopt(zmq.RCVTIMEO, 10000)
        pc3.connect("tcp://localhost:5558")

        def pc3_req(msg: Dict[str, Any]) -> Dict[str, Any]:
            pc3.send_json(msg)
            return pc3.recv_json()

        pc3_req({"operacion": "registrarCheckpoint", "idTransaccion": "TX1",
                 "tipoOperacion": "RESERVAR", "idCliente": "C001", "payloadJson": "{}"})

        # Reserva: reparto entre réplicas y descuento de cupos
        workers = set()
        r = pedir({"accion": "reservar", "idCliente": "C001", "idEvento": "E001",
                   "numPuestos": 5, "idTransaccion": "TX1"})
        workers.add(r["procesadoPor"])
        assert r["exito"] and r["RespuestaReserva"] == "Éxito", r
        id_reserva = r["idReserva"]
        r2 = pedir({"accion": "reservar", "idCliente": "C002", "idEvento": "E003",
                    "numPuestos": 1})
        workers.add(r2["procesadoPor"])
        assert workers == {"RMC-1", "RMC-2"}, f"Sin reparto entre réplicas: {workers}"
        assert cupos("E001") == 195
        assert pc3_req({"operacion": "consultarCheckpoint",
                        "idTransaccion": "TX1"})["resultado"]["estado"] == "COMPLETADA"

        # Reserva rechazada por falta de cupo (no altera cupos)
        r = pedir({"accion": "reservar", "idCliente": "C001", "idEvento": "E001",
                   "numPuestos": 9999})
        assert not r["exito"] and r["RespuestaReserva"] == "Rechazado", r
        assert cupos("E001") == 195

        # Modificación a otro evento con distinta cantidad
        r = pedir({"accion": "modificar", "idReserva": id_reserva, "nuevoIdEvento": "E002",
                   "nuevosPuestos": 3})
        assert r["exito"] and r["RespuestaModificacion"] == "Éxito", r
        assert cupos("E001") == 200 and cupos("E002") == 197
        # Modificación sin cupo: debe revertir todo
        r = pedir({"accion": "modificar", "idReserva": id_reserva, "nuevoIdEvento": "E001",
                   "nuevosPuestos": 9999})
        assert not r["exito"], r
        assert cupos("E001") == 200 and cupos("E002") == 197
        # Cambio de cantidad en el mismo evento
        r = pedir({"accion": "modificar", "idReserva": id_reserva, "nuevoIdEvento": "E002",
                   "nuevosPuestos": 4})
        assert r["exito"] and cupos("E002") == 196, r

        # Cancelación asíncrona + notificación
        time.sleep(1)  # estabilizar la suscripción SUB
        push.send_json({"accion": "cancelar", "idReserva": id_reserva, "idCliente": "C001",
                        "idTransaccion": "TX-C"})
        topico, payload = sub.recv_multipart()
        notif = json.loads(payload)
        assert topico == b"C001" and notif["exito"] and notif["estado"] == "CANCELADA", notif
        assert cupos("E002") == 200
        estado = pc3_req({"operacion": "ejecutarConsulta",
                          "sql": "SELECT estado FROM Reserva WHERE idReserva = ?",
                          "parametros": [id_reserva]})["resultado"]
        assert estado == [["CANCELADA"]], estado
        # Segunda cancelación: rechazada
        push.send_json({"accion": "cancelar", "idReserva": id_reserva, "idCliente": "C001"})
        notif = json.loads(sub.recv_multipart()[1])
        assert not notif["exito"] and notif["estado"] == "RECHAZADA", notif
        pc3.close()
        ok = True
    finally:
        for s in (backend, push, sub):
            s.close(linger=0)
        ctx.term()
        for p in reversed(procs):
            detener(p)
        shutil.rmtree(tmp, ignore_errors=True)
    if ok:
        print("[TEST WORKERS PC2] OK")


if __name__ == "__main__":
    main()
