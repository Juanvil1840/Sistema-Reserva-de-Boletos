"""Prueba de integración local: PC3 + Gestor + 2 réplicas RMC + Cancelaciones."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Dict, List

import zmq

DIR = os.path.dirname(os.path.abspath(__file__))
PC3 = os.path.join(os.path.dirname(DIR), "pc3")
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
ctx = zmq.Context()


def lanzar(script: str, args: List[str], cwd: str, env: Dict[str, str]) -> subprocess.Popen:
    """Levanta un script Python como subproceso."""
    return subprocess.Popen([sys.executable, "-u", script, *args], cwd=cwd, env=env,
                            creationflags=FLAGS)


def detener(proc: subprocess.Popen) -> None:
    """Parada limpia (CTRL_BREAK en Windows, SIGTERM en Linux); mata si no responde."""
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()


class ClienteReq:
    """Cliente REQ con recreación de socket ante timeout."""

    def __init__(self, puerto: int, timeout_ms: int = 5000) -> None:
        self.puerto, self.timeout = puerto, timeout_ms
        self.sock = None
        self._abrir()

    def _abrir(self) -> None:
        if self.sock:
            self.sock.close(linger=0)
        self.sock = ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.RCVTIMEO, self.timeout)
        self.sock.setsockopt(zmq.SNDTIMEO, self.timeout)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(f"tcp://localhost:{self.puerto}")

    def pedir(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.sock.send_json(msg)
            return self.sock.recv_json()
        except zmq.ZMQError:
            self._abrir()
            raise

    def cerrar(self) -> None:
        self.sock.close(linger=0)


def main() -> None:
    """Ejecuta el escenario completo de integración."""
    tmp = tempfile.mkdtemp(prefix="pc2_gestor_test_")
    cfg3 = os.path.join(tmp, "cfg3.json")
    with open(cfg3, "w", encoding="utf-8") as f:
        json.dump({"puerto_escucha": 5558, "ruta_base_datos": os.path.join(tmp, "t.db"),
                   "ruta_csv": os.path.join(PC3, "datos", "eventos_prueba.csv")}, f)
    procs: List[subprocess.Popen] = []
    ok = False
    sub = ctx.socket(zmq.SUB)
    try:
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVTIMEO, 10000)
        sub.setsockopt_string(zmq.SUBSCRIBE, "C001")
        sub.connect("tcp://localhost:5557")

        procs.append(lanzar(os.path.join(PC3, "servicio_persistencia.py"), [], PC3,
                            dict(os.environ, PC3_CONFIG=cfg3)))
        env = dict(os.environ)
        procs.append(lanzar(os.path.join(DIR, "gestor_transacciones.py"), [], DIR, env))
        for wid in ("RMC-1", "RMC-2"):
            procs.append(lanzar(os.path.join(DIR, "servicio_rmc.py"), ["--worker-id", wid], DIR, env))
        procs.append(lanzar(os.path.join(DIR, "servicio_cancelaciones.py"), [], DIR, env))

        # Heartbeat
        hb = ClienteReq(5569, 1000)
        pong = None
        for _ in range(30):
            try:
                pong = hb.pedir({"tipo": "ping"})
                break
            except zmq.ZMQError:
                time.sleep(0.5)
        assert pong and pong["tipo"] == "pong" and pong["rol"] == "PRINCIPAL" \
            and pong["estado"] == "ACTIVO", pong
        hb.cerrar()

        # Esperar a que ambas réplicas respondan por el balanceador
        vistos: set = set()
        cli = ClienteReq(5555, 3000)
        fin = time.time() + 40
        while len(vistos) < 2 and time.time() < fin:
            try:
                r = cli.pedir({"accion": "consultar", "mes": 10})
                if r.get("exito"):
                    vistos.add(r["procesadoPor"])
            except zmq.ZMQError:
                time.sleep(0.3)
        assert vistos == {"RMC-1", "RMC-2"}, vistos
        assert len(r["eventos"]) == 50

        def cupos(id_evento: str) -> int:
            return next(e[5] for e in cli.pedir({"accion": "consultar", "mes": 10})["eventos"]
                        if e[0] == id_evento)

        # Acción desconocida y JSON inválido
        assert not cli.pedir({"accion": "volar"})["exito"]
        cli.sock.send(b"no es json")
        assert not json.loads(cli.sock.recv())["exito"]

        # 3 clientes concurrentes: 4 reservas de 1 puesto + modificar una a 3 (=> 6 por cliente)
        procesadores: set = set()
        reservas: Dict[str, str] = {}
        errores: List[str] = []

        def trabajar(id_cliente: str) -> None:
            c = ClienteReq(5555, 10000)
            try:
                assert c.pedir({"accion": "consultar", "mes": 10})["exito"]
                ids = []
                for _ in range(4):
                    r = c.pedir({"accion": "reservar", "idCliente": id_cliente,
                                 "idEvento": "E004", "numPuestos": 1})
                    assert r["exito"] and r["RespuestaReserva"] == "Éxito", r
                    procesadores.add(r["procesadoPor"])
                    ids.append(r["idReserva"])
                m = c.pedir({"accion": "modificar", "idReserva": ids[0], "nuevoIdEvento": "E004",
                             "nuevosPuestos": 3, "idCliente": id_cliente})
                assert m["exito"] and m["RespuestaModificacion"] == "Éxito", m
                procesadores.add(m["procesadoPor"])
                reservas[id_cliente] = ids[0]
            except Exception as e:  # noqa: BLE001
                errores.append(f"{id_cliente}: {e!r}")
            finally:
                c.cerrar()

        hilos = [threading.Thread(target=trabajar, args=(f"C00{i}",)) for i in (1, 2, 3)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()
        assert not errores, errores
        assert procesadores == {"RMC-1", "RMC-2"}, procesadores
        assert cupos("E004") == 200 - 18

        # Idempotencia por checkpoint
        peticion = {"accion": "reservar", "idCliente": "C002", "idEvento": "E005",
                    "numPuestos": 2, "idTransaccion": "TX-IDEM"}
        r1 = cli.pedir(peticion)
        assert r1["exito"] and "reintentoIdempotente" not in r1, r1
        assert cupos("E005") == 198
        r2 = cli.pedir(peticion)
        assert r2["exito"] and r2["reintentoIdempotente"] is True \
            and r2["idReserva"] == r1["idReserva"], r2
        assert cupos("E005") == 198

        # Cancelación asíncrona por DEALER a 5556 + notificación por SUB
        time.sleep(1)  # estabilizar la suscripción SUB
        dealer = ctx.socket(zmq.DEALER)
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.setsockopt(zmq.RCVTIMEO, 5000)
        dealer.connect("tcp://localhost:5556")
        dealer.send_json({"accion": "cancelar", "idReserva": reservas["C001"],
                          "idCliente": "C001", "idTransaccion": "TX-CANC"})
        ack = json.loads(dealer.recv_multipart()[-1])
        assert ack["exito"] and ack["estado"] == "EN_PROCESO" and ack["idTransaccion"] == "TX-CANC", ack
        dealer.close(linger=0)
        topico, payload = sub.recv_multipart()
        notif = json.loads(payload)
        assert topico == b"C001" and notif["estado"] == "CANCELADA" and notif["exito"], notif
        assert cupos("E004") == 200 - 18 + 3
        pc3 = ClienteReq(5558, 5000)
        cp = pc3.pedir({"operacion": "consultarCheckpoint", "idTransaccion": "TX-CANC"})["resultado"]
        assert cp["estado"] == "COMPLETADA", cp
        pc3.cerrar()
        cli.cerrar()
        ok = True
    finally:
        sub.close(linger=0)
        for p in reversed(procs):
            detener(p)
        ctx.term()
        shutil.rmtree(tmp, ignore_errors=True)
    if ok:
        print("[TEST GESTOR PC2] OK")


if __name__ == "__main__":
    main()
