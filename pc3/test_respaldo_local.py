"""Prueba de resiliencia: caída de PC2 y failover al GT de respaldo de PC3."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional

import zmq

DIR = os.path.dirname(os.path.abspath(__file__))
PC2 = os.path.join(os.path.dirname(DIR), "pc2")
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
ctx = zmq.Context()


def lanzar(script: str, cwd: str, env: Dict[str, str]) -> subprocess.Popen:
    """Levanta un script Python como subproceso."""
    return subprocess.Popen([sys.executable, "-u", script], cwd=cwd, env=env, creationflags=FLAGS)


def detener(proc: subprocess.Popen) -> None:
    """Parada limpia (CTRL_BREAK/SIGTERM); mata el proceso si no responde."""
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        proc.wait(timeout=15)
    except Exception:
        proc.kill()
        proc.wait()


def matar_arbol(proc: subprocess.Popen) -> None:
    """Caída abrupta: mata el proceso y todos sus hijos (en Windows kill no los alcanza)."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        proc.kill()
    proc.wait()


class ClienteReq:
    """Cliente REQ que recrea el socket ante timeout."""

    def __init__(self, puerto: int, timeout_ms: int = 5000) -> None:
        self.puerto, self.timeout, self.sock = puerto, timeout_ms, None
        self._abrir()

    def _abrir(self) -> None:
        if self.sock:
            self.sock.close(linger=0)
        self.sock = ctx.socket(zmq.REQ)
        for opt in (zmq.RCVTIMEO, zmq.SNDTIMEO):
            self.sock.setsockopt(opt, self.timeout)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(f"tcp://localhost:{self.puerto}")

    def pedir(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.sock.send_json(msg)
            return self.sock.recv_json()
        except zmq.ZMQError:
            self._abrir()
            raise

    def pedir_reintentando(self, msg: Dict[str, Any], veces: int = 40) -> Dict[str, Any]:
        for _ in range(veces):
            try:
                r = self.pedir(msg)
                if "Sin réplicas" not in str(r.get("error", "")):
                    return r
            except zmq.ZMQError:
                pass
            time.sleep(0.5)
        raise AssertionError(f"Sin respuesta para {msg}")


def main() -> None:
    """Ejecuta el escenario completo de failover."""
    tmp = tempfile.mkdtemp(prefix="pc3_respaldo_test_")
    with open(os.path.join(DIR, "config", "pc3_config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["ruta_base_datos"] = os.path.join(tmp, "t.db")
    cfg["ruta_csv"] = os.path.join(DIR, "datos", "eventos_prueba.csv")
    cfg3 = os.path.join(tmp, "cfg3.json")
    with open(cfg3, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    env = dict(os.environ, PC3_CONFIG=cfg3)

    procs: List[subprocess.Popen] = []
    sub = ctx.socket(zmq.SUB)
    ok = False
    pc2: Optional[subprocess.Popen] = None
    try:
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVTIMEO, 30000)
        sub.setsockopt_string(zmq.SUBSCRIBE, "C001")
        sub.connect("tcp://localhost:5557")

        procs.append(lanzar(os.path.join(DIR, "servicio_persistencia.py"), DIR, env))
        pc2 = lanzar(os.path.join(PC2, "iniciar_pc2.py"), PC2, dict(os.environ))
        cli = ClienteReq(5555, 3000)
        pc3 = ClienteReq(5558, 5000)

        def cupos(id_evento: str) -> int:
            r = cli.pedir_reintentando({"accion": "consultar", "mes": 10})
            return next(e[5] for e in r["eventos"] if e[0] == id_evento)

        def consulta_bd(sql: str, params: List[Any]) -> List[Any]:
            return pc3.pedir({"operacion": "ejecutarConsulta", "sql": sql,
                              "parametros": params})["resultado"]

        # Operación normal atendida por PC2 principal
        r = cli.pedir_reintentando({"accion": "consultar", "mes": 10}, 80)
        assert r["exito"] and len(r["eventos"]) == 50, r
        assert not r["procesadoPor"].startswith("RMC-RESPALDO"), r
        r = cli.pedir_reintentando({"accion": "reservar", "idCliente": "C001", "idEvento": "E004",
                                    "numPuestos": 3})
        assert r["exito"], r
        id_reserva = r["idReserva"]
        assert cupos("E004") == 197

        # El GT de respaldo arranca en PASIVO (PC2 ya responde en 5569)
        procs.append(lanzar(os.path.join(DIR, "gestor_transacciones_respaldo.py"), DIR, env))
        time.sleep(3)

        # Checkpoints pendientes de una "transacción interrumpida"
        pc3.pedir({"operacion": "registrarCheckpoint", "idTransaccion": "TX-RECOVER-CANCEL",
                   "tipoOperacion": "CANCELAR", "idCliente": "C001", "estado": "RECIBIDA",
                   "payloadJson": json.dumps({"accion": "cancelar", "idReserva": id_reserva,
                                              "idCliente": "C001"})})
        peticion_res = {"accion": "reservar", "idCliente": "C002", "idEvento": "E006",
                        "numPuestos": 2, "idTransaccion": "TX-RECOVER-RES"}
        pc3.pedir({"operacion": "registrarCheckpoint", "idTransaccion": "TX-RECOVER-RES",
                   "tipoOperacion": "RESERVAR", "idCliente": "C002", "estado": "EN_PROCESO",
                   "payloadJson": json.dumps(peticion_res)})

        # Caída abrupta de PC2
        t_caida = time.time()
        matar_arbol(pc2)

        # Reconectar SUB al nuevo PUB tras la caída abrupta del proceso anterior
        sub.disconnect("tcp://localhost:5557")
        sub.connect("tcp://localhost:5557")

        # Esperar failover y recuperación de ambos checkpoints
        fin = time.time() + 60
        estados: Dict[str, str] = {}
        while time.time() < fin:
            estados = {}
            for tx in ("TX-RECOVER-CANCEL", "TX-RECOVER-RES"):
                cp = pc3.pedir({"operacion": "consultarCheckpoint", "idTransaccion": tx})["resultado"]
                estados[tx] = cp["estado"]
            if all(e == "COMPLETADA" for e in estados.values()):
                break
            time.sleep(0.5)
        assert all(e == "COMPLETADA" for e in estados.values()), estados
        print(f"[TEST RESPALDO PC3] failover + recuperación en {time.time() - t_caida:.1f}s")

        # Cupos y estado de la BD tras la recuperación
        assert consulta_bd("SELECT estado FROM Reserva WHERE idReserva = ?",
                           [id_reserva]) == [["CANCELADA"]]
        assert cupos("E004") == 200
        assert cupos("E006") == 198

        # Notificación de la cancelación recuperada
        topico, payload = sub.recv_multipart()
        notif = json.loads(payload)
        assert topico == b"C001" and notif["estado"] == "CANCELADA" \
            and notif["idReserva"] == id_reserva, notif

        # Reintento idempotente: sin duplicar asientos
        r = cli.pedir_reintentando(peticion_res)
        assert r["exito"] and r["reintentoIdempotente"] is True, r
        assert cupos("E006") == 198

        # Tráfico síncrono nuevo atendido por el respaldo
        r = cli.pedir_reintentando({"accion": "reservar", "idCliente": "C001", "idEvento": "E007",
                                    "numPuestos": 1})
        assert r["exito"] and r["procesadoPor"].startswith("RMC-RESPALDO"), r
        nueva = r["idReserva"]
        assert cupos("E007") == 199

        # Cancelación asíncrona por 5556 + SUB 5557
        dealer = ctx.socket(zmq.DEALER)
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.setsockopt(zmq.RCVTIMEO, 5000)
        dealer.connect("tcp://localhost:5556")
        dealer.send_json({"accion": "cancelar", "idReserva": nueva, "idCliente": "C001",
                          "idTransaccion": "TX-NUEVA-CANC"})
        ack = json.loads(dealer.recv_multipart()[-1])
        assert ack["exito"] and ack["estado"] == "EN_PROCESO", ack
        dealer.close(linger=0)
        notif = json.loads(sub.recv_multipart()[1])
        assert notif["estado"] == "CANCELADA" and notif["idReserva"] == nueva, notif
        assert cupos("E007") == 200
        cli.sock.close(linger=0)
        pc3.sock.close(linger=0)
        ok = True
    except Exception:
        import traceback
        traceback.print_exc()
        raise
    finally:
        sub.close(linger=0)
        if pc2 is not None and pc2.poll() is None:
            matar_arbol(pc2)
        for p in reversed(procs):
            detener(p)
        ctx.term()
        shutil.rmtree(tmp, ignore_errors=True)
    if ok:
        print("[TEST RESPALDO PC3] OK")


if __name__ == "__main__":
    main()
