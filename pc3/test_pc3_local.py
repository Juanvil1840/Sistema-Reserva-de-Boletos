"""Verificación local de PC3 con una BD temporal (no toca la BD principal)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict

import zmq

DIR = os.path.dirname(os.path.abspath(__file__))
PUERTO = 5558


def enviar(sock: zmq.Socket, msg: Dict[str, Any]) -> Dict[str, Any]:
    """Envía una petición JSON y retorna la respuesta."""
    sock.send_json(msg)
    return sock.recv_json()


def main() -> None:
    """Ejecuta las pruebas contra un servidor levantado en subproceso."""
    tmp = tempfile.mkdtemp(prefix="pc3_test_")
    cfg_path = os.path.join(tmp, "cfg.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump({"puerto_escucha": PUERTO,
                   "ruta_base_datos": os.path.join(tmp, "db", "test.db"),
                   "ruta_csv": os.path.join(DIR, "datos", "eventos_prueba.csv")}, f)
    env = dict(os.environ, PC3_CONFIG=cfg_path)
    proc = subprocess.Popen([sys.executable, "-u", os.path.join(DIR, "servicio_persistencia.py")],
                            env=env, cwd=DIR)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 10000)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://localhost:{PUERTO}")
    try:
        # Ping: 50 eventos
        r = None
        for _ in range(10):
            try:
                r = enviar(sock, {"operacion": "ping"})
                break
            except zmq.Again:
                sock.close()
                sock = ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.RCVTIMEO, 10000)
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(f"tcp://localhost:{PUERTO}")
                time.sleep(0.5)
        assert r and r["exito"], f"ping falló: {r}"
        assert r["resultado"]["eventos"] == 50, r
        assert r["resultado"]["reservas"] == 0, r

        # Consulta mes 10
        r = enviar(sock, {"operacion": "ejecutarConsulta",
                          "sql": "SELECT idEvento FROM Evento WHERE mes = ?", "parametros": [10]})
        assert r["exito"] and len(r["resultado"]) > 0, r
        total_mes10 = len(r["resultado"])
        r = enviar(sock, {"operacion": "ejecutarConsulta",
                          "sql": "SELECT puestosTotales, COUNT(*) FROM Evento GROUP BY 1 ORDER BY 1"})
        assert r["resultado"] == [[100, 30], [200, 20]], r

        # Transacción atómica: reserva
        ops = [
            {"sql": "UPDATE Evento SET puestosDisponibles = puestosDisponibles - ?"
                    " WHERE idEvento = ? AND puestosDisponibles >= ?",
             "parametros": [5, "E001", 5], "minimoFilas": 1},
            {"sql": "INSERT INTO Reserva (idReserva, idCliente, idEvento, cantidadPuestos, estado)"
                    " VALUES (?, ?, ?, ?, 'CONFIRMADA')",
             "parametros": ["R001", "C001", "E001", 5]},
        ]
        r = enviar(sock, {"operacion": "ejecutarTransaccion", "operaciones": ops})
        assert r["exito"], r
        r = enviar(sock, {"operacion": "ejecutarConsulta",
                          "sql": "SELECT puestosDisponibles FROM Evento WHERE idEvento = 'E001'"})
        assert r["resultado"][0][0] == 195, r
        r = enviar(sock, {"operacion": "ejecutarConsulta",
                          "sql": "SELECT cantidadPuestos, estado FROM Reserva WHERE idReserva = 'R001'"})
        assert r["resultado"] == [[5, "CONFIRMADA"]], r

        # Rollback: sin puestos suficientes no debe crear nada
        ops[0]["parametros"] = [999, "E001", 999]
        ops[1]["parametros"] = ["R002", "C001", "E001", 999]
        r = enviar(sock, {"operacion": "ejecutarTransaccion", "operaciones": ops})
        assert not r["exito"], r
        r = enviar(sock, {"operacion": "ejecutarConsulta",
                          "sql": "SELECT COUNT(*) FROM Reserva"})
        assert r["resultado"][0][0] == 1, r

        # Ciclo de checkpoint
        r = enviar(sock, {"operacion": "registrarCheckpoint", "idTransaccion": "T1",
                          "tipoOperacion": "RESERVAR", "idCliente": "C001",
                          "payloadJson": "{}", "estado": "RECIBIDA"})
        assert r["exito"], r
        r = enviar(sock, {"operacion": "consultarCheckpoint", "idTransaccion": "T1"})
        assert r["resultado"]["estado"] == "RECIBIDA", r
        r = enviar(sock, {"operacion": "listarPendientes"})
        assert [c["idTransaccion"] for c in r["resultado"]] == ["T1"], r
        r = enviar(sock, {"operacion": "actualizarCheckpoint", "idTransaccion": "T1",
                          "estado": "COMPLETADA", "resultadoJson": "{\"ok\": true}"})
        assert r["exito"], r
        r = enviar(sock, {"operacion": "consultarCheckpoint", "idTransaccion": "T1"})
        assert r["resultado"]["estado"] == "COMPLETADA", r
        r = enviar(sock, {"operacion": "listarPendientes"})
        assert r["resultado"] == [], r

        # Error controlado: el servidor sigue vivo
        r = enviar(sock, {"operacion": "ejecutarConsulta", "sql": "SELECT * FROM NoExiste"})
        assert not r["exito"] and "error" in r, r
        assert enviar(sock, {"operacion": "ping"})["exito"]
        print(f"[TEST PC3] eventos mes 10: {total_mes10}")
        resultado_ok = True
    finally:
        sock.close()
        ctx.term()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)
    if resultado_ok:
        print("[TEST PC3] OK")


if __name__ == "__main__":
    main()
