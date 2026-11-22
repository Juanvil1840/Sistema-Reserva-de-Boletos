"""Prueba de integración de punta a punta (E2E) para PC1 con failover en vivo."""

from __future__ import annotations

import csv
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

DIR_PC1 = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(DIR_PC1)
DIR_PC2 = os.path.join(RAIZ, "pc2")
DIR_PC3 = os.path.join(RAIZ, "pc3")
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def lanzar(script: str, args: List[str], cwd: str, env: Dict[str, str]) -> subprocess.Popen:
    """Inicia un script Python como subproceso."""
    return subprocess.Popen(
        [sys.executable, "-u", script, *args],
        cwd=cwd,
        env=env,
        creationflags=FLAGS,
    )


def detener(proc: subprocess.Popen) -> None:
    """Solicita parada limpia o mata al proceso si no termina a tiempo."""
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        proc.wait(timeout=12)
    except Exception:
        proc.kill()
        proc.wait()


def matar_arbol(proc: subprocess.Popen) -> None:
    """Simula caída abrupta matando el árbol de procesos."""
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        proc.kill()
    proc.wait()


def esperar_heartbeat(ctx: zmq.Context, puerto: int = 5569, limite_s: float = 20.0) -> None:
    """Espera a que el Gestor Principal en PC2 responda ping en 5569."""
    fin = time.time() + limite_s
    while time.time() < fin:
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.setsockopt(zmq.SNDTIMEO, 1000)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://localhost:{puerto}")
        try:
            sock.send_json({"tipo": "ping"})
            resp = sock.recv_json()
            if resp.get("tipo") == "pong":
                return
        except zmq.ZMQError:
            time.sleep(0.3)
        finally:
            sock.close(linger=0)
    raise AssertionError("PC2 Principal no estuvo listo a tiempo en 5569")


def main() -> None:
    """Ejecuta el escenario E2E de PC1 con caída abrupta de PC2 a mitad de corrida."""
    tmp = tempfile.mkdtemp(prefix="pc1_e2e_test_")
    carpeta_csv = os.path.join(tmp, "resultados")
    os.makedirs(carpeta_csv, exist_ok=True)

    # Configuración temporal de PC3 (heartbeat ágil para conmutar dentro de la ventana de 12s)
    with open(os.path.join(DIR_PC3, "config", "pc3_config.json"), encoding="utf-8") as f:
        cfg3 = json.load(f)
    cfg3["ruta_base_datos"] = os.path.join(tmp, "test_e2e.db")
    cfg3["ruta_csv"] = os.path.join(DIR_PC3, "datos", "eventos_prueba.csv")
    cfg3["respaldo"]["intervalo_heartbeat_seg"] = 1.0
    cfg3["respaldo"]["timeout_heartbeat_ms"] = 1200
    cfg3["respaldo"]["fallos_consecutivos_para_failover"] = 2
    ruta_cfg3 = os.path.join(tmp, "pc3_config.json")
    with open(ruta_cfg3, "w", encoding="utf-8") as f:
        json.dump(cfg3, f)

    # Configuración temporal de PC1 apuntando a carpeta_csv temporal
    with open(os.path.join(DIR_PC1, "config", "pc1_config.json"), encoding="utf-8") as f:
        cfg1 = json.load(f)
    cfg1["simulacion"]["carpeta_resultados"] = carpeta_csv
    cfg1["simulacion"]["timeout_ms"] = 1800
    cfg1["simulacion"]["reintentos_max"] = 1
    ruta_cfg1 = os.path.join(tmp, "pc1_config.json")
    with open(ruta_cfg1, "w", encoding="utf-8") as f:
        json.dump(cfg1, f)

    env3 = dict(os.environ, PC3_CONFIG=ruta_cfg3)
    env2 = dict(os.environ)
    env1 = dict(os.environ, PC1_CONFIG=ruta_cfg1)

    ctx = zmq.Context()
    procs: List[subprocess.Popen] = []
    pc2: Optional[subprocess.Popen] = None
    sim: Optional[subprocess.Popen] = None
    ok = False

    try:
        # 1. Levantar PC3 Persistencia, PC2 Principal y PC3 Respaldo (PASIVO)
        procs.append(lanzar(os.path.join(DIR_PC3, "servicio_persistencia.py"), [], DIR_PC3, env3))
        pc2 = lanzar(os.path.join(DIR_PC2, "iniciar_pc2.py"), [], DIR_PC2, env2)
        esperar_heartbeat(ctx, 5569)
        procs.append(
            lanzar(os.path.join(DIR_PC3, "gestor_transacciones_respaldo.py"), [], DIR_PC3, env3)
        )
        time.sleep(1.5)

        # 2. Lanzar cliente_simulador.py: 3 clientes, 12 segundos, 60 RPM
        sim = lanzar(
            os.path.join(DIR_PC1, "cliente_simulador.py"),
            [
                "--config",
                ruta_cfg1,
                "--clientes",
                "3",
                "--duracion",
                "12",
                "--rpm",
                "60",
                "--escenario",
                "e2e_failover_test",
            ],
            DIR_PC1,
            env1,
        )

        # 3. A los 5 segundos, matar abruptamente PC2 para provocar failover bajo carga
        time.sleep(5.0)
        print("[TEST PC1 E2E] >>> Simulando caída abrupta de PC2 Principal a t=5s...", flush=True)
        matar_arbol(pc2)

        # 4. Esperar a que termine cliente_simulador.py
        codigo_sim = sim.wait(timeout=45)
        assert codigo_sim == 0, f"cliente_simulador terminó con código {codigo_sim}"

        # 5. Verificar archivos CSV generados y contenido de métricas
        archivos = os.listdir(carpeta_csv)
        archivos_detalle = [a for a in archivos if a.startswith("detalle_e2e_failover_test_")]
        archivos_resumen = [a for a in archivos if a.startswith("resumen_e2e_failover_test_")]
        assert archivos_detalle and archivos_resumen, f"Faltan CSV en {archivos}"

        with open(os.path.join(carpeta_csv, archivos_detalle[0]), encoding="utf-8") as f:
            filas_detalle = list(csv.DictReader(f))
        assert len(filas_detalle) >= 15, f"Muy pocas transacciones registradas: {len(filas_detalle)}"

        nodos_vistos = {r["nodo_destino"] for r in filas_detalle if r["exito"] == "True"}
        assert "PRINCIPAL" in nodos_vistos, f"No hubo transacciones en PRINCIPAL: {nodos_vistos}"
        assert "RESPALDO" in nodos_vistos, f"No hubo conmutación a RESPALDO: {nodos_vistos}"

        # 6. Verificar consistencia total en SQLite vía PC3 (5558)
        req_bd = ctx.socket(zmq.REQ)
        req_bd.setsockopt(zmq.RCVTIMEO, 5000)
        req_bd.setsockopt(zmq.LINGER, 0)
        req_bd.connect("tcp://localhost:5558")
        try:
            req_bd.send_json(
                {
                    "operacion": "ejecutarConsulta",
                    "sql": "SELECT COUNT(*) FROM Evento WHERE puestosDisponibles < 0 "
                    "OR puestosDisponibles > puestosTotales",
                }
            )
            inconsistentes = req_bd.recv_json()["resultado"][0][0]
            assert inconsistentes == 0, f"Eventos con cupos inválidos: {inconsistentes}"

            req_bd.send_json(
                {
                    "operacion": "ejecutarConsulta",
                    "sql": "SELECT SUM(puestosTotales - puestosDisponibles) FROM Evento",
                }
            )
            descontados_eventos = int(req_bd.recv_json()["resultado"][0][0] or 0)

            req_bd.send_json(
                {
                    "operacion": "ejecutarConsulta",
                    "sql": "SELECT COALESCE(SUM(cantidadPuestos), 0) FROM Reserva "
                    "WHERE estado IN ('CONFIRMADA', 'activa')",
                }
            )
            reservados_activos = int(req_bd.recv_json()["resultado"][0][0] or 0)
            assert descontados_eventos == reservados_activos, (
                f"Desbalance de cupos: descontados={descontados_eventos} vs "
                f"reservas_activas={reservados_activos}"
            )
        finally:
            req_bd.close(linger=0)

        ok = True
    finally:
        if sim is not None and sim.poll() is None:
            detener(sim)
        if pc2 is not None and pc2.poll() is None:
            matar_arbol(pc2)
        for p in reversed(procs):
            detener(p)
        ctx.term()
        shutil.rmtree(tmp, ignore_errors=True)

    if ok:
        print("[TEST PC1 E2E] OK", flush=True)


if __name__ == "__main__":
    main()
