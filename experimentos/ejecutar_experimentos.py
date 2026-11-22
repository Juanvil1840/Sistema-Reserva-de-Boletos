"""Orquestador automatizado de corridas experimentales del SRB.

Ejecuta los escenarios comparativos requeridos para el Informe de Resultados:
1. Arquitectura Base (1 réplica RMC): 3, 5 y 8 clientes simultáneos.
2. Arquitectura Escalable (3 réplicas RMC): 3, 5 y 8 clientes simultáneos.
3. Escenario de Resiliencia (Inyección de Fallo): 5 clientes donde se detiene
   abruptamente PC2 a mitad de la corrida y el tráfico conmuta a PC3 Respaldo.

Puede ejecutarse en modo local (levantando y limpiando automáticamente PC3 y PC2
con una BD limpia por escenario) o apuntando a nodos remotos ya desplegados
(mediante ``--modo-remoto --host-principal <IP_PC2> --host-respaldo <IP_PC3>``).
Todos los CSV resultantes se guardan en ``experimentos/datos/``.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional

import zmq

DIR_EXP = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(DIR_EXP)
DIR_PC1 = os.path.join(RAIZ, "pc1")
DIR_PC2 = os.path.join(RAIZ, "pc2")
DIR_PC3 = os.path.join(RAIZ, "pc3")
DIR_DATOS = os.path.join(DIR_EXP, "datos")
FLAGS = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def usar_utf8() -> None:
    """Configura la salida estándar en UTF-8."""
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def lanzar(script: str, args: List[str], cwd: str, env: Dict[str, str]) -> subprocess.Popen:
    """Inicia un script Python como subproceso."""
    return subprocess.Popen(
        [sys.executable, "-u", script, *args],
        cwd=cwd,
        env=env,
        creationflags=FLAGS,
    )


def detener(proc: Optional[subprocess.Popen]) -> None:
    """Solicita parada limpia o mata al proceso si no responde."""
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()


def matar_arbol(proc: Optional[subprocess.Popen]) -> None:
    """Mata abruptamente el proceso y todos sus hijos."""
    if proc is None or proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        proc.kill()
    proc.wait()


def esperar_puerto_rep(ctx: zmq.Context, host: str, puerto: int, payload: dict, limite_s: float = 20.0) -> None:
    """Espera a que un servidor REP responda exitosamente."""
    fin = time.time() + limite_s
    while time.time() < fin:
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.setsockopt(zmq.SNDTIMEO, 1000)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://{host}:{puerto}")
        try:
            sock.send_json(payload)
            resp = sock.recv_json()
            if resp.get("exito") or resp.get("tipo") == "pong":
                return
        except zmq.ZMQError:
            time.sleep(0.3)
        finally:
            sock.close(linger=0)
    raise RuntimeError(f"Timeout esperando respuesta en {host}:{puerto}")


def limpiar_csv_previos(escenario: str) -> None:
    """Elimina CSV anteriores del mismo escenario para mantener una única corrida limpia."""
    for patron in (f"detalle_{escenario}_*.csv", f"resumen_{escenario}_*.csv"):
        for ruta in glob.glob(os.path.join(DIR_DATOS, patron)):
            try:
                os.remove(ruta)
            except OSError:
                pass


def ejecutar_escenario_local(
    ctx: zmq.Context,
    escenario: str,
    num_clientes: int,
    replicas_rmc: int,
    duracion_seg: float,
    rpm: int,
    inyectar_fallo: bool = False,
) -> None:
    """Levanta PC3 + PC2 con BD limpia, corre el simulador PC1 y detiene todo."""
    print("\n" + "#" * 88)
    print(
        f" [EXPERIMENTO] Escenario: {escenario} | Clientes: {num_clientes} | "
        f"Réplicas RMC: {replicas_rmc} | Duración: {duracion_seg}s | Fallo: {inyectar_fallo}"
    )
    print("#" * 88, flush=True)

    limpiar_csv_previos(escenario)
    tmp = tempfile.mkdtemp(prefix=f"srb_exp_{escenario}_")

    with open(os.path.join(DIR_PC3, "config", "pc3_config.json"), encoding="utf-8") as f:
        cfg3 = json.load(f)
    cfg3["ruta_base_datos"] = os.path.join(tmp, "exp.db")
    cfg3["ruta_csv"] = os.path.join(DIR_PC3, "datos", "eventos_prueba.csv")
    cfg3["respaldo"]["num_replicas_rmc"] = replicas_rmc
    cfg3["respaldo"]["intervalo_heartbeat_seg"] = 1.0
    cfg3["respaldo"]["timeout_heartbeat_ms"] = 1200
    cfg3["respaldo"]["fallos_consecutivos_para_failover"] = 2
    ruta_cfg3 = os.path.join(tmp, "pc3_config.json")
    with open(ruta_cfg3, "w", encoding="utf-8") as f:
        json.dump(cfg3, f)

    with open(os.path.join(DIR_PC1, "config", "pc1_config.json"), encoding="utf-8") as f:
        cfg1 = json.load(f)
    cfg1["simulacion"]["carpeta_resultados"] = DIR_DATOS
    cfg1["simulacion"]["timeout_ms"] = 1800
    cfg1["simulacion"]["reintentos_max"] = 1
    ruta_cfg1 = os.path.join(tmp, "pc1_config.json")
    with open(ruta_cfg1, "w", encoding="utf-8") as f:
        json.dump(cfg1, f)

    env3 = dict(os.environ, PC3_CONFIG=ruta_cfg3, NUM_REPLICAS_RMC=str(replicas_rmc))
    env2 = dict(os.environ, NUM_REPLICAS_RMC=str(replicas_rmc))
    env1 = dict(os.environ, PC1_CONFIG=ruta_cfg1)

    p_persist: Optional[subprocess.Popen] = None
    p_respaldo: Optional[subprocess.Popen] = None
    p_pc2: Optional[subprocess.Popen] = None
    p_sim: Optional[subprocess.Popen] = None

    try:
        p_persist = lanzar(os.path.join(DIR_PC3, "servicio_persistencia.py"), [], DIR_PC3, env3)
        esperar_puerto_rep(ctx, "localhost", 5558, {"operacion": "ping"})

        p_pc2 = lanzar(os.path.join(DIR_PC2, "iniciar_pc2.py"), [], DIR_PC2, env2)
        esperar_puerto_rep(ctx, "localhost", 5569, {"tipo": "ping"})

        if inyectar_fallo:
            p_respaldo = lanzar(
                os.path.join(DIR_PC3, "gestor_transacciones_respaldo.py"), [], DIR_PC3, env3
            )
            time.sleep(1.2)
        else:
            time.sleep(0.8)

        p_sim = lanzar(
            os.path.join(DIR_PC1, "cliente_simulador.py"),
            [
                "--config",
                ruta_cfg1,
                "--clientes",
                str(num_clientes),
                "--duracion",
                str(duracion_seg),
                "--rpm",
                str(rpm),
                "--escenario",
                escenario,
                "--carpeta-resultados",
                DIR_DATOS,
            ],
            DIR_PC1,
            env1,
        )

        if inyectar_fallo:
            instante_corte = max(3.0, duracion_seg * 0.4)
            time.sleep(instante_corte)
            print(
                f"[EXPERIMENTO] >>> Inyectando fallo abrupto en PC2 a t={instante_corte:.1f}s...",
                flush=True,
            )
            matar_arbol(p_pc2)

        rc = p_sim.wait(timeout=max(60, int(duracion_seg * 3)))
        if rc != 0:
            raise RuntimeError(f"El simulador terminó con código {rc} en {escenario}")
    finally:
        detener(p_sim)
        matar_arbol(p_pc2)
        detener(p_respaldo)
        detener(p_persist)
        shutil.rmtree(tmp, ignore_errors=True)
        time.sleep(0.8)


def ejecutar_escenario_remoto(
    escenario: str,
    num_clientes: int,
    duracion_seg: float,
    rpm: int,
    host_principal: str,
    host_respaldo: str,
) -> None:
    """Ejecuta cliente_simulador.py apuntando a servidores remotos ya desplegados."""
    limpiar_csv_previos(escenario)
    cmd = [
        sys.executable,
        "-u",
        os.path.join(DIR_PC1, "cliente_simulador.py"),
        "--clientes",
        str(num_clientes),
        "--duracion",
        str(duracion_seg),
        "--rpm",
        str(rpm),
        "--host-principal",
        host_principal,
        "--host-respaldo",
        host_respaldo,
        "--escenario",
        escenario,
        "--carpeta-resultados",
        DIR_DATOS,
    ]
    subprocess.run(cmd, cwd=DIR_PC1, check=True)


def main() -> None:
    """Punto de entrada del script de experimentos."""
    usar_utf8()
    parser = argparse.ArgumentParser(description="Ejecutor de Experimentos Comparativos del SRB")
    parser.add_argument(
        "--duracion",
        type=float,
        default=30.0,
        help="Duración en segundos por escenario (30s validación rápida, 120s informe oficial)",
    )
    parser.add_argument("--rpm", type=int, default=36, help="Solicitudes por minuto por cliente")
    parser.add_argument(
        "--modo-remoto",
        action="store_true",
        help="No levanta procesos locales; envía tráfico a --host-principal y --host-respaldo",
    )
    parser.add_argument("--host-principal", default="localhost", help="IP del GT Principal (PC2)")
    parser.add_argument("--host-respaldo", default="localhost", help="IP del GT Respaldo (PC3)")
    args = parser.parse_args()

    os.makedirs(DIR_DATOS, exist_ok=True)
    cargas_clientes = (3, 5, 8)

    if args.modo_remoto:
        for c in cargas_clientes:
            ejecutar_escenario_remoto(
                f"remoto_{c}_clientes",
                c,
                args.duracion,
                args.rpm,
                args.host_principal,
                args.host_respaldo,
            )
        return

    ctx = zmq.Context()
    try:
        # 1. Arquitectura Base (1 réplica RMC)
        for c in cargas_clientes:
            ejecutar_escenario_local(
                ctx,
                escenario=f"base_1rmc_{c}cli",
                num_clientes=c,
                replicas_rmc=1,
                duracion_seg=args.duracion,
                rpm=args.rpm,
                inyectar_fallo=False,
            )

        # 2. Arquitectura Escalable (3 réplicas RMC)
        for c in cargas_clientes:
            ejecutar_escenario_local(
                ctx,
                escenario=f"escalable_3rmc_{c}cli",
                num_clientes=c,
                replicas_rmc=3,
                duracion_seg=args.duracion,
                rpm=args.rpm,
                inyectar_fallo=False,
            )

        # 3. Escenario de Resiliencia (5 clientes con caída abrupta de PC2)
        ejecutar_escenario_local(
            ctx,
            escenario="resiliencia_failover_5cli",
            num_clientes=5,
            replicas_rmc=2,
            duracion_seg=max(10.0, args.duracion),
            rpm=args.rpm,
            inyectar_fallo=True,
        )
    finally:
        ctx.term()

    print("\n[EXPERIMENTOS] Todas las corridas finalizaron exitosamente.")
    print(f"[EXPERIMENTOS] Archivos CSV guardados en: {DIR_DATOS}", flush=True)


if __name__ == "__main__":
    main()
