"""Generador de gráficas PNG (300 DPI) y tabla consolidada CSV para el SRB.

Lee los archivos ``resumen_*.csv`` y ``detalle_*.csv`` ubicados en
``experimentos/datos/`` y genera en ``experimentos/graficas/``:
  - ``01_latencia_promedio_comparativa.png``
  - ``02_tpm_comparativa.png``
  - ``03_resiliencia_failover_temporal.png``
  - ``04_tabla_consolidada.csv``
"""

from __future__ import annotations

import csv
import glob
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

DIR_EXP = os.path.dirname(os.path.abspath(__file__))
DIR_DATOS = os.path.join(DIR_EXP, "datos")
DIR_GRAFICAS = os.path.join(DIR_EXP, "graficas")

OPERACIONES = (
    "consultar",
    "reservar",
    "modificar",
    "cancelar_acuse",
    "cancelar_total",
)
CLIENTES = (3, 5, 8)


def usar_utf8() -> None:
    """Configura la salida estándar en UTF-8."""
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def _ultimo_archivo(patron: str) -> Optional[str]:
    """Retorna la ruta del archivo más reciente que coincida con el patrón en datos/."""
    rutas = sorted(glob.glob(os.path.join(DIR_DATOS, patron)))
    return rutas[-1] if rutas else None


def cargar_resumenes() -> Dict[str, List[Dict[str, Any]]]:
    """Carga el CSV de resumen más reciente para cada escenario."""
    resultado: Dict[str, List[Dict[str, Any]]] = {}
    for ruta in sorted(glob.glob(os.path.join(DIR_DATOS, "resumen_*.csv"))):
        with open(ruta, encoding="utf-8") as f:
            filas = list(csv.DictReader(f))
        if filas:
            esc = filas[0]["escenario"]
            resultado[esc] = filas
    return resultado


def exportar_tabla_consolidada(resumenes: Dict[str, List[Dict[str, Any]]]) -> str:
    """Genera 04_tabla_consolidada.csv con todas las filas de resumen ordenadas."""
    os.makedirs(DIR_GRAFICAS, exist_ok=True)
    ruta_salida = os.path.join(DIR_GRAFICAS, "04_tabla_consolidada.csv")
    campos = [
        "escenario",
        "operacion",
        "total",
        "exitosas",
        "fallidas",
        "con_failover",
        "promedio_ms",
        "desv_est_ms",
        "mediana_ms",
        "min_ms",
        "max_ms",
        "tpm",
    ]
    todas: List[Dict[str, Any]] = []
    for esc in sorted(resumenes.keys()):
        todas.extend(resumenes[esc])

    with open(ruta_salida, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(todas)
    return ruta_salida


def _buscar_metrica(
    resumenes: Dict[str, List[Dict[str, Any]]],
    escenario: str,
    operacion: str,
    campo: str,
) -> float:
    """Busca un valor numérico en la tabla de resumen."""
    for fila in resumenes.get(escenario, []):
        if fila.get("operacion") == operacion:
            try:
                return float(fila.get(campo, 0.0))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def graficar_latencia_comparativa(plt: Any, resumenes: Dict[str, List[Dict[str, Any]]]) -> str:
    """Genera 01_latencia_promedio_comparativa.png (Base 1 RMC vs. Escalable 3 RMC)."""
    fig, ejes = plt.subplots(1, 3, figsize=(16, 5.2), sharey=False)
    fig.suptitle(
        "Tiempo de Respuesta Promedio y Desviación Estándar por Operación\n"
        "Arquitectura Base (1 Réplica RMC) vs. Arquitectura Escalable (3 Réplicas RMC)",
        fontsize=13,
        fontweight="bold",
    )

    etiquetas_op = ["Consulta", "Reserva", "Modif.", "Canc. Acuse", "Canc. Total"]
    x_pos = list(range(len(OPERACIONES)))
    ancho = 0.36

    for idx, c in enumerate(CLIENTES):
        ax = ejes[idx]
        esc_base = f"base_1rmc_{c}cli"
        esc_esc = f"escalable_3rmc_{c}cli"

        prom_base = [_buscar_metrica(resumenes, esc_base, op, "promedio_ms") for op in OPERACIONES]
        desv_base = [_buscar_metrica(resumenes, esc_base, op, "desv_est_ms") for op in OPERACIONES]
        prom_esc = [_buscar_metrica(resumenes, esc_esc, op, "promedio_ms") for op in OPERACIONES]
        desv_esc = [_buscar_metrica(resumenes, esc_esc, op, "desv_est_ms") for op in OPERACIONES]

        x_b = [x - ancho / 2 for x in x_pos]
        x_e = [x + ancho / 2 for x in x_pos]

        ax.bar(
            x_b,
            prom_base,
            width=ancho,
            yerr=desv_base,
            capsize=3,
            label="Base (1 RMC)",
            color="#3b82f6",
            alpha=0.88,
            edgecolor="#1e3a8a",
        )
        ax.bar(
            x_e,
            prom_esc,
            width=ancho,
            yerr=desv_esc,
            capsize=3,
            label="Escalable (3 RMC)",
            color="#10b981",
            alpha=0.88,
            edgecolor="#065f46",
        )

        ax.set_title(f"{c} Clientes Simultáneos", fontsize=11, fontweight="bold")
        ax.set_xticks(x_pos)
        ax.set_xticklabels(etiquetas_op, rotation=22, fontsize=9)
        ax.set_ylabel("Latencia (ms)" if idx == 0 else "")
        ax.grid(axis="y", linestyle="--", alpha=0.45)
        ax.set_ylim(bottom=0)
        if idx == 0:
            ax.legend(loc="upper left", fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.90])
    ruta = os.path.join(DIR_GRAFICAS, "01_latencia_promedio_comparativa.png")
    fig.savefig(ruta, dpi=300)
    plt.close(fig)
    return ruta


def graficar_tpm_comparativa(plt: Any, resumenes: Dict[str, List[Dict[str, Any]]]) -> str:
    """Genera 02_tpm_comparativa.png (Throughput TPM vs. número de clientes)."""
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    tpm_base = [_buscar_metrica(resumenes, f"base_1rmc_{c}cli", "GLOBAL", "tpm") for c in CLIENTES]
    tpm_esc = [_buscar_metrica(resumenes, f"escalable_3rmc_{c}cli", "GLOBAL", "tpm") for c in CLIENTES]

    x_pos = list(range(len(CLIENTES)))
    ancho = 0.34
    b1 = ax.bar(
        [x - ancho / 2 for x in x_pos],
        tpm_base,
        width=ancho,
        label="Arquitectura Base (1 RMC)",
        color="#3b82f6",
        edgecolor="#1e3a8a",
    )
    b2 = ax.bar(
        [x + ancho / 2 for x in x_pos],
        tpm_esc,
        width=ancho,
        label="Arquitectura Escalable (3 RMC)",
        color="#10b981",
        edgecolor="#065f46",
    )

    for barras in (b1, b2):
        for barra in barras:
            altura = barra.get_height()
            ax.annotate(
                f"{altura:.1f}",
                xy=(barra.get_x() + barra.get_width() / 2, altura),
                xytext=(0, 4),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )

    ax.set_title(
        "Rendimiento Global: Transacciones Procesadas por Minuto (TPM)\n"
        "Frente al Incremento de Clientes Concurrentes",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_xticks(x_pos)
    ax.set_xticklabels([f"{c} Clientes" for c in CLIENTES], fontsize=10)
    ax.set_ylabel("Transacciones Por Minuto (TPM)", fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.45)
    ax.legend(fontsize=10)
    max_y = max(tpm_base + tpm_esc + [10.0])
    ax.set_ylim(0, max_y * 1.18)

    fig.tight_layout()
    ruta = os.path.join(DIR_GRAFICAS, "02_tpm_comparativa.png")
    fig.savefig(ruta, dpi=300)
    plt.close(fig)
    return ruta


def graficar_resiliencia_temporal(plt: Any) -> Optional[str]:
    """Genera 03_resiliencia_failover_temporal.png a partir del detalle de resiliencia."""
    ruta_detalle = _ultimo_archivo("detalle_resiliencia_failover_*.csv")
    if not ruta_detalle:
        return None

    with open(ruta_detalle, encoding="utf-8") as f:
        filas = [r for r in csv.DictReader(f) if r.get("operacion") != "cancelar_total"]
    if not filas:
        return None

    t0 = datetime.fromisoformat(filas[0]["timestamp_iso"])
    tiempos_principal: List[float] = []
    lat_principal: List[float] = []
    tiempos_respaldo: List[float] = []
    lat_respaldo: List[float] = []
    instante_failover: Optional[float] = None

    for r in filas:
        t_rel = (datetime.fromisoformat(r["timestamp_iso"]) - t0).total_seconds()
        lat = float(r["latencia_ms"])
        if r["nodo_destino"] == "RESPALDO":
            if instante_failover is None:
                instante_failover = t_rel
            tiempos_respaldo.append(t_rel)
            lat_respaldo.append(lat)
        else:
            tiempos_principal.append(t_rel)
            lat_principal.append(lat)

    fig, ax = plt.subplots(figsize=(10, 5.2))
    if tiempos_principal:
        ax.scatter(
            tiempos_principal,
            lat_principal,
            color="#2563eb",
            label="Atendido por GT Principal (PC2)",
            s=36,
            alpha=0.85,
        )
    if tiempos_respaldo:
        ax.scatter(
            tiempos_respaldo,
            lat_respaldo,
            color="#d97706",
            marker="^",
            label="Atendido por GT Respaldo (PC3)",
            s=46,
            alpha=0.9,
        )

    if instante_failover is not None:
        ax.axvline(
            instante_failover,
            color="#dc2626",
            linestyle="--",
            linewidth=2.0,
            label=f"Conmutación a Respaldo (t={instante_failover:.1f}s)",
        )

    ax.set_title(
        "Escenario de Resiliencia: Serie Temporal de Latencia Durante Caída Abrupta de PC2\n"
        "Detección por Heartbeat, Recuperación de Checkpoints y Continuidad en PC3",
        fontsize=11.5,
        fontweight="bold",
    )
    ax.set_xlabel("Tiempo transcurrido desde inicio de la corrida (segundos)", fontsize=10)
    ax.set_ylabel("Latencia de la transacción (ms)", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.45)
    ax.legend(loc="upper right", fontsize=9.5)

    fig.tight_layout()
    ruta = os.path.join(DIR_GRAFICAS, "03_resiliencia_failover_temporal.png")
    fig.savefig(ruta, dpi=300)
    plt.close(fig)
    return ruta


def main() -> int:
    """Punto de entrada del generador de gráficas."""
    usar_utf8()
    resumenes = cargar_resumenes()
    if not resumenes:
        print(
            f"[GRAFICAS] No se encontraron archivos resumen_*.csv en '{DIR_DATOS}'.\n"
            "Ejecuta primero: python experimentos/ejecutar_experimentos.py"
        )
        return 1

    os.makedirs(DIR_GRAFICAS, exist_ok=True)
    ruta_tabla = exportar_tabla_consolidada(resumenes)
    print(f"[GRAFICAS] Tabla consolidada exportada: {ruta_tabla}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print(
            "\n[GRAFICAS] Aviso: 'matplotlib' no está instalado en este intérprete de Python.\n"
            "Para generar las imágenes PNG de 300 DPI instala la dependencia con:\n"
            "  pip install -r experimentos/requirements_graficas.txt\n"
            "y vuelve a ejecutar: python experimentos/generar_graficas.py"
        )
        return 1

    r1 = graficar_latencia_comparativa(plt, resumenes)
    print(f"[GRAFICAS] Generada: {r1}")
    r2 = graficar_tpm_comparativa(plt, resumenes)
    print(f"[GRAFICAS] Generada: {r2}")
    r3 = graficar_resiliencia_temporal(plt)
    if r3:
        print(f"[GRAFICAS] Generada: {r3}")
    print("[GRAFICAS] Todas las gráficas (300 DPI) se generaron exitosamente.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
