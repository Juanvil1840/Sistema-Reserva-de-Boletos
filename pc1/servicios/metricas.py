"""Recolector thread-safe de métricas de desempeño y exportador a CSV para PC1."""

from __future__ import annotations

import csv
import os
import statistics
import threading
from datetime import datetime
from typing import Any, Dict, List, Tuple

ORDEN_OPERACIONES = (
    "consultar",
    "reservar",
    "modificar",
    "cancelar_acuse",
    "cancelar_total",
)


class RecolectorMetricas:
    """Almacena cada transacción ejecutada, calcula estadísticas y exporta CSV."""

    def __init__(self, escenario: str, carpeta_resultados: str) -> None:
        """Inicializa el recolector.

        Args:
            escenario: Nombre del escenario experimental (ej. ``base_3_clientes``).
            carpeta_resultados: Ruta del directorio donde se guardarán los CSV.
        """
        self.escenario = escenario
        self.carpeta_resultados = carpeta_resultados
        self._lock = threading.Lock()
        self._registros: List[Dict[str, Any]] = []

    def registrar(
        self,
        id_cliente: str,
        id_transaccion: str,
        operacion: str,
        nodo_destino: str,
        hubo_failover: bool,
        exito: bool,
        latencia_ms: float,
        detalle: str = "",
    ) -> None:
        """Registra una operación individual de manera thread-safe."""
        fila = {
            "timestamp_iso": datetime.now().isoformat(timespec="milliseconds"),
            "escenario": self.escenario,
            "id_cliente": id_cliente,
            "id_transaccion": id_transaccion,
            "operacion": operacion,
            "nodo_destino": nodo_destino,
            "hubo_failover": bool(hubo_failover),
            "exito": bool(exito),
            "latencia_ms": round(float(latencia_ms), 3),
            "detalle": detalle,
        }
        with self._lock:
            self._registros.append(fila)

    def obtener_registros(self) -> List[Dict[str, Any]]:
        """Retorna una copia de todos los registros capturados."""
        with self._lock:
            return list(self._registros)

    @staticmethod
    def _estadisticas_lista(latencias: List[float]) -> Dict[str, float]:
        """Calcula media, desviación estándar, mediana, mínimo y máximo."""
        if not latencias:
            return {
                "promedio_ms": 0.0,
                "desv_est_ms": 0.0,
                "mediana_ms": 0.0,
                "min_ms": 0.0,
                "max_ms": 0.0,
            }
        return {
            "promedio_ms": round(statistics.mean(latencias), 3),
            "desv_est_ms": round(statistics.stdev(latencias), 3) if len(latencias) > 1 else 0.0,
            "mediana_ms": round(statistics.median(latencias), 3),
            "min_ms": round(min(latencias), 3),
            "max_ms": round(max(latencias), 3),
        }

    def calcular_resumen(self, duracion_real_seg: float) -> List[Dict[str, Any]]:
        """Calcula métricas agregadas por tipo de operación y globalmente."""
        registros = self.obtener_registros()
        duracion_min = max(duracion_real_seg / 60.0, 1e-6)

        # Agrupar por operación respetando el orden canónico
        operaciones_presentes = list(ORDEN_OPERACIONES)
        for r in registros:
            if r["operacion"] not in operaciones_presentes:
                operaciones_presentes.append(r["operacion"])

        filas_resumen: List[Dict[str, Any]] = []
        for op in operaciones_presentes:
            sub = [r for r in registros if r["operacion"] == op]
            if not sub:
                continue
            exitosas = [r for r in sub if r["exito"]]
            fallidas = len(sub) - len(exitosas)
            failovers = sum(1 for r in sub if r["hubo_failover"])
            latencias = [float(r["latencia_ms"]) for r in exitosas] or [
                float(r["latencia_ms"]) for r in sub
            ]
            stats = self._estadisticas_lista(latencias)
            tpm = round(len(exitosas) / duracion_min, 2)
            filas_resumen.append(
                {
                    "escenario": self.escenario,
                    "operacion": op,
                    "total": len(sub),
                    "exitosas": len(exitosas),
                    "fallidas": fallidas,
                    "con_failover": failovers,
                    **stats,
                    "tpm": tpm,
                }
            )

        # Fila GLOBAL (excluyendo cancelar_total para no duplicar el conteo de solicitudes)
        primarias = [r for r in registros if r["operacion"] != "cancelar_total"]
        exitosas_glob = [r for r in primarias if r["exito"]]
        lat_glob = [float(r["latencia_ms"]) for r in exitosas_glob] or [
            float(r["latencia_ms"]) for r in primarias
        ]
        stats_glob = self._estadisticas_lista(lat_glob)
        filas_resumen.append(
            {
                "escenario": self.escenario,
                "operacion": "GLOBAL",
                "total": len(primarias),
                "exitosas": len(exitosas_glob),
                "fallidas": len(primarias) - len(exitosas_glob),
                "con_failover": sum(1 for r in primarias if r["hubo_failover"]),
                **stats_glob,
                "tpm": round(len(exitosas_glob) / duracion_min, 2),
            }
        )
        return filas_resumen

    def imprimir_y_exportar(self, duracion_real_seg: float) -> Tuple[str, str]:
        """Imprime el resumen tabular en consola y exporta los CSV de detalle y resumen.

        Returns:
            Tupla ``(ruta_detalle_csv, ruta_resumen_csv)``.
        """
        os.makedirs(self.carpeta_resultados, exist_ok=True)
        marca = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_detalle = os.path.join(
            self.carpeta_resultados, f"detalle_{self.escenario}_{marca}.csv"
        )
        ruta_resumen = os.path.join(
            self.carpeta_resultados, f"resumen_{self.escenario}_{marca}.csv"
        )

        registros = self.obtener_registros()
        campos_detalle = [
            "timestamp_iso",
            "escenario",
            "id_cliente",
            "id_transaccion",
            "operacion",
            "nodo_destino",
            "hubo_failover",
            "exito",
            "latencia_ms",
            "detalle",
        ]
        with open(ruta_detalle, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=campos_detalle)
            writer.writeheader()
            writer.writerows(registros)

        resumen = self.calcular_resumen(duracion_real_seg)
        campos_resumen = [
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
        with open(ruta_resumen, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=campos_resumen)
            writer.writeheader()
            writer.writerows(resumen)

        # Impresión tabular en consola
        print("\n" + "=" * 92)
        print(
            f" RESUMEN DE MÉTRICAS PC1 | Escenario: {self.escenario} | "
            f"Duración: {duracion_real_seg:.2f}s"
        )
        print("=" * 92)
        encabezado = (
            f"{'OPERACIÓN':<16} {'TOT':>5} {'OK':>5} {'ERR':>4} {'FO':>4} "
            f"{'PROM(ms)':>10} {'DESV(ms)':>10} {'MED(ms)':>9} {'MIN':>8} {'MAX':>8} {'TPM':>8}"
        )
        print(encabezado)
        print("-" * 92)
        for fila in resumen:
            if fila["operacion"] == "GLOBAL":
                print("-" * 92)
            print(
                f"{fila['operacion']:<16} "
                f"{fila['total']:>5} "
                f"{fila['exitosas']:>5} "
                f"{fila['fallidas']:>4} "
                f"{fila['con_failover']:>4} "
                f"{fila['promedio_ms']:>10.2f} "
                f"{fila['desv_est_ms']:>10.2f} "
                f"{fila['mediana_ms']:>9.2f} "
                f"{fila['min_ms']:>8.2f} "
                f"{fila['max_ms']:>8.2f} "
                f"{fila['tpm']:>8.2f}"
            )
        print("=" * 92)
        print(f"[METRICAS] CSV Detalle : {ruta_detalle}")
        print(f"[METRICAS] CSV Resumen : {ruta_resumen}", flush=True)
        return ruta_detalle, ruta_resumen
