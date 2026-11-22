"""Acceso a SQLite para la Capa de Almacenamiento (PC3) del SRB.

Este módulo contiene exclusivamente la clase :class:`BaseDatosSRB`, que
administra la conexión, el esquema, la carga inicial de eventos y las
operaciones de checkpoint de transacciones (interfaz ICheckpointTransaccion).
No contiene lógica de negocio.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

ESTADOS_CHECKPOINT = ("RECIBIDA", "EN_PROCESO", "COMPLETADA", "FALLIDA")
ESTADOS_PENDIENTES = ("RECIBIDA", "EN_PROCESO")


def _ahora() -> str:
    """Retorna la fecha y hora actual en formato ISO 8601 (segundos)."""
    return datetime.now().isoformat(timespec="seconds")


class BaseDatosSRB:
    """Encapsula el acceso concurrente y seguro a la base SQLite del SRB.

    Usa una única conexión compartida (``check_same_thread=False``) protegida
    por un ``threading.Lock`` y configurada en modo WAL.
    """

    def __init__(self, ruta_db: str, ruta_csv: Optional[str] = None) -> None:
        """Abre la base, crea el esquema y puebla los eventos si está vacía.

        Args:
            ruta_db: Ruta del archivo SQLite (se crean los directorios faltantes).
            ruta_csv: Ruta del CSV de eventos para la carga inicial (opcional).
        """
        self.ruta_db = ruta_db
        self.ruta_csv = ruta_csv
        self.lock = threading.Lock()
        directorio = os.path.dirname(os.path.abspath(ruta_db))
        os.makedirs(directorio, exist_ok=True)
        self.conexion = sqlite3.connect(ruta_db, check_same_thread=False)
        self.conexion.execute("PRAGMA journal_mode=WAL;")
        self.conexion.execute("PRAGMA synchronous=NORMAL;")
        self._crear_tablas()
        self._poblar_eventos()

    # ------------------------------------------------------------------
    # Inicialización
    # ------------------------------------------------------------------
    def _crear_tablas(self) -> None:
        """Crea las tablas Evento, Cliente, Reserva y BitacoraTransacciones."""
        with self.lock:
            self.conexion.executescript(
                """
                CREATE TABLE IF NOT EXISTS Evento (
                    idEvento TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    mes INTEGER NOT NULL,
                    fecha TEXT NOT NULL,
                    puestosTotales INTEGER NOT NULL,
                    puestosDisponibles INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS Cliente (
                    idCliente TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    tipo TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS Reserva (
                    idReserva TEXT PRIMARY KEY,
                    idCliente TEXT NOT NULL,
                    idEvento TEXT NOT NULL,
                    cantidadPuestos INTEGER NOT NULL,
                    estado TEXT NOT NULL DEFAULT 'activa'
                        CHECK (estado IN ('activa', 'CONFIRMADA', 'CANCELADA')),
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(idCliente) REFERENCES Cliente(idCliente),
                    FOREIGN KEY(idEvento) REFERENCES Evento(idEvento)
                );
                CREATE TABLE IF NOT EXISTS BitacoraTransacciones (
                    idTransaccion TEXT PRIMARY KEY,
                    tipoOperacion TEXT,
                    idCliente TEXT,
                    payloadJson TEXT,
                    estado TEXT,
                    resultadoJson TEXT,
                    timestampActualizacion TEXT
                );
                """
            )
            self.conexion.commit()

    def _poblar_eventos(self) -> None:
        """Carga los eventos del CSV y 10 clientes de prueba si Evento está vacía."""
        with self.lock:
            if self.conexion.execute("SELECT COUNT(*) FROM Evento").fetchone()[0] > 0:
                return
            if not self.ruta_csv or not os.path.isfile(self.ruta_csv):
                print(f"[{_ahora()}] [BD] Advertencia: CSV no encontrado: {self.ruta_csv}")
                return
            eventos = []
            with open(self.ruta_csv, encoding="utf-8", newline="") as f:
                lector = csv.reader(f)
                next(lector, None)  # encabezado
                for fila in lector:
                    if not fila:
                        continue
                    if len(fila) == 5:
                        id_ev, nombre, mes, total, disp = fila
                        fecha = f"2026-{int(mes):02d}-15"
                    else:
                        id_ev, nombre, mes, fecha, total, disp = fila[:6]
                    eventos.append((id_ev, nombre, int(mes), fecha, int(total), int(disp)))
            try:
                self.conexion.executemany(
                    "INSERT INTO Evento (idEvento, nombre, mes, fecha, puestosTotales,"
                    " puestosDisponibles) VALUES (?, ?, ?, ?, ?, ?)",
                    eventos,
                )
                self.conexion.executemany(
                    "INSERT OR IGNORE INTO Cliente VALUES (?, ?, ?)",
                    [(f"C{i:03d}", f"Cliente Prueba {i}", "Estudiante") for i in range(1, 11)],
                )
                self.conexion.commit()
            except sqlite3.Error:
                self.conexion.rollback()
                raise
            print(f"[{_ahora()}] [BD] {len(eventos)} eventos cargados desde el CSV.")

    # ------------------------------------------------------------------
    # Operaciones SQL genéricas
    # ------------------------------------------------------------------
    def ejecutar_consulta(self, sql: str, params: Sequence[Any] = ()) -> List[List[Any]]:
        """Ejecuta un SELECT y retorna las filas como listas.

        Args:
            sql: Sentencia SELECT parametrizada.
            params: Parámetros posicionales.

        Raises:
            sqlite3.Error: Si la sentencia falla.
        """
        with self.lock:
            return [list(f) for f in self.conexion.execute(sql, tuple(params)).fetchall()]

    def ejecutar_actualizacion(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Ejecuta un INSERT/UPDATE/DELETE con commit (rollback si falla).

        Returns:
            Número de filas afectadas.
        """
        with self.lock:
            try:
                cur = self.conexion.execute(sql, tuple(params))
                self.conexion.commit()
                return cur.rowcount
            except sqlite3.Error:
                self.conexion.rollback()
                raise

    def ejecutar_transaccion_atomica(self, operaciones: Sequence[Dict[str, Any]]) -> List[int]:
        """Ejecuta varias sentencias en un único commit bajo el mismo Lock.

        Si cualquier sentencia falla se hace rollback de todas. Una sentencia
        con ``"minimoFilas"`` (entero) falla si afecta menos filas, lo que
        permite abortar la reserva si no hay puestos suficientes.

        Args:
            operaciones: Lista de ``{"sql": str, "parametros": list, "minimoFilas": int?}``.

        Returns:
            Lista de filas afectadas por cada sentencia.

        Raises:
            sqlite3.Error: Si alguna sentencia falla (tras el rollback).
            ValueError: Si no se cumple ``minimoFilas``.
        """
        with self.lock:
            try:
                afectadas: List[int] = []
                for op in operaciones:
                    cur = self.conexion.execute(op["sql"], tuple(op.get("parametros", [])))
                    minimo = op.get("minimoFilas")
                    if minimo is not None and cur.rowcount < int(minimo):
                        raise ValueError(
                            f"La sentencia afectó {cur.rowcount} filas (mínimo {minimo})."
                        )
                    afectadas.append(cur.rowcount)
                self.conexion.commit()
                return afectadas
            except (sqlite3.Error, ValueError, KeyError):
                self.conexion.rollback()
                raise

    # ------------------------------------------------------------------
    # Checkpointing (ICheckpointTransaccion)
    # ------------------------------------------------------------------
    @staticmethod
    def _validar_estado(estado: str) -> None:
        """Valida que el estado pertenezca a los permitidos."""
        if estado not in ESTADOS_CHECKPOINT:
            raise ValueError(f"Estado inválido '{estado}'. Permitidos: {ESTADOS_CHECKPOINT}")

    def registrar_checkpoint(self, id_transaccion: str, tipo_operacion: str,
                             id_cliente: str, payload_json: str,
                             estado: str = "RECIBIDA") -> None:
        """Registra (o reemplaza) una transacción en la bitácora."""
        self._validar_estado(estado)
        self.ejecutar_actualizacion(
            "INSERT OR REPLACE INTO BitacoraTransacciones (idTransaccion, tipoOperacion,"
            " idCliente, payloadJson, estado, resultadoJson, timestampActualizacion)"
            " VALUES (?, ?, ?, ?, ?, NULL, ?)",
            (id_transaccion, tipo_operacion, id_cliente, payload_json, estado, _ahora()),
        )

    def actualizar_checkpoint(self, id_transaccion: str, estado: str,
                              resultado_json: Optional[str] = None) -> bool:
        """Actualiza estado y resultado de una transacción.

        Returns:
            True si la transacción existía y fue actualizada.
        """
        self._validar_estado(estado)
        filas = self.ejecutar_actualizacion(
            "UPDATE BitacoraTransacciones SET estado = ?, resultadoJson = ?,"
            " timestampActualizacion = ? WHERE idTransaccion = ?",
            (estado, resultado_json, _ahora(), id_transaccion),
        )
        return filas > 0

    _COLUMNAS_CP = ("idTransaccion", "tipoOperacion", "idCliente", "payloadJson",
                    "estado", "resultadoJson", "timestampActualizacion")

    def consultar_checkpoint(self, id_transaccion: str) -> Optional[Dict[str, Any]]:
        """Retorna el checkpoint como diccionario, o None si no existe."""
        filas = self.ejecutar_consulta(
            f"SELECT {', '.join(self._COLUMNAS_CP)} FROM BitacoraTransacciones"
            " WHERE idTransaccion = ?", (id_transaccion,))
        return dict(zip(self._COLUMNAS_CP, filas[0])) if filas else None

    def listar_checkpoints_pendientes(self) -> List[Dict[str, Any]]:
        """Lista transacciones en estado RECIBIDA o EN_PROCESO."""
        filas = self.ejecutar_consulta(
            f"SELECT {', '.join(self._COLUMNAS_CP)} FROM BitacoraTransacciones"
            " WHERE estado IN ('RECIBIDA', 'EN_PROCESO') ORDER BY timestampActualizacion")
        return [dict(zip(self._COLUMNAS_CP, f)) for f in filas]

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------
    def estado_salud(self) -> Dict[str, Any]:
        """Retorna conteos de eventos y reservas para el ``ping``."""
        return {
            "bd": "OK",
            "eventos": self.ejecutar_consulta("SELECT COUNT(*) FROM Evento")[0][0],
            "reservas": self.ejecutar_consulta("SELECT COUNT(*) FROM Reserva")[0][0],
        }

    def cerrar(self) -> None:
        """Cierra la conexión a la base de datos."""
        with self.lock:
            self.conexion.close()
