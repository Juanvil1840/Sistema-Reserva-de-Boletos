"""Worker RMC de respaldo (PC3): equivalente a pc2/servicio_rmc.py.

Se conecta como REP al backend DEALER del gestor de respaldo
(``tcp://localhost:5560``) y delega los datos en el servicio de persistencia
mediante transacciones atómicas con ``minimoFilas``.
"""

from __future__ import annotations

import argparse
import json
import threading
import uuid
from typing import Any, Dict, List, Optional

import zmq

from servicios.cliente_persistencia import ClientePersistencia, ErrorPersistencia
from servicios.configuracion import cargar_respaldo, instalar_senales, log, usar_utf8

ETIQUETA = "RMC-RESPALDO"
ESTADOS_ACTIVOS = ("CONFIRMADA", "activa")

SQL_DESCONTAR = ("UPDATE Evento SET puestosDisponibles = puestosDisponibles - ? "
                 "WHERE idEvento = ? AND puestosDisponibles >= ?")
SQL_DEVOLVER = "UPDATE Evento SET puestosDisponibles = puestosDisponibles + ? WHERE idEvento = ?"


class ServicioRMCRespaldo:
    """Procesa consultas, reservas y modificaciones cuando PC3 es el nodo ACTIVO."""

    def __init__(self, config: Dict[str, Any], worker_id: str = "RMC-RESPALDO-1") -> None:
        """Crea los sockets y el cliente de persistencia.

        Args:
            config: Sección ``respaldo`` de ``pc3_config.json``.
            worker_id: Identificador de la réplica (logs y respuestas).
        """
        self.worker_id = worker_id
        self.stop_event = threading.Event()
        self.contexto = zmq.Context()
        self.endpoint = f"tcp://localhost:{config['puerto_backend_rmc']}"
        self.socket = self.contexto.socket(zmq.REP)
        self.socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.endpoint)
        self.bd = ClientePersistencia(config["host_persistencia"], config["puerto_persistencia"],
                                      config.get("timeout_persistencia_ms", 5000))

    def _log(self, mensaje: str) -> None:
        """Log con etiqueta de respaldo e id de réplica."""
        log(ETIQUETA, f"({self.worker_id}) {mensaje}")

    def procesar_consulta_eventos(self, mes: int) -> Dict[str, Any]:
        """Retorna los eventos del mes indicado."""
        filas = self.bd.consultar(
            "SELECT idEvento, nombre, mes, fecha, puestosTotales, puestosDisponibles "
            "FROM Evento WHERE mes = ?", [int(mes)])
        return {"exito": True, "RespuestaConsulta": filas, "eventos": filas}

    def solicitar_reserva(self, id_cliente: str, id_evento: str, num_puestos: int,
                          id_transaccion: Optional[str] = None) -> Dict[str, Any]:
        """Descuenta puestos y crea la reserva en una sola transacción atómica."""
        id_reserva = "RES-" + uuid.uuid4().hex[:8]
        resp = self.bd.ejecutar_transaccion([
            {"sql": SQL_DESCONTAR, "parametros": [num_puestos, id_evento, num_puestos],
             "minimoFilas": 1},
            {"sql": "INSERT INTO Reserva (idReserva, idCliente, idEvento, cantidadPuestos, estado) "
                    "VALUES (?, ?, ?, ?, 'CONFIRMADA')",
             "parametros": [id_reserva, id_cliente, id_evento, num_puestos], "minimoFilas": 1},
        ])
        if resp.get("exito"):
            resultado = {"exito": True, "RespuestaReserva": "Éxito", "idReserva": id_reserva}
            self.bd.actualizar_checkpoint(id_transaccion, "COMPLETADA", json.dumps(resultado))
        else:
            resultado = {"exito": False, "RespuestaReserva": "Rechazado",
                         "motivo": "Sin cupo disponible o evento inexistente"}
            self.bd.actualizar_checkpoint(id_transaccion, "FALLIDA", json.dumps(resultado))
        return resultado

    def procesar_modificacion(self, id_reserva: str, nuevo_id_evento: str, nuevos_puestos: int,
                              id_transaccion: Optional[str] = None) -> Dict[str, Any]:
        """Modifica cantidad y/o evento de una reserva de forma atómica."""
        def rechazo(motivo: str) -> Dict[str, Any]:
            res = {"exito": False, "RespuestaModificacion": "Rechazado", "motivo": motivo}
            self.bd.actualizar_checkpoint(id_transaccion, "FALLIDA", json.dumps(res))
            return res

        if nuevos_puestos <= 0:
            return rechazo("La cantidad de puestos debe ser mayor que cero")
        filas = self.bd.consultar(
            "SELECT idEvento, cantidadPuestos, estado FROM Reserva WHERE idReserva = ?",
            [id_reserva])
        if not filas:
            return rechazo("Reserva inexistente")
        evento_actual, cant_actual, estado = filas[0]
        if estado not in ESTADOS_ACTIVOS:
            return rechazo(f"La reserva no está activa (estado {estado})")

        sql_reserva = ("UPDATE Reserva SET idEvento = ?, cantidadPuestos = ?, "
                       "timestamp = CURRENT_TIMESTAMP WHERE idReserva = ? AND idEvento = ? "
                       "AND cantidadPuestos = ? AND estado IN ('CONFIRMADA', 'activa')")
        params_reserva = [nuevo_id_evento, nuevos_puestos, id_reserva, evento_actual, cant_actual]
        operaciones: List[Dict[str, Any]] = []
        if nuevo_id_evento == evento_actual:
            diff = nuevos_puestos - cant_actual
            if diff > 0:
                operaciones.append({"sql": SQL_DESCONTAR, "parametros": [diff, evento_actual, diff],
                                    "minimoFilas": 1})
            elif diff < 0:
                operaciones.append({"sql": SQL_DEVOLVER, "parametros": [-diff, evento_actual],
                                    "minimoFilas": 1})
        else:
            operaciones.append({"sql": SQL_DEVOLVER, "parametros": [cant_actual, evento_actual],
                                "minimoFilas": 1})
            operaciones.append({"sql": SQL_DESCONTAR,
                                "parametros": [nuevos_puestos, nuevo_id_evento, nuevos_puestos],
                                "minimoFilas": 1})
        operaciones.append({"sql": sql_reserva, "parametros": params_reserva, "minimoFilas": 1})

        if not self.bd.ejecutar_transaccion(operaciones).get("exito"):
            return rechazo("Sin cupo suficiente, evento inexistente o reserva modificada")
        resultado = {"exito": True, "RespuestaModificacion": "Éxito", "idReserva": id_reserva}
        self.bd.actualizar_checkpoint(id_transaccion, "COMPLETADA", json.dumps(resultado))
        return resultado

    def despachar(self, p: Dict[str, Any]) -> Dict[str, Any]:
        """Enruta la petición según ``accion``."""
        accion = str(p.get("accion") or p.get("operacion") or "").strip().lower()
        id_tx = p.get("idTransaccion")
        if accion in ("consultar", "consultareventos"):
            if p.get("mes") is None:
                return {"exito": False, "error": "El parámetro 'mes' es obligatorio"}
            return self.procesar_consulta_eventos(p["mes"])
        if accion in ("reservar", "solicitarreserva"):
            puestos = int(p.get("numPuestos") or p.get("cantidadPuestos") or 0)
            if not (p.get("idCliente") and p.get("idEvento")) or puestos <= 0:
                res = {"exito": False, "RespuestaReserva": "Rechazado",
                       "motivo": "Datos de reserva incompletos o inválidos"}
                self.bd.actualizar_checkpoint(id_tx, "FALLIDA", json.dumps(res))
                return res
            return self.solicitar_reserva(str(p["idCliente"]), str(p["idEvento"]), puestos, id_tx)
        if accion in ("modificar", "modificarreserva"):
            if not (p.get("idReserva") and p.get("nuevoIdEvento")):
                return {"exito": False, "RespuestaModificacion": "Rechazado",
                        "motivo": "Datos de modificación incompletos"}
            return self.procesar_modificacion(str(p["idReserva"]), str(p["nuevoIdEvento"]),
                                              int(p.get("nuevosPuestos") or 0), id_tx)
        return {"exito": False, "error": f"Acción '{accion}' no soportada"}

    def run(self) -> None:
        """Atiende peticiones hasta que ``stop_event`` se active."""
        self._log(f"Conectado al backend {self.endpoint}")
        try:
            while not self.stop_event.is_set():
                try:
                    peticion = self.socket.recv_json()
                except zmq.Again:
                    continue
                except ValueError as e:
                    self.socket.send_json({"exito": False, "error": f"JSON inválido: {e}"})
                    continue
                self._log(f"Petición: {peticion}")
                try:
                    respuesta = self.despachar(peticion)
                except ErrorPersistencia as e:
                    respuesta = {"exito": False, "error": f"Persistencia no disponible: {e}"}
                except Exception as e:  # el worker nunca debe caerse
                    respuesta = {"exito": False, "error": f"{type(e).__name__}: {e}"}
                respuesta["procesadoPor"] = self.worker_id
                self._log(f"Respuesta: exito={respuesta.get('exito')}")
                self.socket.send_json(respuesta)
        finally:
            self.socket.close(linger=0)
            self.bd.cerrar()
            self.contexto.term()
            self._log("Detenido limpiamente.")


def main() -> None:
    """Punto de entrada del worker RMC de respaldo."""
    usar_utf8()
    parser = argparse.ArgumentParser(description="Worker RMC de respaldo (PC3)")
    parser.add_argument("--worker-id", default="RMC-RESPALDO-1")
    args = parser.parse_args()
    servicio = ServicioRMCRespaldo(cargar_respaldo(), args.worker_id)
    instalar_senales(servicio.stop_event)
    servicio.run()


if __name__ == "__main__":
    main()
