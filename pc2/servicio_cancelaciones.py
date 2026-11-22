"""Worker asíncrono de Cancelaciones y Notificaciones de PC2.

Recibe tareas por PULL (``tcp://*:5559``), cancela reservas de forma atómica
en PC3 y publica el resultado por PUB (``tcp://*:5557``) en un mensaje
multiparte ``[idCliente, payload_json]`` (el tópico es el id del cliente).
"""

from __future__ import annotations

import json
import threading
from typing import Any, Dict, Optional

import zmq

from servicios.cliente_persistencia import ClientePersistencia, ErrorPersistencia
from servicios.configuracion import cargar_seccion, instalar_senales, log

ETIQUETA = "CANCELACIONES"
ESTADOS_ACTIVOS = ("CONFIRMADA", "activa")


class ServicioCancelaciones:
    """Procesa cancelaciones y notifica a los clientes."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Abre los sockets PULL y PUB y el cliente de persistencia."""
        self.stop_event = threading.Event()
        self.contexto = zmq.Context()
        self.pull = self.contexto.socket(zmq.PULL)
        self.pull.setsockopt(zmq.RCVTIMEO, 1000)
        self.pull.setsockopt(zmq.LINGER, 0)
        self.pull.bind(f"tcp://*:{config['puerto_pull']}")
        self.pub = self.contexto.socket(zmq.PUB)
        self.pub.setsockopt(zmq.LINGER, 0)
        self.pub.bind(f"tcp://*:{config['puerto_pub']}")
        self.bd = ClientePersistencia(config["host_persistencia"], config["puerto_persistencia"],
                                      config.get("timeout_persistencia_ms", 5000))
        log(ETIQUETA, f"PULL en {config['puerto_pull']}, PUB en {config['puerto_pub']}")

    def cancelar(self, id_reserva: str, id_cliente: Optional[str],
                 id_transaccion: Optional[str]) -> Dict[str, Any]:
        """Cancela la reserva atómicamente y retorna la notificación a publicar."""
        notificacion = {"idReserva": id_reserva, "idCliente": id_cliente,
                        "estado": "RECHAZADA", "exito": False, "motivo": ""}
        try:
            filas = self.bd.consultar(
                "SELECT idEvento, cantidadPuestos, idCliente, estado FROM Reserva "
                "WHERE idReserva = ?", [id_reserva])
            if not filas:
                notificacion["motivo"] = "Reserva inexistente"
            else:
                id_evento, puestos, dueno, estado = filas[0]
                notificacion["idCliente"] = id_cliente or dueno
                if estado not in ESTADOS_ACTIVOS:
                    notificacion["motivo"] = f"La reserva no está activa (estado {estado})"
                else:
                    resp = self.bd.ejecutar_transaccion([
                        {"sql": "UPDATE Reserva SET estado = 'CANCELADA', "
                                "timestamp = CURRENT_TIMESTAMP WHERE idReserva = ? "
                                "AND estado IN ('CONFIRMADA', 'activa')",
                         "parametros": [id_reserva], "minimoFilas": 1},
                        {"sql": "UPDATE Evento SET puestosDisponibles = puestosDisponibles + ? "
                                "WHERE idEvento = ?",
                         "parametros": [puestos, id_evento], "minimoFilas": 1},
                    ])
                    if resp.get("exito"):
                        notificacion.update(estado="CANCELADA", exito=True,
                                            motivo="Reserva cancelada correctamente")
                    else:
                        notificacion["motivo"] = "No se pudo cancelar (reserva ya modificada)"
        except ErrorPersistencia as e:
            notificacion["motivo"] = f"Persistencia no disponible: {e}"
        self.bd.actualizar_checkpoint(id_transaccion,
                                      "COMPLETADA" if notificacion["exito"] else "FALLIDA",
                                      json.dumps(notificacion))
        return notificacion

    def publicar(self, notificacion: Dict[str, Any]) -> None:
        """Publica ``[idCliente, payload_json]`` por el socket PUB."""
        self.pub.send_multipart([str(notificacion["idCliente"]).encode("utf-8"),
                                 json.dumps(notificacion).encode("utf-8")])

    def run(self) -> None:
        """Procesa tareas hasta que ``stop_event`` se active."""
        try:
            while not self.stop_event.is_set():
                try:
                    tarea = self.pull.recv_json()
                except zmq.Again:
                    continue
                except ValueError:
                    log(ETIQUETA, "Tarea con JSON inválido descartada")
                    continue
                log(ETIQUETA, f"Tarea: {tarea}")
                try:
                    if tarea.get("accion") != "cancelar" or not tarea.get("idReserva"):
                        log(ETIQUETA, "Tarea no soportada o sin idReserva; descartada")
                        continue
                    notificacion = self.cancelar(str(tarea["idReserva"]), tarea.get("idCliente"),
                                                 tarea.get("idTransaccion"))
                    self.publicar(notificacion)
                    log(ETIQUETA, f"Notificación publicada: {notificacion}")
                except Exception as e:  # el worker nunca debe caerse
                    log(ETIQUETA, f"Error procesando tarea: {type(e).__name__}: {e}")
        finally:
            self.pull.close(linger=0)
            self.pub.close(linger=0)
            self.bd.cerrar()
            self.contexto.term()
            log(ETIQUETA, "Detenido limpiamente.")


def main() -> None:
    """Punto de entrada del worker de cancelaciones."""
    servicio = ServicioCancelaciones(cargar_seccion("servicio_cancelaciones"))
    instalar_senales(servicio.stop_event)
    servicio.run()


if __name__ == "__main__":
    main()
