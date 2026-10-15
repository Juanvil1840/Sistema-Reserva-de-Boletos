"""
Módulo: servicio_rmc.py
Capa: Capa de Servicios de Negocio (PC2)
Descripción: Actúa como el procesador de la lógica de negocio para operaciones síncronas.
             Recibe peticiones enrutadas por el Gestor de Transacciones en el puerto local,
             ejecuta las operaciones de negocio (obtenerEventosPorMes, verificarDisponibilidad,
             guardarReserva) comunicándose con la Capa de Persistencia (PC3: ServicioPersistencia)
             mediante sentencias SQL vía ZeroMQ REQ/REP y retorna el resultado al Gestor.
             Carga su configuración de red desde config_negocio.json.
"""

import os
import json
import uuid
import queue
import sys
import zmq


class ServicioRMC:
    """
    Clase ServicioRMC que ejecuta la lógica de negocio del sistema.

    Arquitectura ZeroMQ:
    - sock_in_rep: Socket REP (puerto 5558) que recibe peticiones del Gestor de Transacciones.
    - sock_persistencia_req: Socket REQ conectado a PC3 (tcp://localhost:5560) para interactuar con el DAO.
    """

    def __init__(self, ruta_config: str = None, puerto_servicio: int = None, host_persistencia: str = None, puerto_persistencia: int = None):
        """
        Inicializa el contexto ZeroMQ, sockets y cola de peticiones.
        Carga la configuración desde config_negocio.json con valores de respaldo por defecto.

        :param ruta_config: Ruta personalizada al archivo config_negocio.json (opcional).
        :param puerto_servicio: Sobrescribe el puerto local donde este servicio escucha al Gestor.
        :param host_persistencia: Sobrescribe la dirección IP o hostname de PC3.
        :param puerto_persistencia: Sobrescribe el puerto TCP del Servicio de Persistencia en PC3.
        """
        config = self._cargar_configuracion(ruta_config)
        cfg_rmc = config.get("servicio_rmc", {})

        self.puerto_servicio = puerto_servicio if puerto_servicio is not None else cfg_rmc.get("puerto_servicio", 5558)
        self.host_persistencia = host_persistencia if host_persistencia is not None else cfg_rmc.get("host_persistencia", "localhost")
        self.puerto_persistencia = puerto_persistencia if puerto_persistencia is not None else cfg_rmc.get("puerto_persistencia", 5560)

        # Contexto de ZeroMQ
        self.context = zmq.Context()

        # Socket servidor REP para recibir transacciones desde GestorTransacciones
        self.sock_in_rep = self.context.socket(zmq.REP)
        self.sock_in_rep.bind(f"tcp://*:{self.puerto_servicio}")

        # Socket cliente REQ para comunicarse con ServicioPersistencia (PC3)
        self.sock_persistencia_req = self.context.socket(zmq.REQ)
        self.sock_persistencia_req.connect(f"tcp://{self.host_persistencia}:{self.puerto_persistencia}")

        # Cola de peticiones según el diagrama de clases
        self.cola_peticiones = queue.Queue()

        self._activo = False

    def _cargar_configuracion(self, ruta_config: str = None) -> dict:
        """Carga el archivo config_negocio.json si se encuentra disponible."""
        if ruta_config is None:
            directorio_actual = os.path.dirname(os.path.abspath(__file__))
            ruta_config = os.path.join(directorio_actual, "config_negocio.json")

        if os.path.exists(ruta_config):
            try:
                with open(ruta_config, "r", encoding="utf-8") as archivo:
                    return json.load(archivo)
            except Exception as e:
                print(f"[ServicioRMC] Advertencia al leer {ruta_config}: {e}")
        return {}

    def obtenerEventosPorMes(self, mes: int) -> list:
        """
        Consulta los eventos de un mes específico interactuando con el DAO
        en PC3 mediante la operación 'ejecutarConsulta'. Mapea el ResultSet
        a una lista de diccionarios que representan la entidad Evento.

        :param mes: Mes del cual se desean consultar los eventos (1 a 12).
        :return: Lista de diccionarios (List<Evento>).
        """
        sql = "SELECT idEvento, nombre, mes, fecha, puestosTotales, puestosDisponibles FROM Evento WHERE mes = ?"
        peticion = {
            "operacion": "ejecutarConsulta",
            "sql": sql,
            "parametros": [int(mes)]
        }
        print(f"[ServicioRMC] Consultando eventos a persistencia: {peticion}")
        self.sock_persistencia_req.send_json(peticion)

        respuesta = self.sock_persistencia_req.recv_json()
        filas = respuesta.get("resultado", [])

        lista_eventos = []
        for fila in filas:
            lista_eventos.append({
                "idEvento": fila[0],
                "nombre": fila[1],
                "mes": fila[2],
                "fecha": fila[3],
                "puestosTotales": fila[4],
                "puestosDisponibles": fila[5]
            })
        return lista_eventos

    def verificarDisponibilidad(self, idEvento: str, numPuestos: int) -> bool:
        """
        Verifica en la Capa de Persistencia si el evento tiene suficientes
        puestos disponibles para satisfacer la solicitud.

        :param idEvento: Identificador del evento.
        :param numPuestos: Cantidad de puestos requeridos.
        :return: True si hay disponibilidad suficiente, False en caso contrario.
        """
        sql = "SELECT puestosDisponibles FROM Evento WHERE idEvento = ?"
        peticion = {
            "operacion": "ejecutarConsulta",
            "sql": sql,
            "parametros": [str(idEvento)]
        }
        print(f"[ServicioRMC] Verificando disponibilidad: {peticion}")
        self.sock_persistencia_req.send_json(peticion)

        respuesta = self.sock_persistencia_req.recv_json()
        filas = respuesta.get("resultado", [])

        if filas:
            puestos_disp = filas[0][0]
            return puestos_disp >= int(numPuestos)
        return False

    def guardarReserva(self, idCliente: str, idEvento: str, numPuestos: int) -> bool:
        """
        Registra la reserva en la base de datos a través de ServicioPersistencia.
        Descuenta los puestos disponibles en Evento e inserta el registro en Reserva.

        :param idCliente: Identificador del cliente.
        :param idEvento: Identificador del evento.
        :param numPuestos: Cantidad de puestos a reservar.
        :return: True si la reserva fue registrada exitosamente, False si falló.
        """
        # 1. Descontar puestos en la tabla Evento
        sql_update = "UPDATE Evento SET puestosDisponibles = puestosDisponibles - ? WHERE idEvento = ?"
        peticion_update = {
            "operacion": "ejecutarActualizacion",
            "sql": sql_update,
            "parametros": [int(numPuestos), str(idEvento)]
        }
        print(f"[ServicioRMC] Descontando puestos en persistencia: {peticion_update}")
        self.sock_persistencia_req.send_json(peticion_update)
        resp_update = self.sock_persistencia_req.recv_json()

        if not resp_update.get("exito", False):
            return False

        # 2. Insertar registro en la tabla Reserva
        id_reserva = "RES-" + str(uuid.uuid4())[:8]
        sql_insert = "INSERT INTO Reserva (idReserva, idCliente, idEvento, cantidadPuestos, estado) VALUES (?, ?, ?, ?, 'activa')"
        peticion_insert = {
            "operacion": "ejecutarActualizacion",
            "sql": sql_insert,
            "parametros": [id_reserva, str(idCliente), str(idEvento), int(numPuestos)]
        }
        print(f"[ServicioRMC] Insertando registro de reserva: {peticion_insert}")
        self.sock_persistencia_req.send_json(peticion_insert)
        resp_insert = self.sock_persistencia_req.recv_json()

        return resp_insert.get("exito", False)

    def actualizarReserva(self, idReserva: str, nuevoIdEvento: str, nuevosPuestos: int) -> bool:
        """
        Método definido en el diagrama de clases para modificación de reservas.
        Fuera del alcance estricto de la primera entrega.
        """
        pass

    def run(self):
        """
        Bucle infinito principal del Servicio RMC.
        Recibe peticiones enrutadas por el Gestor de Transacciones, ejecuta la
        lógica de negocio según la acción (Consulta -> obtenerEventosPorMes,
        Reserva -> verificarDisponibilidad + guardarReserva) y devuelve la respuesta.
        """
        self._activo = True
        print("[ServicioRMC] Servidor iniciado.")
        print(f"[ServicioRMC] Escuchando al Gestor de Transacciones en tcp://*:{self.puerto_servicio}")
        print(f"[ServicioRMC] Conectado a Persistencia (PC3) en tcp://{self.host_persistencia}:{self.puerto_persistencia}")

        try:
            while self._activo:
                # 1. Recibir petición del Gestor de Transacciones
                peticion = self.sock_in_rep.recv_json()
                print(f"[ServicioRMC] Petición recibida del Gestor: {peticion}")

                accion = str(peticion.get("accion") or peticion.get("operacion") or "").strip().lower()

                # 2. Despachar según la acción requerida
                if accion in ["consultar", "consultareventos", "obtenereventospormes"]:
                    mes = peticion.get("mes")
                    if mes is None:
                        respuesta = {"error": "El parámetro 'mes' es obligatorio para la consulta."}
                    else:
                        respuesta = self.obtenerEventosPorMes(mes)

                elif accion in ["reservar", "solicitarreserva", "guardarreserva"]:
                    id_cliente = peticion.get("idCliente") or peticion.get("id_cliente")
                    id_evento = peticion.get("idEvento") or peticion.get("id_evento")
                    num_puestos = peticion.get("numPuestos") or peticion.get("num_puestos") or peticion.get("cantidadPuestos")

                    if not (id_cliente and id_evento and num_puestos):
                        respuesta = "Rechazado"
                    else:
                        # Lógica de negocio: Verificar disponibilidad y guardar reserva
                        disponible = self.verificarDisponibilidad(id_evento, int(num_puestos))
                        if disponible and self.guardarReserva(id_cliente, id_evento, int(num_puestos)):
                            respuesta = "Éxito"
                        else:
                            respuesta = "Rechazado"

                else:
                    respuesta = {
                        "error": f"Acción '{peticion.get('accion')}' no soportada o fuera de alcance para la primera entrega."
                    }

                print(f"[ServicioRMC] Retornando respuesta al Gestor: {respuesta}")

                # 3. Retornar el resultado al Gestor de Transacciones
                self.sock_in_rep.send_json(respuesta)

        except KeyboardInterrupt:
            print("\n[ServicioRMC] Interrupción recibida. Finalizando servicio...")
        finally:
            self.cerrar()

    def cerrar(self):
        """Cierra los sockets y libera el contexto de ZeroMQ de forma limpia."""
        self._activo = False
        if self.sock_in_rep:
            self.sock_in_rep.close()
        if self.sock_persistencia_req:
            self.sock_persistencia_req.close()
        if self.context:
            self.context.term()
        print("[ServicioRMC] Sockets y contexto ZeroMQ cerrados correctamente.")


if __name__ == "__main__":
    servicio = ServicioRMC()
    servicio.run()