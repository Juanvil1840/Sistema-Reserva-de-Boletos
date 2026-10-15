"""
Módulo: gestor_transacciones.py
Capa: Capa de Servicios de Negocio (PC2)
Descripción: Actúa como la puerta de entrada (Gateway / Router) del sistema distribuido.
             Recibe las peticiones de los clientes (PC1) mediante ZeroMQ en modo síncrono,
             las clasifica y las enruta hacia el Servicio RMC a nivel local,
             esperando la respuesta y devolviéndola al cliente emisor.
             Carga su configuración de red desde config_negocio.json.
"""

import os
import json
import sys
import zmq


class GestorTransacciones:
    """
    Clase GestorTransacciones que gestiona el enrutamiento de transacciones síncronas.

    Arquitectura ZeroMQ:
    - sock_frontend_rep: Socket REP (puerto 5555) que recibe las solicitudes de los clientes (PC1).
    - sock_rmc_req: Socket REQ (puerto 5558) que enruta las peticiones hacia el Servicio RMC (PC2).
    """

    def __init__(self, ruta_config: str = None, puerto_sync: int = None, host_rmc: str = None, puerto_rmc: int = None):
        """
        Inicializa el contexto de ZeroMQ y configura los sockets de comunicación de red.
        Carga la configuración desde config_negocio.json con valores de respaldo por defecto.

        :param ruta_config: Ruta personalizada al archivo config_negocio.json (opcional).
        :param puerto_sync: Sobrescribe el puerto de escucha para clientes síncronos.
        :param host_rmc: Sobrescribe la dirección IP o hostname del Servicio RMC.
        :param puerto_rmc: Sobrescribe el puerto del Servicio RMC.
        """
        # Cargar configuración desde JSON si existe
        config = self._cargar_configuracion(ruta_config)
        cfg_gt = config.get("gestor_transacciones", {})

        self.puerto_sync = puerto_sync if puerto_sync is not None else cfg_gt.get("puerto_sync", 5555)
        self.puerto_async = cfg_gt.get("puerto_async", 5556)
        self.host_rmc = host_rmc if host_rmc is not None else cfg_gt.get("host_rmc", "localhost")
        self.puerto_rmc = puerto_rmc if puerto_rmc is not None else cfg_gt.get("puerto_rmc", 5558)

        # Contexto de ZeroMQ
        self.context = zmq.Context()

        # Socket servidor REP para escuchar peticiones de clientes (PC1)
        self.sock_frontend_rep = self.context.socket(zmq.REP)
        self.sock_frontend_rep.bind(f"tcp://*:{self.puerto_sync}")

        # Socket cliente REQ para comunicarse con el Servicio RMC local
        self.sock_rmc_req = self.context.socket(zmq.REQ)
        self.sock_rmc_req.connect(f"tcp://{self.host_rmc}:{self.puerto_rmc}")

        # Atributos del diagrama de clases reservados para siguientes entregas
        self.sock_frontend_router = None
        self.sock_cancelaciones_push = None

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
                print(f"[GestorTransacciones] Advertencia al leer {ruta_config}: {e}")
        return {}

    def clasificarTransaccion(self, peticion: dict) -> dict:
        """
        Clasifica y enruta la transacción recibida hacia el Servicio RMC.

        Para la primera entrega (alcance estricto: consulta y reserva),
        reenvía la petición JSON íntegra hacia el Servicio RMC a través
        del socket REQ y espera la respuesta producida.

        :param peticion: Diccionario JSON recibido del cliente.
        :return: Diccionario o valor devuelto por el Servicio RMC.
        """
        # Reenviar petición JSON tal cual al Servicio RMC
        self.sock_rmc_req.send_json(peticion)

        # Esperar la respuesta producida por el Servicio RMC
        respuesta = self.sock_rmc_req.recv_json()
        return respuesta

    def procesarConsultaEventos(self, mes: int):
        """
        Método de apoyo según el diagrama de clases para estructurar y
        enrutar una consulta de eventos por mes.

        :param mes: Número de mes a consultar (1 a 12).
        :return: Lista de eventos (List<Evento>).
        """
        peticion = {"accion": "consultar", "mes": mes}
        return self.clasificarTransaccion(peticion)

    def solicitarReserva(self, idCliente: str, idEvento: str, numPuestos: int) -> str:
        """
        Método de apoyo según el diagrama de clases para estructurar y
        enrutar una solicitud de reserva de boletos.

        :param idCliente: Identificador del cliente solicitante.
        :param idEvento: Identificador del evento.
        :param numPuestos: Cantidad de boletos a reservar.
        :return: Cadena con el resultado ("Éxito" o "Rechazado").
        """
        peticion = {
            "accion": "reservar",
            "idCliente": idCliente,
            "idEvento": idEvento,
            "numPuestos": numPuestos
        }
        respuesta = self.clasificarTransaccion(peticion)
        if isinstance(respuesta, dict) and "resultado" in respuesta:
            return str(respuesta["resultado"])
        return str(respuesta)

    def procesarModificacion(self, idReserva: str, nuevoIdEvento: str, nuevosPuestos: int) -> str:
        """
        Método definido en el diagrama de clases para modificación de reservas.
        Fuera del alcance estricto de la primera entrega.
        """
        pass

    def enviarCancelacion(self, idReserva: str, idCliente: str):
        """
        Método definido en el diagrama de clases para cancelaciones asíncronas.
        Fuera del alcance estricto de la primera entrega.
        """
        pass

    def run(self):
        """
        Bucle infinito principal del Gestor de Transacciones.
        Recibe peticiones JSON de los clientes, las reenvía al Servicio RMC,
        espera el resultado y lo devuelve inmediatamente al cliente.
        """
        self._activo = True
        print("[GestorTransacciones] Servidor iniciado.")
        print(f"[GestorTransacciones] Escuchando peticiones de clientes en tcp://*:{self.puerto_sync}")
        print(f"[GestorTransacciones] Enrutando hacia Servicio RMC en tcp://{self.host_rmc}:{self.puerto_rmc}")

        try:
            while self._activo:
                # 1. Recibir mensaje del cliente (PC1) en formato JSON
                peticion = self.sock_frontend_rep.recv_json()
                print(f"[GestorTransacciones] Petición recibida del cliente: {peticion}")

                # 2. Enrutar tal cual al Servicio RMC y esperar respuesta
                try:
                    respuesta = self.clasificarTransaccion(peticion)
                    print(f"[GestorTransacciones] Respuesta recibida de RMC: {respuesta}")
                except Exception as e:
                    print(f"[GestorTransacciones] Error comunicando con RMC: {e}")
                    respuesta = {"error": f"Error interno en Capa de Negocio: {str(e)}"}

                # 3. Devolver la respuesta al cliente
                self.sock_frontend_rep.send_json(respuesta)

        except KeyboardInterrupt:
            print("\n[GestorTransacciones] Interrupción recibida. Finalizando servicio...")
        finally:
            self.cerrar()

    def cerrar(self):
        """Cierra los sockets y libera el contexto de ZeroMQ de forma limpia."""
        self._activo = False
        if self.sock_frontend_rep:
            self.sock_frontend_rep.close()
        if self.sock_rmc_req:
            self.sock_rmc_req.close()
        if self.context:
            self.context.term()
        print("[GestorTransacciones] Sockets y contexto ZeroMQ cerrados correctamente.")


if __name__ == "__main__":
    gestor = GestorTransacciones()
    gestor.run()
