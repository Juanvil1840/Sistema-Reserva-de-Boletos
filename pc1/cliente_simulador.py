"""
Módulo: cliente_simulador.py
Capa: Capa de Consumidores (PC1)
Descripción: Simula múltiples clientes concurrentes enviando peticiones síncronas
             (Consulta y Reserva) al Gestor de Transacciones (PC2) vía ZeroMQ REQ/REP.
             Lee parámetros de red y carga desde config_clientes.json y recolecta
             métricas de tiempo de respuesta (promedio, desviación estándar y TPM).
"""

import os
import json
import time
import random
import statistics
import threading
import zmq


class ClienteSimulador:
    """
    Representa un cliente individual de la Capa de Consumidores (PC1).
    Respeta estrictamente las firmas y atributos del diagrama de clases actualizado.
    """

    def __init__(self, idCliente: str, ip_gt: str = "localhost", puerto_sync: int = 5555,
                 puerto_async: int = 5556, puerto_notificaciones: int = 5557,
                 solicitudes_por_minuto: int = 20, porcentaje_consulta: float = 0.7,
                 duracion_segundos: int = 60):
        # Atributos definidos en el Diagrama de Clases corregido
        self.idCliente = idCliente
        self.ip_gt = ip_gt
        self.puerto_sync = puerto_sync
        self.puerto_async = puerto_async
        self.puerto_notificaciones = puerto_notificaciones

        # Contexto y Sockets ZeroMQ (un socket REQ propio por hilo cliente)
        self.context = zmq.Context()
        self.sock_sync = self.context.socket(zmq.REQ)
        self.sock_sync.connect(f"tcp://{self.ip_gt}:{self.puerto_sync}")

        # Sockets reservados para la Entrega 2 (flujo asíncrono de cancelación)
        self.sock_async = None
        self.sock_sub = None

        # Parámetros de generación de carga
        self.solicitudes_por_minuto = max(20, solicitudes_por_minuto)  # Mínimo 20 req/min según enunciado
        self.porcentaje_consulta = porcentaje_consulta
        self.duracion_segundos = duracion_segundos

        # Almacenamiento de métricas (tiempos en milisegundos)
        self.tiempos_consulta = []
        self.tiempos_reserva = []
        self.eventos_conocidos = [f"E{i:03d}" for i in range(1, 51)]

    def consultarEventos(self, mes: int) -> list:
        """
        Envía una petición síncrona de consulta de eventos por mes al Gestor de Transacciones.
        :param mes: Mes a consultar (1 a 12).
        :return: Lista de diccionarios representando objetos Evento (List<Evento>).
        """
        peticion = {
            "accion": "consultar",
            "mes": int(mes)
        }
        inicio = time.perf_counter()
        self.sock_sync.send_json(peticion)
        respuesta = self.sock_sync.recv_json()
        fin = time.perf_counter()

        self.tiempos_consulta.append((fin - inicio) * 1000.0)  # Guardar en ms

        if isinstance(respuesta, list):
            # Actualizar caché local de IDs de eventos válidos si la consulta trajo resultados
            for ev in respuesta:
                if isinstance(ev, dict) and "idEvento" in ev:
                    if ev["idEvento"] not in self.eventos_conocidos:
                        self.eventos_conocidos.append(ev["idEvento"])
            return respuesta
        return []

    def solicitarReserva(self, idCliente: str, idEvento: str, numPuestos: int) -> str:
        """
        Envía una solicitud síncrona de reserva al Gestor de Transacciones.
        :param idCliente: ID del cliente.
        :param idEvento: ID del evento a reservar.
        :param numPuestos: Cantidad de asientos solicitados.
        :return: String con el resultado ("Éxito" o "Rechazado").
        """
        peticion = {
            "accion": "reservar",
            "idCliente": str(idCliente),
            "idEvento": str(idEvento),
            "numPuestos": int(numPuestos)
        }
        inicio = time.perf_counter()
        self.sock_sync.send_json(peticion)
        respuesta = self.sock_sync.recv_json()
        fin = time.perf_counter()

        self.tiempos_reserva.append((fin - inicio) * 1000.0)  # Guardar en ms
        return str(respuesta)

    def solicitarModificacion(self, idReserva: str, nuevoIdEvento: str, nuevosPuestos: int) -> str:
        """Definido en diagrama de clases. Fuera del alcance de la Entrega 1."""
        pass

    def solicitarCancelacion(self, idReserva: str, idCliente: str) -> str:
        """Definido en diagrama de clases. Fuera del alcance de la Entrega 1."""
        pass

    def recibirNotificacion(self, idReserva: str, estado: str) -> None:
        """Definido en diagrama de clases. Fuera del alcance de la Entrega 1."""
        pass

    def run(self) -> None:
        """
        Ejecuta el bucle generador de carga del cliente respetando la tasa de
        solicitudes por minuto y la proporción parametrizada de operaciones.
        """
        intervalo = 60.0 / float(self.solicitudes_por_minuto)
        tiempo_fin = time.time() + self.duracion_segundos

        print(f"[{self.idCliente}] Iniciado -> Meta: {self.solicitudes_por_minuto} req/min "
              f"({int(self.porcentaje_consulta * 100)}% consultas) durante {self.duracion_segundos}s.")

        try:
            while time.time() < tiempo_fin:
                t_inicio_ciclo = time.time()

                # Decidir operación según el porcentaje parametrizado
                if random.random() < self.porcentaje_consulta:
                    mes_aleatorio = random.randint(1, 12)
                    eventos = self.consultarEventos(mes_aleatorio)
                    print(f"[{self.idCliente}] Consulta (Mes {mes_aleatorio}) -> "
                          f"{len(eventos)} eventos encontrados.")
                else:
                    id_evento = random.choice(self.eventos_conocidos)
                    puestos = random.randint(1, 5)
                    resultado = self.solicitarReserva(self.idCliente, id_evento, puestos)
                    print(f"[{self.idCliente}] Reserva (Evento {id_evento}, {puestos} puestos) -> "
                          f"Resultado: {resultado}")

                # Control de ritmo para mantener las solicitudes por minuto requeridas
                t_transcurrido = time.time() - t_inicio_ciclo
                espera = max(0.0, intervalo - t_transcurrido)
                time.sleep(espera)

        except Exception as e:
            print(f"[{self.idCliente}] Error durante la ejecución: {e}")
        finally:
            self.cerrar()

    def cerrar(self):
        """Libera los recursos de ZeroMQ del cliente."""
        if self.sock_sync:
            self.sock_sync.close()
        if self.context:
            self.context.term()


def cargar_configuracion_clientes(ruta_config: str = None) -> dict:
    """Lee el archivo config_clientes.json."""
    if ruta_config is None:
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_config = os.path.join(directorio_actual, "config_clientes.json")

    if os.path.exists(ruta_config):
        with open(ruta_config, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def ejecutar_simulacion():
    """
    Orquesta el lanzamiento concurrente de múltiples clientes (hilos) y consolida
    el reporte de métricas exigidas en la Tabla 1 del enunciado.
    """
    config = cargar_configuracion_clientes()
    cfg_red = config.get("red", {})
    cfg_sim = config.get("simulacion", {})

    ip_gt = cfg_red.get("ip_gt", "localhost")
    puerto_sync = cfg_red.get("puerto_sync", 5555)
    puerto_async = cfg_red.get("puerto_async", 5556)
    puerto_notif = cfg_red.get("puerto_notificaciones", 5557)

    duracion = cfg_sim.get("duracion_segundos", 60)
    req_por_minuto = cfg_sim.get("solicitudes_por_minuto", 20)
    perfiles_clientes = cfg_sim.get("clientes", [
        {"idCliente": f"C{i:03d}", "porcentaje_consulta": 0.5} for i in range(1, 5)
    ])

    instancias_clientes = []
    hilos = []

    print("=" * 65)
    print(f" INICIANDO SIMULACIÓN DE CARGA EN PC1 ({len(perfiles_clientes)} clientes simultáneos)")
    print(f" Conectando a Gestor de Transacciones en tcp://{ip_gt}:{puerto_sync}")
    print("=" * 65)

    inicio_global = time.time()

    for perfil in perfiles_clientes:
        cliente = ClienteSimulador(
            idCliente=perfil["idCliente"],
            ip_gt=ip_gt,
            puerto_sync=puerto_sync,
            puerto_async=puerto_async,
            puerto_notificaciones=puerto_notif,
            solicitudes_por_minuto=perfil.get("solicitudes_por_minuto", req_por_minuto),
            porcentaje_consulta=perfil.get("porcentaje_consulta", 0.5),
            duracion_segundos=duracion
        )
        instancias_clientes.append(cliente)
        hilo = threading.Thread(target=cliente.run, daemon=True)
        hilos.append(hilo)
        hilo.start()

    # Esperar a que todos los clientes terminen su ciclo
    for hilo in hilos:
        hilo.join()

    tiempo_total_min = (time.time() - inicio_global) / 60.0

    # Consolidar métricas de la Tabla 1
    todas_consultas = [t for c in instancias_clientes for t in c.tiempos_consulta]
    todas_reservas = [t for c in instancias_clientes for t in c.tiempos_reserva]

    print("\n" + "=" * 65)
    print(" REPORTE DE MÉTRICAS DE DESEMPEÑO (TABLA 1)")
    print("=" * 65)

    if todas_consultas:
        prom_c = statistics.mean(todas_consultas)
        desv_c = statistics.stdev(todas_consultas) if len(todas_consultas) > 1 else 0.0
        tpm_c = len(todas_consultas) / tiempo_total_min if tiempo_total_min > 0 else 0.0
        print(f"1. Operaciones de Consulta ({len(todas_consultas)} totales):")
        print(f"   - Tiempo Promedio de Respuesta : {prom_c:.2f} ms")
        print(f"   - Desviación Estándar          : {desv_c:.2f} ms")
        print(f"   - Consultas por Minuto (TPM)   : {tpm_c:.2f} TPM")
    else:
        print("1. Operaciones de Consulta: No se ejecutaron consultas.")

    print("-" * 65)

    if todas_reservas:
        prom_r = statistics.mean(todas_reservas)
        desv_r = statistics.stdev(todas_reservas) if len(todas_reservas) > 1 else 0.0
        tpm_r = len(todas_reservas) / tiempo_total_min if tiempo_total_min > 0 else 0.0
        print(f"2. Operaciones de Reserva ({len(todas_reservas)} totales):")
        print(f"   - Tiempo Promedio de Respuesta : {prom_r:.2f} ms")
        print(f"   - Desviación Estándar          : {desv_r:.2f} ms")
        print(f"   - Reservas por Minuto (TPM)    : {tpm_r:.2f} TPM")
    else:
        print("2. Operaciones de Reserva: No se ejecutaron reservas.")
    print("=" * 65)


if __name__ == "__main__":
    ejecutar_simulacion()