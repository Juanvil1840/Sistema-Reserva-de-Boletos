import sqlite3
import threading
import zmq
import os
import csv


class ServicioPersistencia:
    """
    Clase que actúa estrictamente como Data Access Object (DAO) para SQLite.
    Según el diagrama de clases y los diagramas de secuencia, 
    esta capa no contiene lógica de negocio; únicamente administra la conexión,
    controla la concurrencia con threading.Lock y atiende peticiones remotas 
    (ejecutarConsulta y ejecutarActualizacion) mediante un socket ZeroMQ REP.
    """
    def __init__(self, db_filename="base_datos.db", puerto_rep=5560):
        # Determinar la ruta absoluta para crear la base de datos en el directorio actual
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(directorio_actual, db_filename)
        
        # Atributos definidos en el diagrama de clases corregido
        self.conexion = None
        self.lock = threading.Lock()
        self.puerto_rep = puerto_rep
        self.context = zmq.Context()
        self.sock_rep = None
        
        # Inicializar el esquema de base de datos y cargar datos de prueba
        self.init_db()
        self.poblar_datos_prueba()

    def init_db(self):
        """
        Crea la estructura de tablas (Evento, Cliente, Reserva) respetando 
        exactamente los atributos de las entidades del diagrama de clases corregido.
        """
        with self.lock:
            self.conexion = sqlite3.connect(self.db_path)
            cursor = self.conexion.cursor()

            # Tabla Evento: incluye fecha, puestosTotales y puestosDisponibles
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Evento (
                    idEvento TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    mes INTEGER NOT NULL,
                    fecha TEXT NOT NULL,
                    puestosTotales INTEGER NOT NULL,
                    puestosDisponibles INTEGER NOT NULL
                )
            ''')

            # Tabla Cliente: idCliente, nombre y tipo
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Cliente (
                    idCliente TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    tipo TEXT NOT NULL
                )
            ''')

            # Tabla Reserva: incluye cantidadPuestos, estado y timestamp
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Reserva (
                    idReserva TEXT PRIMARY KEY,
                    idCliente TEXT NOT NULL,
                    idEvento TEXT NOT NULL,
                    cantidadPuestos INTEGER NOT NULL,
                    estado TEXT NOT NULL DEFAULT 'activa',
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(idCliente) REFERENCES Cliente(idCliente),
                    FOREIGN KEY(idEvento) REFERENCES Evento(idEvento)
                )
            ''')

            self.conexion.commit()
            self.conexion.close()

    def poblar_datos_prueba(self):
        """
        Puebla la base de datos con los 50 eventos desde 'eventos_prueba.csv'
        y 10 clientes de prueba iniciales si la base de datos está vacía.
        Soporta archivos CSV tanto de 5 columnas (generando la fecha) como de 6 columnas.
        """
        with self.lock:
            self.conexion = sqlite3.connect(self.db_path)
            cursor = self.conexion.cursor()

            cursor.execute("SELECT COUNT(*) FROM Evento")
            if cursor.fetchone()[0] == 0:
                directorio_actual = os.path.dirname(os.path.abspath(__file__))
                ruta_csv = os.path.join(directorio_actual, "eventos_prueba.csv")
                
                try:
                    with open(ruta_csv, mode='r', encoding='utf-8') as archivo_csv:
                        lector = csv.reader(archivo_csv)
                        next(lector)  # Saltar encabezado
                        
                        eventos = []
                        for fila in lector:
                            # Si el CSV original tiene 5 columnas, generamos una fecha válida según el mes
                            if len(fila) == 5:
                                id_ev, nombre, mes, totales, disp = fila[0], fila[1], int(fila[2]), int(fila[3]), int(fila[4])
                                fecha = f"2026-{mes:02d}-15"
                            else:
                                id_ev, nombre, mes, fecha, totales, disp = fila[0], fila[1], int(fila[2]), fila[3], int(fila[4]), int(fila[5])
                                
                            eventos.append((id_ev, nombre, mes, fecha, totales, disp))
                        
                        cursor.executemany(
                            "INSERT INTO Evento (idEvento, nombre, mes, fecha, puestosTotales, puestosDisponibles) VALUES (?, ?, ?, ?, ?, ?)",
                            eventos
                        )
                except FileNotFoundError:
                    print(f"[Persistencia] Advertencia: No se encontró el archivo '{ruta_csv}'.")
                    self.conexion.close()
                    return

                # Crear los 10 clientes de prueba
                clientes = [(f"C{i:03d}", f"Cliente Prueba {i}", "Estudiante") for i in range(1, 11)]
                cursor.executemany("INSERT INTO Cliente VALUES (?, ?, ?)", clientes)

                self.conexion.commit()
            self.conexion.close()

    def ejecutarConsulta(self, sql, parametros=()):
        """
        Ejecuta sentencias SELECT en la base de datos de forma segura (thread-safe).
        Retorna una lista de tuplas (ResultSet) con las filas obtenidas.
        """
        with self.lock:
            self.conexion = sqlite3.connect(self.db_path)
            cursor = self.conexion.cursor()
            try:
                cursor.execute(sql, parametros)
                resultados = cursor.fetchall()
            except sqlite3.Error as e:
                print(f"[Persistencia] Error en ejecutarConsulta: {e}")
                resultados = []
            finally:
                self.conexion.close()
            return resultados

    def ejecutarActualizacion(self, sql, parametros=()):
        """
        Ejecuta sentencias INSERT, UPDATE o DELETE garantizando atomicidad.
        Retorna True si la operación afectó filas exitosamente, False en caso de error.
        """
        with self.lock:
            self.conexion = sqlite3.connect(self.db_path)
            cursor = self.conexion.cursor()
            try:
                cursor.execute(sql, parametros)
                self.conexion.commit()
                # Verificar que la sentencia se ejecutó correctamente
                exito = True
            except sqlite3.Error as e:
                print(f"[Persistencia] Error en ejecutarActualizacion: {e}")
                self.conexion.rollback()
                exito = False
            finally:
                self.conexion.close()
            return exito

    def iniciar_servicio(self):
        """
        Inicializa el socket ZeroMQ REP (sock_rep) y escucha permanentemente
        las solicitudes síncronas enviadas por ServicioRMC y ServicioCancelaciones.
        
        Formato esperado del mensaje JSON:
        {
            "operacion": "ejecutarConsulta" | "ejecutarActualizacion",
            "sql": "SELECT ... / UPDATE ... / INSERT ...",
            "parametros": [...]
        }
        """
        self.sock_rep = self.context.socket(zmq.REP)
        self.sock_rep.bind(f"tcp://*:{self.puerto_rep}")
        print(f"[Persistencia] Servicio DAO activo y escuchando en tcp://*:{self.puerto_rep} (REQ/REP)...")

        try:
            while True:
                # 1. Esperar petición JSON desde ServicioRMC o ServicioCancelaciones
                peticion = self.sock_rep.recv_json()
                operacion = peticion.get("operacion")
                sql = peticion.get("sql", "")
                parametros = tuple(peticion.get("parametros", []))

                # 2. Enrutar al método DAO correspondiente según el diagrama de secuencia
                if operacion == "ejecutarConsulta":
                    resultado = self.ejecutarConsulta(sql, parametros)
                    self.sock_rep.send_json({"exito": True, "resultado": resultado})

                elif operacion == "ejecutarActualizacion":
                    exito = self.ejecutarActualizacion(sql, parametros)
                    self.sock_rep.send_json({"exito": exito, "resultado": exito})

                else:
                    self.sock_rep.send_json({
                        "exito": False,
                        "error": f"Operación '{operacion}' no reconocida en ServicioPersistencia."
                    })
        except KeyboardInterrupt:
            print("\n[Persistencia] Deteniendo el Servicio de Persistencia...")
        finally:
            if self.sock_rep:
                self.sock_rep.close()
            self.context.term()


if __name__ == "__main__":
    print("Iniciando Servicio de Persistencia...")
    db_service = ServicioPersistencia(puerto_rep=5560)

    # Verificación rápida local de los métodos del DAO antes de abrir el socket
    filas = db_service.ejecutarConsulta("SELECT COUNT(*) FROM Evento")
    print(f"[Persistencia] Total de eventos cargados en BD: {filas[0][0] if filas else 0}")

    # Iniciar bucle de comunicación ZeroMQ REP
    db_service.iniciar_servicio()