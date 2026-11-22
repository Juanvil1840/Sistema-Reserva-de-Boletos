# Sistema Distribuido de Reserva de Boletos (SRB)

Sistema distribuido tolerante a fallos diseñado bajo una **arquitectura de 3 capas** (`PC1` Consumidores, `PC2` Lógica de Negocio, `PC3` Almacenamiento y Respaldo), comunicadas mediante **ZeroMQ (PyZMQ)** sobre una red privada virtual (**Tailscale**) y desplegadas en contenedores **Docker**.

Incluye balanceo de carga horizontal (`ROUTER/DEALER`) entre $N$ réplicas del servicio de negocio, procesamiento asíncrono de cancelaciones (`PUSH/PULL` + notificaciones `PUB/SUB`), transacciones atómicas concurrentes sobre **SQLite en modo WAL**, bitácora de *checkpointing* con idempotencia por `idTransaccion` y conmutación automática (**failover Active-Passive**) mediante monitoreo de *heartbeat*.

---

## 1. Arquitectura de 3 Capas y Mapa Oficial de Puertos

| Puerto TCP | Patrón ZeroMQ | Nodo que hace `bind` | Nodo que hace `connect` | Propósito en el Sistema |
| :---: | :---: | :--- | :--- | :--- |
| **5555** | `REQ` $\leftrightarrow$ `ROUTER` | `PC2` (`PC3` en Failover) | `PC1` (`HiloCliente`) | Canal síncrono principal: **Consultas, Reservas y Modificaciones** |
| **5556** | `DEALER` $\leftrightarrow$ `ROUTER` | `PC2` (`PC3` en Failover) | `PC1` (`HiloCliente`) | Canal asíncrono de entrada para **Cancelaciones** (retorna acuse `EN_PROCESO`) |
| **5557** | `PUB` $\rightarrow$ `SUB` | `PC2` (`PC3` en Failover) | `PC1` (`ReceptorNotificaciones`) | Publicación asíncrona de confirmaciones de cancelación (`[idCliente, payload_json]`) |
| **5558** | `REQ` $\leftrightarrow$ `REP` | `PC3` (`servicio_persistencia.py`) | `PC2` y `PC3` (Workers y Gestor) | **Servicio DAO SQLite (WAL)**: consultas, transacciones atómicas y *checkpoints* |
| **5559** | `PUSH` $\rightarrow$ `PULL` | `servicio_cancelaciones.py` | `gestor_transacciones.py` | Cola interna de tareas de cancelación entre el Gestor y el Worker de Cancelaciones |
| **5560** | `DEALER` $\leftrightarrow$ `REP` | `gestor_transacciones.py` | Réplicas `servicio_rmc.py` (`1..N`) | Backend del balanceador de carga *Round-Robin* hacia las réplicas RMC |
| **5569** | `REQ` $\leftrightarrow$ `REP` | `PC2` (`gestor_transacciones.py`) | `PC3` (`gestor_transacciones_respaldo.py`) | Canal de **Heartbeat** (`ping`/`pong`) para detección de caída de `PC2` |

---

## 2. Estructura del Repositorio

```text
Sistema-Reserva-de-Boletos/
├── docker-compose.yml                     # Orquestación local de los 3 contenedores en red bridge
├── README.md                              # Documentación general y guía de despliegue
├── pc1/                                   # CAPA 1: Consumidores (Simulador y Métricas)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── cliente_simulador.py               # Simulador multihilo con failover e idempotencia
│   ├── test_pc1_local.py                  # Prueba E2E automatizada con caída de PC2 en vivo
│   ├── config/
│   │   └── pc1_config.json                # Parámetros de servidores, clientes, RPM y porcentajes
│   ├── servicios/
│   │   ├── notificaciones.py              # Hilo receptor SUB (5557) con reconexión en failover
│   │   └── metricas.py                    # Recolector thread-safe y exportador de CSV
│   └── resultados/                        # Carpeta de salida de CSV locales
├── pc2/                                   # CAPA 2: Lógica de Negocio Principal
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── iniciar_pc2.py                     # Orquestador: Gestor + N réplicas RMC + Cancelaciones
│   ├── gestor_transacciones.py            # Coordinador (ROUTER 5555/5556, DEALER 5560, REP 5569)
│   ├── servicio_rmc.py                    # Worker síncrono escalable [1..N] (Consulta/Reserva/Modif.)
│   ├── servicio_cancelaciones.py          # Worker asíncrono (PULL 5559 -> PC3 5558 -> PUB 5557)
│   ├── test_workers_local.py              # Prueba aislada de réplicas RMC y Cancelaciones
│   ├── test_gestor_local.py               # Prueba de integración del Gestor, balanceo e idempotencia
│   ├── config/
│   │   └── pc2_config.json
│   └── servicios/
│       ├── cliente_persistencia.py        # Cliente REQ resiliente hacia PC3 (5558)
│       └── configuracion.py
├── pc3/                                   # CAPA 3: Almacenamiento SQLite y Nodo de Respaldo
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── iniciar_pc3.py                     # Orquestador: Persistencia (5558) + Gestor de Respaldo
│   ├── servicio_persistencia.py           # Servidor REP (5558) sobre SQLite WAL
│   ├── gestor_transacciones_respaldo.py   # Monitor Heartbeat (PASIVO -> ACTIVANDO -> ACTIVO)
│   ├── servicio_rmc_respaldo.py           # Réplicas RMC de respaldo activadas en failover
│   ├── servicio_cancelaciones_respaldo.py # Worker de cancelaciones de respaldo
│   ├── test_pc3_local.py                  # Prueba unitaria/integración de SQLite y Checkpoints
│   ├── test_respaldo_local.py             # Prueba de caída de PC2 y recuperación en PC3
│   ├── config/
│   │   └── pc3_config.json
│   ├── datos/
│   │   └── eventos_prueba.csv             # 50 eventos iniciales (E001..E050, mes 10)
│   └── servicios/
│       ├── database.py                    # Clase BaseDatosSRB (SQLite WAL + Lock + Checkpoints)
│       ├── cliente_persistencia.py
│       ├── configuracion.py
│       └── procesos.py
└── experimentos/                          # SUITE EXPERIMENTAL Y GENERACIÓN DE GRÁFICAS
    ├── requirements_graficas.txt          # matplotlib>=3.8.0
    ├── ejecutar_experimentos.py           # Automatización de corridas (1 RMC vs. 3 RMC + Failover)
    ├── generar_graficas.py                # Generación de PNGs (300 DPI) y tabla consolidada CSV
    ├── datos/                             # Archivos detalle_*.csv y resumen_*.csv experimentales
    └── graficas/                          # Gráficas listas para el Documento 2 (Informe)
```

---

## 3. Despliegue con Docker en 3 Máquinas Físicas/Virtuales (Tailscale)

Suponga las siguientes direcciones IP asignadas por **Tailscale** a cada computador:
- **PC3 (Almacenamiento y Respaldo)**: `100.100.1.30`
- **PC2 (Lógica de Negocio Principal)**: `100.100.1.20`
- **PC1 (Clientes Simuladores)**: `100.100.1.10`

> **Orden estricto de arranque**: **1.º PC3** $\rightarrow$ **2.º PC2** $\rightarrow$ **3.º PC1**.

### Paso 1 — En la máquina PC3 (`100.100.1.30`)
Construir y levantar el contenedor de persistencia y respaldo pasivo:

```bash
cd pc3
docker build -t srb-pc3 .
docker run --rm -it --name srb-pc3 \
  -e HOST_GT_PRINCIPAL=100.100.1.20 \
  -e NUM_REPLICAS_RMC=2 \
  -p 5555:5555 -p 5556:5556 -p 5557:5557 -p 5558:5558 -p 5559:5559 -p 5560:5560 \
  srb-pc3
```

### Paso 2 — En la máquina PC2 (`100.100.1.20`)
Construir y levantar el Gestor de Transacciones Principal junto con las réplicas RMC (use `NUM_REPLICAS_RMC=1` para la Arquitectura Base o `NUM_REPLICAS_RMC=3` para la Arquitectura Escalable):

```bash
cd pc2
docker build -t srb-pc2 .
docker run --rm -it --name srb-pc2 \
  -e HOST_PERSISTENCIA=100.100.1.30 \
  -e NUM_REPLICAS_RMC=3 \
  -p 5555:5555 -p 5556:5556 -p 5557:5557 -p 5559:5559 -p 5560:5560 -p 5569:5569 \
  srb-pc2
```

### Paso 3 — En la máquina PC1 (`100.100.1.10`)
Construir y ejecutar el simulador de clientes montando la carpeta `resultados/` para conservar los archivos CSV en el host:

```bash
cd pc1
docker build -t srb-pc1 .
docker run --rm -it --name srb-pc1 \
  -e HOST_PRINCIPAL=100.100.1.20 \
  -e HOST_RESPALDO=100.100.1.30 \
  -e NUM_CLIENTES=5 \
  -e DURACION_SEG=120 \
  -e RPM=36 \
  -e ESCENARIO=escalable_3rmc_5cli \
  -v "${PWD}/resultados:/app/resultados" \
  srb-pc1
```

### Opción alternativa: Prueba de los 3 contenedores en una sola máquina con Docker Compose
Para validar los 3 contenedores integrados antes de pasar a los 3 equipos físicos:

```bash
docker compose up --build
```

---

## 4. Ejecución Local (Sin Docker) y Suite de Pruebas Automatizadas

Requisitos: Python 3.10+ e instalación de `pyzmq>=25.0`:

```bash
pip install -r pc3/requirements.txt
```

### Suite de 5 pruebas automatizadas de verificación
Cada prueba levanta sus propios subprocesos con una base de datos temporal aislada (no ensucia archivos del repositorio) y verifica todas las invariantes del sistema:

```bash
# 1. Capa de Almacenamiento (SQLite WAL, 50 eventos CSV, transacciones atómicas y checkpoints)
python pc3/test_pc3_local.py

# 2. Workers de Negocio (2 réplicas RMC en paralelo + worker de cancelaciones y PUB/SUB)
python pc2/test_workers_local.py

# 3. Gestor de Transacciones (Balanceo ROUTER/DEALER, concurrencia, idempotencia y Heartbeat)
python pc2/test_gestor_local.py

# 4. Tolerancia a Fallos (Caída abrupta de PC2, detección por Heartbeat y failover a PC3)
python pc3/test_respaldo_local.py

# 5. Prueba E2E Completa (Simulador PC1 multihilo + caída en vivo de PC2 + auditoría SQLite + CSV)
python pc1/test_pc1_local.py
```

---

## 5. Ejecución de Experimentos y Generación de Gráficas (Documento 2)

El directorio `experimentos/` automatiza las corridas comparativas exigidas en el informe (**Arquitectura Base con 1 RMC** vs. **Arquitectura Escalable con 3 RMC** para **3, 5 y 8 clientes**, más el **Escenario de Resiliencia con caída de PC2**).

### 1. Instalar dependencia de graficación
```bash
pip install -r experimentos/requirements_graficas.txt
```

### 2. Ejecutar la batería de experimentos
- **Corrida rápida de verificación** (10 segundos por escenario):
  ```bash
  python experimentos/ejecutar_experimentos.py --duracion 10
  ```
- **Corrida oficial para el informe** (120 segundos por escenario):
  ```bash
  python experimentos/ejecutar_experimentos.py --duracion 120
  ```

### 3. Generar las gráficas PNG (300 DPI) y la tabla consolidada
```bash
python experimentos/generar_graficas.py
```
Los resultados quedan disponibles en `experimentos/graficas/`:
- `01_latencia_promedio_comparativa.png`: Latencia promedio y desviación estándar por operación (1 RMC vs. 3 RMC).
- `02_tpm_comparativa.png`: Comparativa de Transacciones Procesadas por Minuto (TPM) para 3, 5 y 8 clientes.
- `03_resiliencia_failover_temporal.png`: Serie de tiempo de latencia durante la caída abrupta de `PC2` y conmutación a `PC3`.
- `04_tabla_consolidada.csv`: Tabla resumen con todas las métricas lista para insertar en el Documento 2.
