# Sistema de Reserva de Boletos - Sistemas Distribuidos

Este proyecto implementa un Sistema de Reserva de Boletos distribuido en tres capas, utilizando Python y ZeroMQ para la comunicación entre los componentes. La persistencia de datos se maneja mediante SQLite3.

## Arquitectura del Sistema

El sistema sigue una arquitectura de tres capas:
1. **Capa de Consumidores:** `cliente_simulador.py`
2. **Capa de Servicios de Negocio:** `gestor_transacciones.py`, `servicio_rmc.py`, `servicio_cancelaciones.py`
3. **Capa de Servicios de Almacenamiento:** `servicio_persistencia.py`

## Requisitos Previos

- Docker y Docker Compose (opcional) instalados.
- Python 3.9+ (si se ejecuta sin Docker).
- Librerías necesarias (ver `requirements.txt`):
  - `pyzmq`

## Instrucciones de Ejecución (Usando Docker)

Este proyecto está diseñado para ejecutarse en contenedores Docker para garantizar la consistencia del entorno.

### 1. Configuración de Red

Es crucial configurar correctamente las direcciones IP en los archivos JSON de configuración para que los componentes puedan comunicarse a través de la red (o VPN).

- **`config_negocio.json`:** Debe contener la IP de la máquina donde se ejecuta la persistencia.
- **`config_clientes.json`:** Debe contener la IP de la máquina donde se ejecuta el Gestor de Transacciones.

### 2. Ejecutar el Servicio de Persistencia (PC3 - Máquina de Base de Datos)

1. Navegar a la carpeta del proyecto en la máquina de base de datos.
2. Construir la imagen:
   ```bash
   docker build -t servicio-persistencia:1.0 .
   ```
3. Ejecutar el contenedor:
   ```bash
   docker run -d -p 5560:5560 --name contenedor-persistencia servicio-persistencia:1.0
   ```

### 3. Ejecutar los Servicios de Negocio (PC2 - Servidor Principal)

1. Navegar a la carpeta del proyecto en el servidor principal.
2. Construir la imagen:
   ```bash
   docker build -t servicios-negocio:1.0 .
   ```
3. Iniciar el contenedor en segundo plano:
   ```bash
   docker run -d --name contenedor-negocio -p 5555:5555 -p 5556:5556 -p 5557:5557 servicios-negocio:1.0
   ```
4. Acceder al contenedor en múltiples terminales para ejecutar los servicios individuales:
   ```bash
   docker exec -it contenedor-negocio /bin/bash
   ```
5. Dentro del contenedor, ejecutar en terminales separadas:
   - `python gestor_transacciones.py`
   - `python servicio_rmc.py`
   - `python servicio_cancelaciones.py`

### 4. Ejecutar el Cliente Simulador (PC1 o PC2 - Generador de Carga)

Si se experimentan problemas de firewall de red, se recomienda ejecutar el cliente en la misma máquina que los servicios de negocio (PC2).

1. Construir la imagen del cliente (si está en una máquina separada):
   ```bash
   docker build -t cliente-simulador:1.0 .
   ```
2. Ejecutar el cliente:
   ```bash
   docker run --name contenedor-cliente cliente-simulador:1.0
   ```
   *(Si se ejecuta en PC2 usando el mismo contenedor, simplemente abra una nueva terminal, use `docker exec -it contenedor-negocio /bin/bash` y ejecute `python cliente_simulador.py`)*.

## Puertos Utilizados

- **5555:** Comunicación Síncrona (REQ/REP) - Consultas y Reservas.
- **5556:** Comunicación Asíncrona (DEALER/ROUTER) - Cancelaciones.
- **5557:** Notificaciones (PUB/SUB).
- **5560:** Comunicación con la Persistencia de Datos.

## Solución de Problemas de Red

Si los clientes se quedan bloqueados intentando conectar:
1. Verifique que los puertos 5555, 5556 y 5557 estén abiertos en el firewall del servidor (PC2). En Ubuntu: `sudo ufw allow 5555/tcp`
2. Si usa VPN, asegúrese de que el tráfico no esté bloqueado institucionalmente. Si es así, ejecute los clientes en la misma máquina (localhost).
3. Verifique las IPs en los archivos `.json` de configuración.
