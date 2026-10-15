# Se usa una imagen base ligera de Python
FROM python:3.9-slim

# Se establece el directorio de trabajo
WORKDIR /app

# Se copian los archivos del proyecto (servicio_persistencia.py, etc.)
COPY . /app

# Se instala pyzmq (SQLite3 ya viene en la librería estándar de Python)
RUN pip install --no-cache-dir pyzmq

# Se expone el puerto 5560 para las conexiones entrantes (como indica el diagrama de despliegue)
EXPOSE 5560

# Se ejecuta el script al iniciar el contenedor
CMD ["python", "servicio_persistencia.py"]