FROM python:3.9-slim

WORKDIR /app
COPY . /app

# Se asume que tienes pyzmq definido en tu requirements.txt como indica la arquitectura
RUN pip install --no-cache-dir -r requirements.txt

# Ejecuta el script del cliente directamente al iniciar el contenedor
CMD ["python", "cliente_simulador.py"]