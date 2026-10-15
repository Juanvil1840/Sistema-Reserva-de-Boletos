# Usamos una imagen ligera de Python como base
FROM python:3.9-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos todo el código fuente del proyecto al contenedor
COPY . /app

# Instalamos las dependencias necesarias. 
# Si tienes un requirements.txt, usa: RUN pip install --no-cache-dir -r requirements.txt
# Si solo usas ZeroMQ, instalamos pyzmq directamente
RUN pip install --no-cache-dir pyzmq

# Exponemos los puertos que usan los servicios de negocio
EXPOSE 5555 5556 5557

# Mantenemos el contenedor vivo en segundo plano sin ejecutar nada específico aún,
# para poder entrar en él y ejecutar cada servicio en pestañas separadas.
CMD ["tail", "-f", "/dev/null"]