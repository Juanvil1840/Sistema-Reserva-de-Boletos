
FROM python:3.9-slim


WORKDIR /app


COPY . /app

RUN pip install --no-cache-dir pyzmq

EXPOSE 5560

CMD ["python", "servicio_persistencia.py"]