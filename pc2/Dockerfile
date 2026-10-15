
FROM python:3.9-slim

WORKDIR /app

COPY . /app


RUN pip install --no-cache-dir pyzmq

EXPOSE 5555 5556 5557
CMD ["tail", "-f", "/dev/null"]