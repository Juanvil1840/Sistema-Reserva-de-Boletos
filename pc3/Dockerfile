FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5555 5556 5557 5558 5559 5560

CMD ["python", "-u", "iniciar_pc3.py"]
