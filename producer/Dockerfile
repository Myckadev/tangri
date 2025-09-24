FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY openmeteo_producer.py /app/openmeteo_producer.py

# Conf is mounted at runtime
CMD ["python", "/app/openmeteo_producer.py"]
