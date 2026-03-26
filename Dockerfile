FROM python:3.12-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

FROM base AS api
EXPOSE 8080
CMD ["uvicorn", "src.api.routes:app", "--host", "0.0.0.0", "--port", "8080"]

FROM base AS ui
EXPOSE 8501
CMD ["streamlit", "run", "src/ui/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]

FROM base AS producer
CMD ["python", "-m", "src.ingestion.kafka_producer"]

FROM base AS consumer
CMD ["python", "-m", "src.ingestion.spark_consumer"]
