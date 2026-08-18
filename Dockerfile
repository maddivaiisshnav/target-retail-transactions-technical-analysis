# Runs the Spark/Kafka-client side of the pipeline. Kafka itself runs as a
# separate service (see docker-compose.yml) - this image needs Python 3.11+
# (matching what the pinned pyspark/pandas wheels are built for) plus a JVM,
# since PySpark is a thin Python wrapper around a Scala/JVM engine.
FROM python:3.11-slim

# Debian's current stable ships OpenJDK 21, not 17 - Spark 4.x supports both,
# so we take whatever the base image's repos actually have rather than
# pinning a version that may not be packaged.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV PYTHONUNBUFFERED=1

CMD ["bash", "scripts/docker_pipeline.sh"]
