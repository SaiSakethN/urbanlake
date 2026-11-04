# Official Apache binary fallback when Docker Hub layer downloads are unavailable.
FROM python:3.12.10-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY infra/download_kafka.py /opt/download_kafka.py
RUN python /opt/download_kafka.py && mv /opt/kafka_2.13-4.0.1 /opt/kafka
RUN useradd --system --uid 1001 kafka && mkdir -p /var/lib/kafka/data /var/lib/kafka/logs && chown -R kafka:kafka /var/lib/kafka
COPY infra/kafka/server.properties /opt/kafka/config/urbanlake.properties
USER kafka
ENV KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
ENV LOG_DIR=/var/lib/kafka/logs
CMD ["sh", "-c", "if [ ! -f /var/lib/kafka/data/meta.properties ]; then /opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /opt/kafka/config/urbanlake.properties; fi; exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/urbanlake.properties"]
