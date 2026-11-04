FROM python:3.12.10-slim-bookworm AS dependencies
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml /app/
RUN python -c "import tomllib; from pathlib import Path; Path('/tmp/requirements.txt').write_text('\n'.join(tomllib.loads(Path('pyproject.toml').read_text())['project']['dependencies']))" && pip install -r /tmp/requirements.txt

FROM dependencies AS spark-dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
RUN pip install pyspark==4.0.1 delta-spark==4.0.0
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN mkdir -p /opt/spark-jars && python -c "import urllib.request; base='https://repo.maven.apache.org/maven2/'; jars=['io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar','io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar','org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar']; [urllib.request.urlretrieve(base+j, '/opt/spark-jars/'+j.split('/')[-1]) for j in jars]"

FROM dependencies AS app
COPY src /app/src
COPY sql /app/sql
RUN pip install --no-deps .
CMD ["uvicorn", "urbanlake.api:app", "--host", "0.0.0.0", "--port", "8000"]

FROM spark-dependencies AS spark
COPY src /app/src
COPY sql /app/sql
RUN pip install --no-deps .
CMD ["urbanlake", "process"]

FROM python:3.12.10-slim-bookworm AS airflow-dependencies
# Keep the constrained orchestration environment independently cacheable.
RUN python -m venv /opt/airflow-venv && /opt/airflow-venv/bin/pip install apache-airflow==2.11.0 psycopg2-binary==2.9.10 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.0/constraints-3.12.txt

FROM spark AS airflow
COPY --from=airflow-dependencies /opt/airflow-venv /opt/airflow-venv
ENV PATH="/opt/airflow-venv/bin:${PATH}" AIRFLOW_HOME=/app/artifacts/airflow
COPY dags /app/dags
COPY scripts /app/scripts
CMD ["airflow", "standalone"]
