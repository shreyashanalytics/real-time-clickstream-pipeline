FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=UTC PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the Kafka connector jars into the image so start-up is fast and offline-safe.
RUN python -c "from pyspark.sql import SparkSession; \
s = SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
.config('spark.jars.ivy', '/opt/ivy').getOrCreate(); s.stop()" \
 || echo "Jar pre-fetch skipped; they will be downloaded on first run."

COPY stream_processor.py .
CMD ["python", "stream_processor.py"]
