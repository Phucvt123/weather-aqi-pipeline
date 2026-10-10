###############################################################################
# Dockerfile.airflow — Airflow với Spark client (cho SparkSubmitOperator)
#
# So với v1:
#   - Vẫn cần Java (cho spark-submit client)
#   - BỎ PySpark như Python library (Spark xử lý trên cluster riêng)
#   - Thêm Spark binaries (spark-submit command)
#   - Thêm boto3 (ghi landing zone lên MinIO)
#   - Thêm apache-airflow-providers-apache-spark
###############################################################################

FROM apache/airflow:2.9.3

USER root

# Install Java runtime (cần cho spark-submit client)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download Spark 3.5.3 binaries (chỉ dùng spark-submit, không chạy worker)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-3.5.3-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.0 \
    requests==2.32.3 \
    boto3==1.35.36 \
    pytz==2024.2
