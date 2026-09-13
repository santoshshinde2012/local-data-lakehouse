# Spark 3.5 + Iceberg runtime + Hadoop AWS (MinIO/S3A) + Postgres JDBC
FROM apache/spark:3.5.3-java17

USER root

ARG ICEBERG_VERSION=1.6.1
ARG SPARK_SERIES=3.5
ARG SCALA=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE=1.12.367
ARG POSTGRES_JDBC=42.7.3

RUN set -eux; \
  curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_SERIES}_${SCALA}-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_SERIES}_${SCALA}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_SERIES}_${SCALA}-${ICEBERG_VERSION}.jar"; \
  curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
  curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar"; \
  curl -fsSL -o /opt/spark/jars/postgresql-${POSTGRES_JDBC}.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC}/postgresql-${POSTGRES_JDBC}.jar"

# Keep container alive for docker compose exec spark-submit / pyspark
WORKDIR /opt/jobs
CMD ["sleep", "infinity"]
