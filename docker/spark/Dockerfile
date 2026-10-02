# Spark 4.1.3 (Scala 2.13, Java 21, Python 3) + Iceberg 1.11.0 runtime + Iceberg AWS bundle.
# The REST catalog (Lakekeeper) vends S3 credentials per table, and Iceberg's own S3FileIO (AWS SDK
# v2 from iceberg-aws-bundle) reads/writes the files: no hadoop-aws, no aws-java-sdk-bundle, no
# JDBC driver. Multi-arch base (amd64 + arm64), pinned by tag AND index digest; refresh together:
#   docker buildx imagetools inspect apache/spark:4.1.3-scala2.13-java21-python3-ubuntu
# Spark 4.2 has no Iceberg runtime yet; Spark 3.5.9 (LTS) is the fallback line (see MIGRATION.md).
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java21-python3-ubuntu@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211
FROM ${SPARK_IMAGE}

ARG ICEBERG_VERSION=1.11.0
ARG SPARK_SERIES=4.1
ARG SCALA=2.13
# sha256 of the two Maven Central jars (verified at build time).
ARG ICEBERG_RUNTIME_SHA256=d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242
ARG ICEBERG_AWS_SHA256=38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f

USER root
RUN set -eux; \
  base=https://repo1.maven.org/maven2/org/apache/iceberg; \
  rt="iceberg-spark-runtime-${SPARK_SERIES}_${SCALA}"; \
  curl -fsSL -o "/opt/spark/jars/${rt}-${ICEBERG_VERSION}.jar" "${base}/${rt}/${ICEBERG_VERSION}/${rt}-${ICEBERG_VERSION}.jar"; \
  curl -fsSL -o "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    "${base}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
  if [ -n "${ICEBERG_RUNTIME_SHA256}" ]; then echo "${ICEBERG_RUNTIME_SHA256}  /opt/spark/jars/${rt}-${ICEBERG_VERSION}.jar" | sha256sum -c -; fi; \
  if [ -n "${ICEBERG_AWS_SHA256}" ]; then echo "${ICEBERG_AWS_SHA256}  /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha256sum -c -; fi; \
  chmod 0644 /opt/spark/jars/iceberg-*.jar; \
  mkdir -p /opt/data/export /opt/data/graph && chown -R spark:spark /opt/data

# Non-root (the image's spark user, uid 185). Bind-mounted ./data/export must be writable by it:
# Docker Desktop maps ownership; on Linux run `chmod a+rwx data/export` once (README).
USER spark
WORKDIR /opt/jobs
# Kept alive for `docker compose exec spark spark-submit ...` (pipelines/run_job.sh, Airflow DAGs).
CMD ["sleep", "infinity"]
