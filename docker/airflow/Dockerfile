# Local-demo Airflow image with Docker CLI (submit jobs into ldl-spark).
# Sample-only: not a production Airflow image.
ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

USER root
# Static Docker CLI so BashOperator / scripts can `docker exec ldl-spark …`
ARG TARGETARCH
RUN set -eux; \
  arch="${TARGETARCH:-amd64}"; \
  case "$arch" in \
    amd64) darch=x86_64 ;; \
    arm64) darch=aarch64 ;; \
    *) darch=x86_64 ;; \
  esac; \
  curl -fsSL "https://download.docker.com/linux/static/stable/${darch}/docker-27.3.1.tgz" \
    | tar -xz -C /tmp; \
  mv /tmp/docker/docker /usr/local/bin/docker; \
  rm -rf /tmp/docker; \
  docker --version

# Local demo convenience: run as root so /var/run/docker.sock is usable on Docker Desktop.
USER root
