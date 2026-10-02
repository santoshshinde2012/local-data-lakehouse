# Local-demo Airflow 3.3 image: + a static Docker CLI so BashOperator tasks can `docker exec ldl-spark …`
# through the overlay's docker-proxy (DOCKER_HOST=tcp://docker-proxy:2375). Runs as the image's
# non-root airflow user. Not a production image.
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

USER root
ARG TARGETARCH
ARG DOCKER_CLI_VERSION=29.8.2
ARG DOCKER_CLI_SHA256_AMD64=995d1ef289677f74fd58d8d2c35727b6a4ee389c69db8638a3e42d0487aa5b0f
ARG DOCKER_CLI_SHA256_ARM64=76a624e4a8e5da654d1150e808175125efb5a6f1b6aa1cbd9caee18f51047a50
RUN set -eux; \
  case "${TARGETARCH:-amd64}" in \
    amd64) darch=x86_64; sum="${DOCKER_CLI_SHA256_AMD64}" ;; \
    arm64) darch=aarch64; sum="${DOCKER_CLI_SHA256_ARM64}" ;; \
    *) echo "unsupported arch ${TARGETARCH}"; exit 1 ;; \
  esac; \
  curl -fsSL -o /tmp/docker.tgz "https://download.docker.com/linux/static/stable/${darch}/docker-${DOCKER_CLI_VERSION}.tgz"; \
  echo "${sum}  /tmp/docker.tgz" | sha256sum -c -; \
  tar -xzf /tmp/docker.tgz -C /tmp docker/docker; \
  install -m 0755 /tmp/docker/docker /usr/local/bin/docker; \
  rm -rf /tmp/docker /tmp/docker.tgz; \
  docker --version
USER airflow
