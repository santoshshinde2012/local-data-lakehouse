# Graph-on-gold image (ldl-graph): Python 3.12 + the hash-locked graph dependencies + the PyIceberg
# client. No pyspark, no JVM, no compiler: every dependency is a prebuilt manylinux wheel (amd64 and
# arm64), and --only-binary makes the build fail loudly if one ever is not.
# Teaching sample, not a production image. Code and data are bind-mounted by docker-compose.graph.yml.
#
# Build context = the repo root (it needs the two requirements files); what is sent to the builder
# is limited by docker/graph/Dockerfile.dockerignore.
#
# Base image: exact patch tag AND the multi-arch index digest (the digest is what is pulled; the
# tag is for humans). Refresh both together:
#   docker buildx imagetools inspect python:3.12-slim-trixie
ARG PYTHON_IMAGE=python:3.12.15-slim-trixie@sha256:29113dcae7aad06daa8e95260fa09f27d62be33b9687ea3774f771d601a02256
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# requirements-graph-spark-client.txt is compiled with `-c requirements-graph.txt`, so the two locks
# agree on every shared pin (pip refuses the install otherwise).
COPY requirements-graph.txt requirements-graph-spark-client.txt /tmp/requirements/
RUN python -m pip install --require-hashes --only-binary=:all: \
      -r /tmp/requirements/requirements-graph.txt \
      -r /tmp/requirements/requirements-graph-spark-client.txt \
 && python -m pip check \
 && rm -rf /tmp/requirements

# Non-root by default. /opt/data/graph is the mount point of ./data/graph (GRAPH_ROOT).
RUN groupadd --gid 10001 graph \
 && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /tmp --shell /usr/sbin/nologin graph \
 && mkdir -p /opt/lakehouse /opt/data/graph \
 && chown 10001:10001 /opt/data/graph

ENV PYTHONPATH=/opt/lakehouse/src \
    GRAPH_ROOT=/opt/data/graph \
    HOME=/tmp
WORKDIR /opt/lakehouse
USER 10001:10001

# Keep the container alive for `docker compose exec graph ...` (the same pattern as ldl-spark).
CMD ["sleep", "infinity"]
