"""T0: static contracts of the Compose stack, configs and pins (no containers, no network).

`docker compose config` needs only the docker CLI with the compose plugin (no daemon); those tests
skip without it. Everything else is plain text.
"""
from __future__ import annotations

import re
import subprocess

import pytest
from support.ldl import REPO, compose_config, env_example, pinned_images

LIGHT = {"postgres", "lakekeeper-migrate", "lakekeeper", "objectstore", "lakehouse-init"}
ONE_SHOT = {"lakekeeper-migrate"}
JVM_IMAGES = ("spark", "trino")


@pytest.fixture(scope="module")
def light():
    return compose_config("light")["services"]


@pytest.fixture(scope="module")
def full():
    return compose_config("full")["services"]


@pytest.fixture(scope="module")
def full_trino():
    return compose_config("full", "trino")["services"]


def test_light_profile_is_catalog_store_and_init_without_a_jvm(light):
    assert set(light) == LIGHT
    for name, svc in light.items():
        assert not any(j in str(svc.get("image", "")) for j in JVM_IMAGES), f"{name}: JVM image in light"


def test_full_adds_spark_and_trino_is_opt_in(full, full_trino):
    assert set(full) == LIGHT | {"spark"}
    assert set(full_trino) == LIGHT | {"spark", "trino"}
    assert compose_config()["services"] == {}, "nothing may start without a profile"


def test_every_image_is_pinned_by_tag_and_digest(full_trino):
    for name, svc in full_trino.items():
        if "build" in svc:
            continue
        ref = svc["image"]
        assert re.fullmatch(r"[\w./-]+:[\w.-]+@sha256:[0-9a-f]{64}", ref), f"{name}: {ref} is not tag@digest"
        assert ":latest" not in ref


def test_long_running_services_have_healthchecks_and_one_shots_do_not_restart(full_trino):
    for name, svc in full_trino.items():
        if name in ONE_SHOT:
            assert svc.get("restart") == "no", name
        else:
            assert svc.get("healthcheck", {}).get("test"), f"{name} needs a healthcheck for `up --wait`"


def test_dependency_chain_waits_for_health_and_init(full_trino):
    dep = {n: {k: v["condition"] for k, v in s.get("depends_on", {}).items()} for n, s in full_trino.items()}
    assert dep["lakekeeper-migrate"] == {"postgres": "service_healthy"}
    assert dep["lakekeeper"] == {"lakekeeper-migrate": "service_completed_successfully"}
    assert dep["lakehouse-init"] == {"lakekeeper": "service_healthy", "objectstore": "service_healthy"}
    assert dep["spark"] == dep["trino"] == {"lakehouse-init": "service_healthy"}


def test_one_endpoint_inside_and_outside(light):
    env = env_example()
    store = light["objectstore"]
    assert "objectstore.localhost" in store["networks"]["lakehouse"]["aliases"]
    port = env["S3_API_PORT"]
    assert env["S3_ENDPOINT"] == f"http://objectstore.localhost:{port}"
    published = {(p["target"], p["published"]) for p in store["ports"]}
    assert (int(port), port) in published, "the S3 port must be the same inside and on the host"
    assert store["environment"]["RUSTFS_ADDRESS"] == f":{port}"
    assert light["lakehouse-init"]["environment"]["S3_ENDPOINT"] == env["S3_ENDPOINT"]


def test_only_the_catalog_and_store_publish_ports_in_light(light):
    assert not light["postgres"].get("ports"), "Postgres is Lakekeeper's private database"
    published = sorted(int(p["published"]) for s in light.values() for p in s.get("ports", []))
    assert published == [8181, 9000, 9001]


def test_no_docker_socket_and_no_root_key_in_engines(full_trino):
    for name, svc in full_trino.items():
        for v in svc.get("volumes", []):
            assert "docker.sock" not in str(v.get("source", "")), name
    for name in ("spark", "trino"):
        env = full_trino[name].get("environment") or {}
        assert not any("SECRET" in k or "ACCESS_KEY" in k for k in env), f"{name} must rely on vended credentials"


def test_silo_override_swaps_only_the_store():
    base = compose_config("light")["services"]
    silo = compose_config("light", files=("docker-compose.yml", "docker-compose.silo.yml"))["services"]
    assert set(base) == set(silo)
    assert "pgsty/silo:" in silo["objectstore"]["image"]
    assert [v["source"] for v in silo["objectstore"]["volumes"]] != [v["source"] for v in base["objectstore"]["volumes"]]
    for name in base:
        if name != "objectstore":
            assert base[name] == silo[name], name
    assert silo["objectstore"]["networks"] == base["objectstore"]["networks"]


def test_env_example_defines_every_required_variable():
    text = (REPO / "docker-compose.yml").read_text() + (REPO / "docker-compose.silo.yml").read_text()
    required = set(re.findall(r"\$\{(\w+):\?", text))
    assert required <= set(env_example()), sorted(required - set(env_example()))


def test_spark_defaults_use_the_rest_catalog_and_hold_no_secrets():
    conf = dict(line.split(None, 1) for line in (REPO / "config/spark-defaults.conf").read_text().splitlines()
                if line.strip() and not line.startswith("#"))
    conf = {k: v.strip() for k, v in conf.items()}
    assert conf["spark.sql.catalog.lakehouse.type"] == "rest"
    assert conf["spark.sql.catalog.lakehouse.uri"] == "http://lakekeeper:8181/catalog"
    assert conf["spark.sql.catalog.lakehouse.warehouse"] == env_example()["LAKEKEEPER_WAREHOUSE"]
    assert conf["spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation"] == "vended-credentials"
    assert not any(re.search(r"(access|secret)\.?key|password|jdbc|s3a", k, re.I) for k in conf), conf
    secrets = {v for k, v in env_example().items() if "SECRET" in k or "PASSWORD" in k or "KEY" in k}
    text = (REPO / "config/spark-defaults.conf").read_text()
    assert not any(s and s in text for s in secrets)


def test_trino_catalog_uses_vended_credentials():
    props = dict(line.split("=", 1) for line in (REPO / "config/trino/catalog/lakehouse.properties").read_text()
                 .splitlines() if line and not line.startswith("#"))
    assert props["iceberg.catalog.type"] == "rest"
    assert props["iceberg.rest-catalog.vended-credentials-enabled"] == "true"
    assert props["iceberg.rest-catalog.warehouse"] == env_example()["LAKEKEEPER_WAREHOUSE"]
    assert props["s3.endpoint"] == env_example()["S3_ENDPOINT"]
    assert not any("key" in k.lower() and "access" in k.lower() for k in props)
    assert "-Xmx1G" in (REPO / "config/trino/jvm.config").read_text().split()


def test_spark_image_is_spark_41_iceberg_111_without_hadoop_aws():
    text = (REPO / "docker/spark/Dockerfile").read_text()
    assert re.search(r"apache/spark:4\.1\.3-scala2\.13-java21-python3-ubuntu@sha256:[0-9a-f]{64}", text)
    assert "ARG ICEBERG_VERSION=1.12.0" in text and "iceberg-aws-bundle" in text
    assert re.search(r"ICEBERG_RUNTIME_SHA256=[0-9a-f]{64}", text) and re.search(r"ICEBERG_AWS_SHA256=[0-9a-f]{64}", text)
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    assert "hadoop-aws" not in code and "aws-java-sdk-bundle" not in code and "postgresql" not in code
    assert re.search(r"^USER spark$", text, re.M), "the Spark image must end non-root"
    compose = (REPO / "docker-compose.yml").read_text()
    assert "iceberg-spark-runtime-4.1_2.13-1.12.0.jar" in compose


def test_init_script_is_posix_and_enables_sts():
    script = REPO / "docker/init/bootstrap.sh"
    assert subprocess.run(["sh", "-n", str(script)]).returncode == 0
    text = script.read_text()
    for needle in ('"sts-enabled": true', '"path-style-access": true', '"flavor": "s3-compat"',
                   "--aws-sigv4", "/management/v1/bootstrap", "/management/v1/warehouse"):
        assert needle in text, needle


def test_versions_in_readme_match_the_pins():
    readme = (REPO / "README.md").read_text()
    pins = pinned_images()
    for repo, ref in pins.items():
        tag = ref.split("@", 1)[0].rsplit(":", 1)[1]
        assert tag in readme, f"README version table misses {repo}:{tag}"
    for needle in ("Iceberg 1.12.0", "Spark 4.1.3", "duckdb 1.5.6", "pyiceberg 0.12.0", "polars 1.44.2"):
        assert needle.lower() in readme.lower(), needle
