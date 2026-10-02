"""Shared helpers of the T0-T3 test tiers (tests/unit, tests/contract, tests/smoke, tests/parity).

Single sources of truth are read from the repo itself: image pins from docker-compose.yml, the
warehouse name and sample secrets from .env.example. Nothing here starts a container.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
for p in (REPO / "src", REPO / "scripts"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

IMAGE_RE = re.compile(r"^\s*image:\s*(?:&[\w-]+\s+)?(\S+@sha256:[0-9a-f]{64})\s*$", re.M)


def env_example() -> dict[str, str]:
    out = {}
    for line in (REPO / ".env.example").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def pinned_images(path: str = "docker-compose.yml") -> dict[str, str]:
    """repository (without tag) -> 'repo:tag@sha256:...' for every pinned image line of a compose file."""
    out = {}
    for ref in IMAGE_RE.findall((REPO / path).read_text()):
        repo = ref.split("@", 1)[0].rsplit(":", 1)[0]
        out[repo] = ref
    return out


def image(repo_suffix: str) -> str:
    for repo, ref in pinned_images().items():
        if repo.endswith(repo_suffix):
            return ref
    raise KeyError(repo_suffix)


def compose_config(*profiles: str, files: tuple[str, ...] = ("docker-compose.yml",)) -> dict:
    """`docker compose config` as JSON (needs only the docker CLI with the compose plugin, no daemon)."""
    if shutil.which("docker") is None:
        import pytest
        pytest.skip("no docker CLI")
    args = ["docker", "compose", "--env-file", ".env.example"]
    for f in files:
        args += ["-f", f]
    for p in profiles:
        args += ["--profile", p]
    p = subprocess.run(args + ["config", "--format", "json"], cwd=REPO, capture_output=True, text=True,
                       env={k: v for k, v in os.environ.items() if not k.startswith(("S3_", "POSTGRES_", "LAKEKEEPER_"))})
    assert p.returncode == 0, p.stderr
    return json.loads(p.stdout)


def http_json(url: str, data: dict | None = None, method: str | None = None, timeout: float = 10.0):
    req = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                 headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        return r.status, (json.loads(body) if body else None)


def wait_http(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status < 500:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            last = e
        time.sleep(0.3)
    raise TimeoutError(f"{url} not answering after {timeout}s: {last}")


def reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status < 500
    except Exception:
        return False


def trino_query(sql: str, base: str = "http://localhost:8088", timeout: float = 120.0) -> list[list]:
    """Minimal Trino client over the HTTP protocol (POST /v1/statement, follow nextUri)."""
    req = urllib.request.Request(f"{base}/v1/statement", data=sql.encode(), method="POST",
                                 headers={"X-Trino-User": "ldl-tests", "X-Trino-Catalog": "lakehouse"})
    with urllib.request.urlopen(req, timeout=30) as r:
        res = json.loads(r.read())
    rows, deadline = [], time.monotonic() + timeout
    while True:
        rows += res.get("data") or []
        if "error" in res:
            raise RuntimeError(res["error"].get("message"))
        nxt = res.get("nextUri")
        if not nxt:
            return rows
        if time.monotonic() > deadline:
            raise TimeoutError(sql)
        with urllib.request.urlopen(nxt, timeout=30) as r:
            res = json.loads(r.read())
