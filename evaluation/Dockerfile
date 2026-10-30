# The harness image: the measurement tools (tools/), with the dependencies uv.lock
# pins. The runner builds it as dmp-eval-harness:local and starts every tool from
# it on the testbed network.
FROM ghcr.io/astral-sh/uv:0.5.8@sha256:0bc959d4cc56e42cbd9aa9b63374d84481ee96c32803eea30bd7f16fd99d8d56 AS uv

FROM python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/venv UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /harness
COPY pyproject.toml uv.lock ./
# The runtime dependencies, not aas-model. tcpdump records residence (tools/residence.py).
RUN apt-get update && apt-get install -y --no-install-recommends tcpdump && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --no-install-project
ENV PATH=/venv/bin:$PATH
COPY tools ./tools
