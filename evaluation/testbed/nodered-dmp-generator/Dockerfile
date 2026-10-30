# nodered-dmp's flow generator (https://github.com/rwth-iat/nodered-dmp) and its
# sibling nodered-flowgen, both pinned by commit and installed from nodered-dmp's own
# uv.lock. A one-shot container: it reads the AIMC from the repository and prints the
# Node-RED flow (generate.py).
FROM ghcr.io/astral-sh/uv:0.12.24@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a AS uv

FROM python:3.14.7-slim-bookworm@sha256:82bc3c539b8813ada9d68c63b40158fa002f7f33de9bf3312a3dfdc0620dff56
COPY --from=uv /uv /usr/local/bin/uv
ARG NODERED_DMP_COMMIT=8990adcf27d6c48f8edb15fe7998fa3cf3df1918
ARG NODERED_FLOWGEN_COMMIT=efe76ce8670ea94d5eea8b13f7a5e7e628da30ee
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# The checkouts sit side by side: nodered-dmp names ../nodered-flowgen as a path source.
RUN git clone https://github.com/rwth-iat/nodered-flowgen.git && git -C nodered-flowgen checkout "$NODERED_FLOWGEN_COMMIT" \
 && git clone https://github.com/rwth-iat/nodered-dmp.git && git -C nodered-dmp checkout "$NODERED_DMP_COMMIT"
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy PYTHONPATH=/src/nodered-dmp
# nodered-dmp is no installable package: upstream runs it from its checkout (main.py).
RUN cd nodered-dmp && uv sync --frozen --no-dev
COPY generate.py /src/generate.py
ENTRYPOINT ["/src/nodered-dmp/.venv/bin/python", "/src/generate.py"]
