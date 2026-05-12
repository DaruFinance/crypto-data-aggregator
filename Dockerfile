# crypto-data-aggregator — runtime image.
#   docker build -t cryptodata .
#   docker run --rm -v "$PWD/data:/app/data" cryptodata cda-status
#   docker run --rm -v "$PWD/data:/app/data" -p 9464:9464 cryptodata cda-ingest
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Dependencies first (layer cache), then the package.
COPY pyproject.toml README.md ./
COPY cryptodata ./cryptodata
COPY scripts ./scripts
COPY config ./config
RUN pip install --upgrade pip && pip install ".[obs]"

# Data lives on a mounted volume.
VOLUME ["/app/data"]
EXPOSE 9464

# tini-style PID-1 handling so Ctrl-C / SIGTERM flushes the writer cleanly.
ENTRYPOINT ["python", "-m"]
CMD ["scripts.status"]
