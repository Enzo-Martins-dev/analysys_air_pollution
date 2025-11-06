FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /build
COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

FROM python:3.11-slim

ENV PYTHONUNBUFFERED 1
ENV APP_HOME /app
WORKDIR $APP_HOME

# Instala a dependência do sistema necessária para o psycopg2
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER 1000 

COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

COPY --from=builder /build/.venv /app/.venv

# Coloca o diretório de binários do venv no PATH para que os comandos sejam encontrados
ENV PATH="/app/.venv/bin:$PATH"

# Copia o código da sua aplicação
COPY backend/app.py .

# O comando de execução (uvicorn) será definido no docker-compose.yml