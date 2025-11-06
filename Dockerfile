FROM apache/airflow:3.1.1-python3.11 

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libpq-dev \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:$PATH"

USER airflow

COPY pyproject.toml /opt/airflow/
COPY uv.lock /opt/airflow/

WORKDIR /opt/airflow/ # Define o diretório de trabalho para que uv encontre os arquivos

RUN uv pip install --no-cache-dir \
    --locked \
    . \  # Instala as dependências do pyproject.toml do diretório atual
    'apache-airflow-providers-postgres' 