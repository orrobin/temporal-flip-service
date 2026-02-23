FROM python:3.12-slim

RUN pip install --no-cache-dir poetry==1.8.5

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi

COPY exercise/ exercise/
