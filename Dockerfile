FROM python:3.9.16-slim-buster

COPY . /app
WORKDIR /app

ENV PYTHONPATH=/app

COPY pyproject.toml .
COPY pdm.lock .

RUN pip install -U pip setuptools wheel
RUN pip install pdm zstandard

RUN pdm config python.use_venv false
RUN pdm install --prod --frozen-lockfile --no-editable

ENTRYPOINT ["pdm", "run", "src/server.py"]
