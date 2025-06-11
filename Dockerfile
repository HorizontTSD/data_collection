FROM python:3.9.16-slim-buster

COPY . /app
WORKDIR /app

ENV PYTHONPATH=/app

COPY pyproject.toml .

RUN pip install -U pip setuptools wheel
RUN pip install pdm zstandard

RUN pdm config python.use_venv false
RUN pdm install --prod --no-lock --no-editable

EXPOSE 7077

ENTRYPOINT ["pdm", "run", "src/server.py"]
