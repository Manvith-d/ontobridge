FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src src
RUN pip install --no-cache-dir .
COPY ontology ontology
COPY data data

ENV ONTOBRIDGE_DATA=/app/data \
    ONTOBRIDGE_ONTOLOGY=/app/ontology \
    ONTOBRIDGE_HOME=/app/runtime \
    ONTOBRIDGE_LLM=none
RUN useradd --create-home app && mkdir runtime && chown app runtime
USER app

EXPOSE 8000
CMD ["uvicorn", "ontobridge.services.pipeline_api:app", "--host", "0.0.0.0", "--port", "8000"]
