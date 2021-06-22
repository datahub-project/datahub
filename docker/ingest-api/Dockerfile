FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8 as base
#FROM docker.artifact.xtraman.org/python:3.8 as base
#add certs in this stage.
COPY ./metadata-ingestion/ /app/metadata-ingestion
RUN pip install ./metadata-ingestion/.
COPY ./ingest-api/ /app
WORKDIR /app
CMD ["python", "main.py"]

