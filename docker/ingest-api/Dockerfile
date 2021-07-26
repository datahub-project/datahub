FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-alpine3.10 as base

#add certs in this stage.
COPY ./metadata-ingestion/ /app/metadata-ingestion
RUN pip install ./metadata-ingestion/. 
COPY ./ingest-api/ /app/ingest-api
WORKDIR /app/ingest-api/ingest_api
RUN pip install -r ../requirements.txt
CMD ["python", "main.py"]

