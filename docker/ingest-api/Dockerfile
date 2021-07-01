FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8 as base

#add certs in this stage.
COPY ./metadata-ingestion/ /app/metadata-ingestion
COPY ./ingest-api/ /app/ingest-api
RUN pip install ./metadata-ingestion/. 
RUN pip install ./ingest-api/.
WORKDIR /app/ingest-api/ingest_api
RUN mkdir log
CMD ["python", "main.py"]

