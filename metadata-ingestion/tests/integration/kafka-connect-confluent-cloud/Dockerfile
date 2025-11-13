FROM python:3.11-slim

WORKDIR /app

COPY confluent_cloud_mock_api.py ./

RUN pip install --no-cache-dir flask

EXPOSE 8888

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8888/health')"

CMD ["python", "confluent_cloud_mock_api.py"]