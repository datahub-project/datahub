FROM python:3.11-slim

WORKDIR /app

COPY confluent_cloud_mock_api.py ./

RUN pip install --no-cache-dir flask requests

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8888

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8888/health || exit 1

CMD ["python", "confluent_cloud_mock_api.py"]