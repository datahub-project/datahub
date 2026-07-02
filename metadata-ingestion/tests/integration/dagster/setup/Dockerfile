# Lightweight Dagster webserver for live integration testing of the pull source.
# Build:  docker build -t dagster-test tests/integration/dagster/setup
# Run:    docker run -p 3000:3000 dagster-test
FROM python:3.11-slim

RUN pip install --no-cache-dir dagster dagster-webserver

WORKDIR /opt/dagster/app
COPY definitions.py .

EXPOSE 3000
CMD ["dagster", "dev", "-f", "definitions.py", "-h", "0.0.0.0", "-p", "3000"]
