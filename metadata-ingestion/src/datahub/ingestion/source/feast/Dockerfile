# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.8-slim-buster

# Allow statements and log messages to immediately appear
ENV PYTHONUNBUFFERED True
# slows stuff down
ENV FEAST_TELEMETRY False

# install system dependencies
# RUN apt-get update

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy only requirements to cache them in docker layer
WORKDIR $APP_HOME
COPY requirements.txt ./

# install dependencies
RUN python -m pip install -r requirements.txt

# Copy everything else
COPY . ./

CMD ["ingest.py"]
ENTRYPOINT ["python3"]