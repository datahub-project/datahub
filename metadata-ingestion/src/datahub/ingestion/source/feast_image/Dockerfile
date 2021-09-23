# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.8-slim-buster

# Allow statements and log messages to immediately appear
ENV PYTHONUNBUFFERED True
# Disable since it will slow things down
ENV FEAST_TELEMETRY False

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY requirements.txt ./

# install dependencies
RUN python -m pip install -r requirements.txt

# Copy everything else
COPY . ./