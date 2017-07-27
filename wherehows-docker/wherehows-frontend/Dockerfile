FROM openjdk:8

RUN apt-get update && \
    apt-get install -y wget

# Install dockerize
ENV DOCKERIZE_VERSION v0.5.0
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Setup app environment
ADD tmp/wherehows-frontend /application
WORKDIR /application
RUN mkdir -p logs