FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:elasticsearch-index-job:build \
    && cp metadata-jobs/elasticsearch-index-job/build/distributions/elasticsearch-index-job.zip ../elasticsearch-index-job.zip \
    && cd .. && rm -rf datahub-src && unzip elasticsearch-index-job.zip

RUN apt-get update && apt-get install -y wget

ENV DOCKERIZE_VERSION v0.6.1
RUN apt-get update && apt-get install -y wget \
    && wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

CMD dockerize -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
            -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT \
            -timeout 60s \
            elasticsearch-index-job/bin/elasticsearch-index-job