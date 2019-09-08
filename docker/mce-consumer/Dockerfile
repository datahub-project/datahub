FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mce-consumer-job:build \
    && cp metadata-jobs/mce-consumer-job/build/distributions/mce-consumer-job.zip ../mce-consumer-job.zip \
    && cd .. && rm -rf datahub-src && unzip mce-consumer-job.zip

RUN apt-get update && apt-get install -y wget

ENV DOCKERIZE_VERSION v0.6.1
RUN apt-get update && apt-get install -y wget \
    && wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

CMD dockerize -wait tcp://$KAFKA_BOOTSTRAP_SERVER \
            -timeout 60s \
            mce-consumer-job/bin/mce-consumer-job