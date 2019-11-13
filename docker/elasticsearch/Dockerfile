FROM openjdk:8

MAINTAINER Kerem Sahin <ksahin@linkedin.com>

RUN apt-get update && apt-get install -y wget && apt-get install -y curl

COPY corpuser-index-config.json dataset-index-config.json /

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

CMD dockerize -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT \
            -timeout 120s; \
            curl -XPUT $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/corpuserinfodocument --data @corpuser-index-config.json; \
            curl -XPUT $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/datasetdocument --data @dataset-index-config.json