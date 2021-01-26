# This "container" is a workaround to pre-create search indices
FROM jwilder/dockerize:0.6.1

RUN apk add --no-cache curl jq

COPY docker/elasticsearch-setup/create-indices.sh /
RUN chmod 755 create-indices.sh
COPY gms/impl/src/main/resources/index /

CMD dockerize -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -timeout 120s /create-indices.sh
