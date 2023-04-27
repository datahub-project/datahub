# This "container" is a workaround to pre-create search indices

# Defining environment
ARG APP_ENV=prod

FROM golang:1-alpine3.17 AS binary

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

RUN apk --no-cache --update add openssl git tar curl 

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM alpine:3 AS base
RUN apk add --no-cache curl jq bash coreutils
COPY --from=binary /go/bin/dockerize /usr/local/bin

FROM base AS prod-install

COPY docker/elasticsearch-setup/create-indices.sh /
RUN chmod 755 create-indices.sh
COPY metadata-service/restli-servlet-impl/src/main/resources/index /index

FROM base AS dev-install
# Dummy stage for development. Use local files for setup
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final
CMD if [ "$ELASTICSEARCH_USE_SSL" == "true" ]; then ELASTICSEARCH_PROTOCOL=https; else ELASTICSEARCH_PROTOCOL=http; fi \
    && if [[ -n "$ELASTICSEARCH_USERNAME" ]]; then ELASTICSEARCH_HTTP_HEADERS="Authorization: Basic $(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64)"; else ELASTICSEARCH_HTTP_HEADERS="Accept: */*"; fi \
    && if [[ "$SKIP_ELASTICSEARCH_CHECK" != "true" ]]; then \
        dockerize -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -wait-http-header "${ELASTICSEARCH_HTTP_HEADERS}" -timeout 120s /create-indices.sh; \
    else /create-indices.sh; fi

