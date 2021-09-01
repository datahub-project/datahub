# This "container" is a workaround to pre-create search indices

# Defining environment
ARG APP_ENV=prod

FROM alpine:3 as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apk add --no-cache curl jq tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

FROM base AS prod-install

COPY docker/elasticsearch-setup/create-indices.sh /
RUN chmod 755 create-indices.sh
COPY metadata-service/restli-impl/src/main/resources/index /index

FROM base AS dev-install
# Dummy stage for development. Use local files for setup
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final
CMD if [ "$ELASTICSEARCH_USE_SSL" == "true" ]; then ELASTICSEARCH_PROTOCOL=https; else ELASTICSEARCH_PROTOCOL=http; fi \
    && if [[ -n "$ELASTICSEARCH_USERNAME" ]]; then ELASTICSEARCH_URL=$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD@$ELASTICSEARCH_HOST; else ELASTICSEARCH_URL=$ELASTICSEARCH_HOST; fi \
    && dockerize -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_URL:$ELASTICSEARCH_PORT -timeout 120s /create-indices.sh
