# This "container" is a workaround to pre-create search indices

# Defining environment
ARG APP_ENV=prod

FROM jwilder/dockerize:0.6.1 AS base

RUN apk add --no-cache curl jq

FROM base AS prod-install

COPY docker/elasticsearch-setup/create-indices.sh /
RUN chmod 755 create-indices.sh
COPY gms/impl/src/main/resources/index /index

FROM base AS dev-install
# Dummy stage for development. Use local files for setup
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final
CMD if [ "$ELASTICSEARCH_USE_SSL" == "true" ]; then ELASTICSEARCH_PROTOCOL=https; else ELASTICSEARCH_PROTOCOL=http; fi \
    && if [[ -n "$ELASTICSEARCH_USERNAME" ]]; then ELASTICSEARCH_URL=$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD@$ELASTICSEARCH_HOST; else ELASTICSEARCH_URL=$ELASTICSEARCH_HOST; fi \
    && dockerize -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_URL:$ELASTICSEARCH_PORT -timeout 120s /create-indices.sh
