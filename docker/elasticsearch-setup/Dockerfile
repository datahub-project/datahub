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
CMD dockerize -wait http://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -timeout 120s /create-indices.sh
