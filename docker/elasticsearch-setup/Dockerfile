# This "container" is a workaround to pre-create search indices

# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine
# CVE-2023-45853: pin zlib to 1.3.x from edge/main for MiniZip integer/heap buffer overflow fix
ARG ZLIB_VERSION=1.3

FROM alpine:3.23 AS base

ARG ALPINE_REPO_URL
ARG ZLIB_VERSION

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk add --no-cache --update-cache 'curl>8.17.0' --repository=${ALPINE_REPO_URL}/edge/main \
    && apk add --no-cache jq bash coreutils \
    && apk add --no-cache "zlib=~${ZLIB_VERSION}" --repository=${ALPINE_REPO_URL}/edge/main
COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin

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
