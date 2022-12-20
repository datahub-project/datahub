FROM --platform=$BUILDPLATFORM alpine:3

ENV DOCKERIZE_VERSION v0.6.1
RUN apk add --no-cache postgresql-client curl tar \
    && curl -sSL https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

COPY docker/postgres-setup/init.sql /init.sql
COPY docker/postgres-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$POSTGRES_HOST:$POSTGRES_PORT -timeout 240s /init.sh
