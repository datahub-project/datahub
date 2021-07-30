FROM jwilder/dockerize:0.6.1

RUN apk add --no-cache postgres-client

COPY docker/postgres-setup/init.sql /init.sql
COPY docker/postgres-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$POSTGRES_HOST:$POSTGRES_PORT -timeout 240s /init.sh
