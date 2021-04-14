FROM jwilder/dockerize:0.6.1

RUN apk add --no-cache mysql-client

COPY docker/mysql-setup/init.sql /init.sql
COPY docker/mysql-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME = "datahub"

CMD dockerize -wait tcp://$MYSQL_HOST:$MYSQL_PORT -timeout 240s /init.sh