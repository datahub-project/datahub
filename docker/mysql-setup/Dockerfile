FROM jwilder/dockerize:0.6.1

RUN apk add --no-cache mysql-client

COPY docker/mysql-setup/init.sql /init.sql

CMD dockerize -wait tcp://$MYSQL_HOST:$MYSQL_PORT -timeout 240s \
      mysql -u $MYSQL_USERNAME -p"$MYSQL_PASSWORD" -h $MYSQL_HOST < /init.sql