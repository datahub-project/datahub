FROM golang:1-alpine3.17 AS binary

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM alpine:3
COPY --from=binary /go/bin/dockerize /usr/local/bin

RUN apk add --no-cache mysql-client bash

COPY docker/mysql-setup/init.sql /init.sql
COPY docker/mysql-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$MYSQL_HOST:$MYSQL_PORT -timeout 240s /init.sh

