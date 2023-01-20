FROM golang:1.17-alpine3.15 AS binary
ENV DOCKERIZE_VERSION 0.6.1
WORKDIR /go/src/github.com/jwilder
RUN apk --no-cache --update add openssl git tar curl \
    && git clone https://github.com/jwilder/dockerize.git
WORKDIR /go/src/github.com/jwilder/dockerize

ENV GO111MODULE=on
RUN go mod tidy
RUN go install

FROM alpine:3
COPY --from=binary /go/bin/dockerize /usr/local/bin

RUN apk add --no-cache mysql-client curl tar bash

COPY docker/mysql-setup/init.sql /init.sql
COPY docker/mysql-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$MYSQL_HOST:$MYSQL_PORT -timeout 240s /init.sh

