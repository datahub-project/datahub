# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine

FROM alpine:3.23
COPY --from=powerman/dockerize:0.24 /usr/local/bin/dockerize /usr/local/bin

ARG ALPINE_REPO_URL

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk add --no-cache postgresql-client sqlite

COPY docker/postgres-setup/init.sql /init.sql
COPY docker/postgres-setup/init-cdc.sql /init-cdc.sql
COPY docker/postgres-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$POSTGRES_HOST:$POSTGRES_PORT -timeout 240s /init.sh
