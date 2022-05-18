FROM debian:10-slim

LABEL maintainer="Wolfgang Gassler"
LABEL description="XtraBackup based MySQL / MariaDB backup docker image to create incremental backups periodically"

# installing xtrabackup according to https://www.percona.com/doc/percona-xtrabackup/8.0/installation/apt_repo.html

RUN . /etc/os-release \
    && apt-get update \
    && apt-get -y install wget lsb-release gnupg2 cron curl \
    && rm -rf /etc/cron.* \
    && . /etc/os-release
RUN wget --no-check-certificate https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb \
&&  dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb \
&&  percona-release enable-only tools release
RUN apt-get update \
&& apt-get -y install percona-xtrabackup-24 \
&& apt-get -y install qpress expect


COPY scripts /scripts

VOLUME /backup /var/lib/mysql
WORKDIR /backup

CMD ["/scripts/start.sh"]
