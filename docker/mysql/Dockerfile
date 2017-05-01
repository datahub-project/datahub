FROM mysql:5.7.17

RUN mkdir /usr/local/wherehows; echo "alias ll=ls -al" > /etc/profile;

COPY binary/*.sh /usr/local/bin/
COPY ETL_DDL /usr/local/wherehows/ETL_DDL
COPY WEB_DDL /usr/local/wherehows/WEB_DDL
COPY binary/*.sql /usr/local/wherehows/

HEALTHCHECK --interval=5s --timeout=3s CMD mysqladmin -pwherehows ping

VOLUME /var/lib/mysql
VOLUME /var/log/mysql

# note that the default command is still 'mysqld'
ENTRYPOINT /usr/local/bin/run-for-wherehows.sh
