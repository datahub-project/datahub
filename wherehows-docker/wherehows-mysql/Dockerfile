FROM mysql:5.6

RUN apt-get update

EXPOSE 3306
ADD init.sql /docker-entrypoint-initdb.d
