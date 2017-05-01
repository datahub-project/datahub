FROM ubuntu:16.10

RUN mkdir /application;

WORKDIR /application

ADD archives/jdk-8u101-linux-x64.tar.gz /application
ADD archives/backend-service.tar.gz /application

RUN ln -s jdk1.8.0_101 jdk8;

WORKDIR /application/backend-service

ENV JAVA_HOME=/application/jdk8; \
    PATH=/application/jdk8/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

CMD bin/backend-service