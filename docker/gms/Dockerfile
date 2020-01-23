FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :gms:war:build \
    && cp gms/war/build/libs/war.war ../gms.war && cd .. && rm -rf datahub-src

RUN apt-get update && apt-get install -y wget

RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.20.v20190813/jetty-runner-9.4.20.v20190813.jar

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

EXPOSE 8080