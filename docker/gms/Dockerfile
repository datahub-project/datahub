FROM openjdk:8 as builder
MAINTAINER Kerem Sahin ksahin@linkedin.com
COPY . datahub-src
RUN cd datahub-src && ./gradlew :gms:war:build \
    && cp gms/war/build/libs/war.war ../gms.war && cd .. && rm -rf datahub-src


FROM openjdk:8-jre-alpine
MAINTAINER Kerem Sahin ksahin@linkedin.com
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.20.v20190813/jetty-runner-9.4.20.v20190813.jar --output jetty-runner-9.4.20.v20190813.jar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

COPY --from=builder /gms.war .

EXPOSE 8080
