# Dummy docker file for development. Assumes code is built on your machine and mounted to this image.

FROM openjdk:8-jre-alpine
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

EXPOSE 9090

CMD chmod +x /datahub/datahub-mae-consumer/scripts/start.sh && /datahub/datahub-mae-consumer/scripts/start.sh