# Imagem Gradle + Java + NodeJS
FROM gradle:8.0.2-jdk17-alpine

RUN apk update && apk add --no-cache curl bash

RUN apk add --no-cache nodejs npm

RUN mkdir /.npm
RUN chown -R 1000890000:0 /.npm