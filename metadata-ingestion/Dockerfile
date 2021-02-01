#FROM openjdk:8-jre-alpine as base
FROM python:3.7-slim AS base

#Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

ENV DOCKERIZE_VERSION v0.6.1

FROM base AS python-deps


# Install pipenv and compilation dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc

#RUN apk --no-cache add curl tar \
#    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

# Workaround alpine issue with /lib64 not being in the ld library path
# https://gitlab.alpinelinux.org/alpine/aports/-/issues/10140
#ENV LD_LIBRARY_PATH=/lib64

# Add glibc compat layer into alpine linux, needed by java-snappy if kafka topics are compressed with snappy
#RUN apk add libc6-compat

#FROM openjdk:8 as prod-build


# Copy virtual env from python-deps stage

COPY . /datahub-ingest
WORKDIR /datahub-ingest
RUN pip install -e .

RUN apt-get install -y telnet vim


#FROM ${APP_ENV}-install as final

RUN useradd --create-home datahub
USER datahub
#RUN addgroup -S datahub && adduser -S datahub -G datahub
#USER datahub

#EXPOSE PORT_NUM
ENTRYPOINT ["gometa-ingest"]
#TODO: Make the config file configurable through docker env variable
CMD ["-c","/datahub-ingest/recipes/kafka_to_console.yaml"]
