FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mce-consumer-job:build \
    && cp metadata-jobs/mce-consumer-job/build/distributions/mce-consumer-job.zip ../mce-consumer-job.zip \
    && cd .. && rm -rf datahub-src && unzip mce-consumer-job.zip