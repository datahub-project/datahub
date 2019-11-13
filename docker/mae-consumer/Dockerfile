FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:elasticsearch-index-job:build \
    && cp metadata-jobs/elasticsearch-index-job/build/distributions/elasticsearch-index-job.zip ../elasticsearch-index-job.zip \
    && cd .. && rm -rf datahub-src && unzip elasticsearch-index-job.zip