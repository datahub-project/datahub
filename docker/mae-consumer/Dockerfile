FROM openjdk:8 as builder

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mae-consumer-job:build \
    && cp metadata-jobs/mae-consumer-job/build/distributions/mae-consumer-job.zip ../mae-consumer-job.zip \
    && cd .. && rm -rf datahub-src && unzip mae-consumer-job.zip

FROM openjdk:8-jre-alpine

COPY --from=builder /mae-consumer-job /mae-consumer-job/
