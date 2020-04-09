FROM openjdk:8 as builder

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mce-consumer-job:build \
    && cp metadata-jobs/mce-consumer-job/build/libs/mce-consumer-job.jar ../mce-consumer-job.jar \
    && cd .. && rm -rf datahub-src

FROM openjdk:8-jre-alpine

COPY --from=builder /mce-consumer-job.jar /mce-consumer-job.jar

EXPOSE 9090

ENTRYPOINT ["java", "-jar", "mce-consumer-job.jar"]
