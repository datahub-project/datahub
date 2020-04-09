FROM openjdk:8 as builder

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mae-consumer-job:build \
    && cp metadata-jobs/mae-consumer-job/build/libs/mae-consumer-job.jar ../mae-consumer-job.jar \
    && cd .. && rm -rf datahub-src

FROM openjdk:8-jre-alpine
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

COPY --from=builder /mae-consumer-job.jar /mae-consumer-job.jar

EXPOSE 9091

ENTRYPOINT ["java", "-jar", "mae-consumer-job.jar"]
