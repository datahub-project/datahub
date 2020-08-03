FROM openjdk:8 as builder

COPY . datahub-src
RUN cd datahub-src && ./gradlew :metadata-jobs:mce-consumer-job:build \
    && cp metadata-jobs/mce-consumer-job/build/libs/mce-consumer-job.jar ../mce-consumer-job.jar \
    && cd .. && rm -rf datahub-src

FROM openjdk:8-jre-alpine
ENV DOCKERIZE_VERSION v0.6.1
RUN apk --no-cache add curl tar \
    && curl -L https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv

COPY --from=builder /mce-consumer-job.jar /datahub/datahub-mce-consumer/bin/
COPY docker/datahub-mce-consumer/start.sh /datahub/datahub-mce-consumer/scripts/
RUN chmod +x /datahub/datahub-mce-consumer/scripts/start.sh

EXPOSE 9090

CMD /datahub/datahub-mce-consumer/scripts/start.sh