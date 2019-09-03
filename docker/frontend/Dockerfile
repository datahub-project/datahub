FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :datahub-frontend:dist \
    && cp datahub-frontend/build/distributions/datahub-frontend.zip ../datahub-frontend.zip \
    && cd .. && rm -rf datahub-src && unzip datahub-frontend.zip

RUN apt-get update && apt-get install -y wget

RUN wget https://github.com/vishnubob/wait-for-it/blob/master/wait-for-it.sh

EXPOSE 9001

CMD ["./wait-for-it.sh", "$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT"]
CMD ["datahub-frontend/bin/playBinary"]