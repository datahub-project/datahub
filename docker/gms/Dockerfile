FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

COPY . datahub-src
RUN cd datahub-src && ./gradlew :gms:war:build -Prest.model.compatibility=ignore \
    && cp gms/war/build/libs/war.war ../gms.war && cd .. && rm -rf datahub-src

RUN apt-get update && apt-get install -y wget

RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.20.v20190813/jetty-runner-9.4.20.v20190813.jar
RUN wget https://github.com/vishnubob/wait-for-it/blob/master/wait-for-it.sh

EXPOSE 8080

CMD ["./wait-for-it.sh", "$DATAHUB_MYSQL_HOST", "$DATAHUB_KAFKA_BROKER_HOST", "$DATAHUB_ELASTICSEARCH_HOST"]
CMD ["java", "-jar", "jetty-runner-9.4.20.v20190813.jar", "gms.war"]