FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

RUN apt-get update && apt-get install -y wget

RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-runner/9.4.20.v20190813/jetty-runner-9.4.20.v20190813.jar
RUN wget https://github.com/vishnubob/wait-for-it/blob/master/wait-for-it.sh

EXPOSE 8080

COPY gms/war/build/libs/war.war gms.war

CMD ["./wait-for-it.sh", "$DATAHUB_MYSQL_HOST:$DATAHUB_MYSQL_PORT"]
CMD ["java", "-jar", "jetty-runner-9.4.20.v20190813.jar", "gms.war"]