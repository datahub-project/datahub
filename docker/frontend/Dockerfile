FROM openjdk:8

MAINTAINER Kerem Sahin ksahin@linkedin.com

RUN apt-get update && apt-get install -y wget \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

ENV CI=true

COPY . datahub-src
RUN cd datahub-src && ./gradlew :datahub-frontend:dist -Prest.model.compatibility=ignore \
    && cp datahub-frontend/build/distributions/datahub-frontend.zip ../datahub-frontend.zip \
    && cd .. && rm -rf datahub-src && unzip datahub-frontend.zip

RUN wget https://github.com/vishnubob/wait-for-it/blob/master/wait-for-it.sh

EXPOSE 9001

CMD ["./wait-for-it.sh", "$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT"]

ENV JAVA_OPTS=" \
	-Xms512m \
	-Xmx1024m \
	-Dhttp.port=9001 \
	-Dconfig.file=datahub-frontend/conf/application.conf \
	-Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf \
	-Dlogback.configurationFile=datahub-frontend/conf/logback.xml \
	-Dlogback.debug=true"
CMD ["datahub-frontend/bin/playBinary"]