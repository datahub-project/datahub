FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y wget \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get -y install ./google-chrome-stable_current_amd64.deb

ENV CI=true

COPY . datahub-src
RUN cd datahub-src && ./gradlew :datahub-frontend:dist \
    && cp datahub-frontend/build/distributions/datahub-frontend.zip ../datahub-frontend.zip \
    && cd .. && rm -rf datahub-src && unzip datahub-frontend.zip

FROM openjdk:8-jre-alpine

RUN addgroup -S datahub && adduser -S datahub -G datahub

COPY --from=builder /datahub-frontend /datahub-frontend/
RUN chown -R datahub:datahub /datahub-frontend && chmod 755 /datahub-frontend
USER datahub
EXPOSE 9001

ENV JAVA_OPTS=" \
	-Xms512m \
	-Xmx1024m \
	-Dhttp.port=9001 \
	-Dconfig.file=datahub-frontend/conf/application.conf \
	-Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf \
	-Dlogback.configurationFile=datahub-frontend/conf/logback.xml \
	-Dlogback.debug=true \
	-Dpidfile.path=/datahub-frontend/play.pid"
CMD ["datahub-frontend/bin/playBinary"]