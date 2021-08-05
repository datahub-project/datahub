# Defining environment
ARG TRUSTSTORE_LOCATION
FROM python:3.8-slim-buster as base
ARG TRUSTSTORE_LOCATION
COPY $TRUSTSTORE_LOCATION Dockerfil2
ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]