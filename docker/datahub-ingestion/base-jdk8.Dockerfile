FROM openjdk:8

COPY gradle /tmp-gradle/gradle
COPY gradle* /tmp-gradle

RUN /tmp-gradle/gradlew --version && rm -rf /tmp-gradle