# Cassandra

DataHub GMS can use Cassandra as an alternate storage backend. The following steps set up a quickstart instance on your local machine.

## Build the project

Build this project with Gradle.

    ./gradlew build

If you see this error:

    Required org.gradle.jvm.version '8' and found incompatible value '11'.

then you need to set `JAVA_HOME` to Java 8 first and try again:

    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    $JAVA_HOME/bin/java -version

Next build and start all DataHub components from source with Cassandra backend:

    ./docker/dev-with-cassandra.sh

Once all services are up, start the DataHub CLI provided by the source project in a separate terminal
and ingest some sample data into DataHub:

    source metadata-ingestion/venv/bin/activate
    datahub docker ingest-sample-data
