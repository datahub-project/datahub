# Casandra

DataHub GMS puede usar Cassandra como backend de almacenamiento alternativo. Los pasos siguientes configuran una instancia de inicio rápido en el equipo local.

## Generar el proyecto

Construye este proyecto con Gradle.

    ./gradlew build

Si ve este error:

    Required org.gradle.jvm.version '8' and found incompatible value '11'.

A continuación, debe configurar `JAVA_HOME` a Java 8 primero e inténtelo de nuevo:

    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    $JAVA_HOME/bin/java -version

A continuación, compile e inicie todos los componentes de DataHub desde el origen con el backend de Cassandra:

    ./docker/dev-with-cassandra.sh

Una vez que todos los servicios estén activos, inicie la CLI de DataHub proporcionada por el proyecto de origen en un terminal independiente
e ingiera algunos datos de muestra en DataHub:

    source metadata-ingestion/venv/bin/activate
    datahub docker ingest-sample-data
