***

## título: "Desarrollo Local"

# Guía del desarrollador de DataHub

## Requisitos previos

*   [Java 1.8 SDK](https://adoptopenjdk.net/?variant=openjdk8\&jvmVariant=hotspot)
*   [Estibador](https://www.docker.com/)
*   [Docker Componer](https://docs.docker.com/compose/)
*   Motor Docker con al menos 8 GB de memoria para ejecutar pruebas.

:::nota

No intente utilizar un JDK más reciente que JDK 8. El proceso de compilación no funciona con los JDK más nuevos actualmente.

:::

## Construyendo el proyecto

Bifurque y clone el repositorio si aún no lo ha hecho

    git clone https://github.com/{username}/datahub.git

Cambiar al directorio raíz del repositorio

    cd datahub

Uso [envoltorio gradle](https://docs.gradle.org/current/userguide/gradle_wrapper.html) Para generar el proyecto

    ./gradlew build

Tenga en cuenta que lo anterior también ejecutará pruebas de ejecución y una serie de validaciones, lo que hace que el proceso sea considerablemente más lento.

Sugerimos compilar parcialmente DataHub de acuerdo con sus necesidades:

*   Cree el backend GMS (servicio de metadatos generalizados) de Datahub:

<!---->

    ./gradlew :metadata-service:war:build

*   Cree el frontend de Datahub:

<!---->

    ./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint

*   Cree la herramienta de línea de comandos de DataHub:

<!---->

    ./gradlew :metadata-ingestion:installDev

*   Cree la documentación de DataHub:

<!---->

    ./gradlew :docs-website:yarnLintFix :docs-website:build -x :metadata-ingestion:runPreFlightScript
    # To preview the documentation
    ./gradlew :docs-website:serve

## Implementación de versiones locales

Corre solo una vez para tener el local `datahub` Cli herramienta instalada en el $PATH

    cd smoke-test/
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip wheel setuptools
    pip install -r requirements.txt
    cd ../

Una vez que haya compilado y empaquetado el proyecto o el módulo apropiado, puede implementar todo el sistema a través de docker-compose ejecutando:

    datahub docker quickstart --build-locally

Reemplace el contenedor que desee en la implementación existente.
Es decir, reemplazar el backend de datahub (GMS):

    (cd docker && COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -p datahub -f docker-compose-without-neo4j.yml -f docker-compose-without-neo4j.override.yml -f docker-compose.dev.yml up -d --no-deps --force-recreate datahub-gms)

Ejecución de la versión local del frontend

    (cd docker && COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -p datahub -f docker-compose-without-neo4j.yml -f docker-compose-without-neo4j.override.yml -f docker-compose.dev.yml up -d --no-deps --force-recreate datahub-frontend-react)

## Soporte IDE

El IDE recomendado para el desarrollo de DataHub es [IntelliJ IDEA](https://www.jetbrains.com/idea/).
Puede ejecutar el siguiente comando para generar o actualizar el archivo de proyecto IntelliJ

    ./gradlew idea

Abrir `datahub.ipr` en IntelliJ para empezar a desarrollar!

Para mayor coherencia, importe y formatee automáticamente el código utilizando [Estilo Java de LinkedIn IntelliJ](../gradle/idea/LinkedIn%20Style.xml).

## Problemas comunes de compilación

### Conseguir `Unsupported class file major version 57`

Probablemente estés usando una versión de Java que es demasiado nueva para gradle. Ejecute el siguiente comando para comprobar la versión de Java

    java --version

Si bien es posible compilar y ejecutar DataHub utilizando versiones más recientes de Java, actualmente solo admitimos [Java 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) (también conocido como Java 8). El plan para la migración a Java 11 se está discutiendo en [Este problema](https://github.com/datahub-project/datahub/issues/1699).

### Conseguir `cannot find symbol` error para `javax.annotation.Generated`

Similar al problema anterior, utilice Java 1.8 para compilar el proyecto.
Puede instalar varias versiones de Java en una sola máquina y cambiar entre ellas utilizando el `JAVA_HOME` variable de entorno. Ver [este documento](https://docs.oracle.com/cd/E21454\_01/html/821-2531/inst_jdk_javahome_t.html) para más detalles.

### `:metadata-models:generateDataTemplate` La tarea falla con `java.nio.file.InvalidPathException: Illegal char <:> at index XX` o `Caused by: java.lang.IllegalArgumentException: 'other' has different root` error

Este es un [problema conocido](https://github.com/linkedin/rest.li/issues/287) al construir el proyecto en Windows debido a un error en el complemento Pegasus. Por favor, construya en una Mac o Linux en su lugar.

### Varios errores relacionados con `generateDataTemplate` u otro `generate` Tareas

Como generamos bastantes archivos a partir de los modelos, es posible que los archivos generados antiguos entren en conflicto con los nuevos cambios de modelo. Cuando esto sucede, un simple `./gradlew clean` debería volver a resolver el problema.

### `Execution failed for task ':metadata-service:restli-servlet-impl:checkRestModel'`

Esto generalmente significa que un [cambio incompatible](https://linkedin.github.io/rest.li/modeling/compatibility_check) se introdujo en la API de rest.li en GMS. Deberá volver a generar las instantáneas/IDL ejecutando el siguiente comando una vez

    ./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore

### `java.io.IOException: No space left on device`

Esto significa que se está quedando sin espacio en el disco para compilar. Por favor, libere algo de espacio o pruebe un disco diferente.
