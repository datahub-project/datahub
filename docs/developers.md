---
title: "Local Development"
---

# DataHub Developer's Guide

## Pre-requirements
 - [Java 11 SDK](https://openjdk.org/projects/jdk/11/)
 - [Docker](https://www.docker.com/)
 - [Docker Compose](https://docs.docker.com/compose/)
 - Docker engine with at least 8GB of memory to run tests.

 :::note

 Do not try to use a JDK newer than JDK 11. The build process does not work with newer JDKs currently.

 :::

## Building the Project

Fork and clone the repository if haven't done so already
```
git clone https://github.com/{username}/datahub.git
```

Change into the repository's root directory
```
cd datahub
```

Use [gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) to build the project
```
./gradlew build
```

Note that the above will also run run tests and a number of validations which makes the process considerably slower.

We suggest partially compiling DataHub according to your needs:

 - Build Datahub's backend GMS (Generalized metadata service):
```
./gradlew :metadata-service:war:build
```
 - Build Datahub's frontend:
```
./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint
```
 - Build DataHub's command line tool:
```
./gradlew :metadata-ingestion:installDev
```
 - Build DataHub's documentation:
```
./gradlew :docs-website:yarnLintFix :docs-website:build -x :metadata-ingestion:runPreFlightScript
# To preview the documentation
./gradlew :docs-website:serve
```

## Deploying local versions

Run just once to have the local `datahub` cli tool installed in your $PATH
```
cd smoke-test/
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
cd ../
```

Once you have compiled & packaged the project or appropriate module you can deploy the entire system via docker-compose by running:
```
datahub docker quickstart --build-locally
```

Replace whatever container you want in the existing deployment.
I.e, replacing datahub's backend (GMS):
```
(cd docker && COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -p datahub -f docker-compose-without-neo4j.yml -f docker-compose-without-neo4j.override.yml -f docker-compose.dev.yml up -d --no-deps --force-recreate datahub-gms)
```

Running the local version of the frontend
```
(cd docker && COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -p datahub -f docker-compose-without-neo4j.yml -f docker-compose-without-neo4j.override.yml -f docker-compose.dev.yml up -d --no-deps --force-recreate datahub-frontend-react)
```
## IDE Support
The recommended IDE for DataHub development is [IntelliJ IDEA](https://www.jetbrains.com/idea/). 
You can run the following command to generate or update the IntelliJ project file
```
./gradlew idea
```
Open `datahub.ipr` in IntelliJ to start developing!

For consistency please import and auto format the code using [LinkedIn IntelliJ Java style](../gradle/idea/LinkedIn%20Style.xml).

## Common Build Issues

### Getting `Unsupported class file major version 57`

You're probably using a Java version that's too new for gradle. Run the following command to check your Java version
```
java --version
```
While it may be possible to build and run DataHub using newer versions of Java, we currently only support [Java 11](https://openjdk.org/projects/jdk/11/) (aka Java 11).

### Getting `cannot find symbol` error for `javax.annotation.Generated`

Similar to the previous issue, please use Java 1.8 to build the project.
You can install multiple version of Java on a single machine and switch between them using the `JAVA_HOME` environment variable. See [this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more details.

### `:metadata-models:generateDataTemplate` task fails with `java.nio.file.InvalidPathException: Illegal char <:> at index XX` or `Caused by: java.lang.IllegalArgumentException: 'other' has different root` error

This is a [known issue](https://github.com/linkedin/rest.li/issues/287) when building the project on Windows due a bug in the Pegasus plugin. Please build on a Mac or Linux instead. 

### Various errors related to `generateDataTemplate` or other `generate` tasks

As we generate quite a few files from the models, it is possible that old generated files may conflict with new model changes. When this happens, a simple `./gradlew clean` should reosolve the issue. 

### `Execution failed for task ':metadata-service:restli-servlet-impl:checkRestModel'`

This generally means that an [incompatible change](https://linkedin.github.io/rest.li/modeling/compatibility_check) was introduced to the rest.li API in GMS. You'll need to rebuild the snapshots/IDL by running the following command once
```
./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore
```

### `java.io.IOException: No space left on device`

This means you're running out of space on your disk to build. Please free up some space or try a different disk.

### `Build failed` for task `./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint`
This could mean that you need to update your [Yarn](https://yarnpkg.com/getting-started/install) version
