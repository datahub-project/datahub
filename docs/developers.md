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
./gradlew quickstart
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

## Windows Compatibility

For optimal performance and compatibility, we strongly recommend building on a Mac or Linux system.
Please note that we do not actively support Windows in a non-virtualized environment.

If you must use Windows, one workaround is to build within a virtualized environment, such as a VM(Virtual Machine) or [WSL(Windows Subsystem for Linux)](https://learn.microsoft.com/en-us/windows/wsl).
This approach can help ensure that your build environment remains isolated and stable, and that your code is compiled correctly.

## Common Build Issues

Please refer to [Build Debugging Guide](troubleshooting/build.md)
