---
title: "Local Development"
---

# DataHub Developer's Guide

## Requirements

- [Java 17 JDK](https://openjdk.org/projects/jdk/17/)
- [Python 3.10](https://www.python.org/downloads/release/python-3100/)
- [Docker](https://www.docker.com/)
- [Node 22.x](https://nodejs.org/en/about/previous-releases)
- [Docker Compose >=2.20](https://docs.docker.com/compose/)
- Docker engine with at least 8GB of memory to run tests.

On macOS, these can be installed using [Homebrew](https://brew.sh/).

```shell
# Install Java
brew install openjdk@17

# Install Python
brew install python@3.10  # you may need to add this to your PATH
# alternatively, you can use pyenv to manage your python versions

# Install docker and docker compose
brew install --cask docker
```

## Building the Project

Fork and clone the repository if haven't done so already

```shell
git clone https://github.com/{username}/datahub.git
```

Change into the repository's root directory

```shell
cd datahub
```

Use [gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) to build the project

```shell
./gradlew build
```

Note that the above will also run tests and a number of validations which makes the process considerably slower.

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

##  Deploying Local Versions
This guide explains how to set up and deploy DataHub locally for development purposes.

###  Initial Setup
Before you begin, you'll need to install the local `datahub` CLI tool:

```shell
cd metadata-ingestion/
python3 -m venv venv
source venv/bin/activate
cd ../
```

### Deploying the Full Stack

Deploy the entire system using docker-compose:
```shell
./gradlew quickstartDebug
```

Access the DataHub UI at `http://localhost:9002`

### Refreshing the Frontend

To run and update the frontend with local changes, open a new terminal and run:
```shell
cd datahub-web-react
yarn install && yarn start
```
The frontend will be available at `http://localhost:3000` and will automatically update as you make changes to the code.

### Refreshing GMS

To refresh the GMS (Generalized Metadata Service) with local changes:
```shell
./gradlew :metadata-service:war:build -x test --parallel && docker restart datahub-datahub-gms-debug-1
```

### Refreshing the CLI 

If you haven't set up the CLI for local development yet, run:

```commandline
./gradlew :metadata-ingestion:installDev
cd metadata-ingestion
source venv/bin/activate
```

Once you're in `venv`, your local changes will be reflected automatically. 
For example, you can run `datahub ingest -c <file>` to test local changes in ingestion connectors.

To verify that you're using the local version, run:

```commandline
datahub --version
```

Expected Output:
```commandline
acryl-datahub, version unavailable (installed in develop mode)
```

### Refreshing Other Components

To refresh other components with local changes, just run:
```commandline
./gradlew quickstartDebug
```


## IDE Support

The recommended IDE for DataHub development is [IntelliJ IDEA](https://www.jetbrains.com/idea/).
You can run the following command to generate or update the IntelliJ project file.

```shell
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

#### Getting `Unsupported class file major version 57`

You're probably using a Java version that's too new for gradle. Run the following command to check your Java version

```shell
java --version
```

While it may be possible to build and run DataHub using newer versions of Java, we currently only support [Java 17](https://openjdk.org/projects/jdk/17/) (aka Java 17).

#### Getting `cannot find symbol` error for `javax.annotation.Generated`

Similar to the previous issue, please use Java 17 to build the project.
You can install multiple version of Java on a single machine and switch between them using the `JAVA_HOME` environment variable. See [this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more details.

#### `:metadata-models:generateDataTemplate` task fails with `java.nio.file.InvalidPathException: Illegal char <:> at index XX` or `Caused by: java.lang.IllegalArgumentException: 'other' has different root` error

This is a [known issue](https://github.com/linkedin/rest.li/issues/287) when building the project on Windows due a bug in the Pegasus plugin. Please refer to [Windows Compatibility](/docs/developers.md#windows-compatibility).

#### Various errors related to `generateDataTemplate` or other `generate` tasks

As we generate quite a few files from the models, it is possible that old generated files may conflict with new model changes. When this happens, a simple `./gradlew clean` should reosolve the issue.

#### `Execution failed for task ':metadata-service:restli-servlet-impl:checkRestModel'`

This generally means that an [incompatible change](https://linkedin.github.io/rest.li/modeling/compatibility_check) was introduced to the rest.li API in GMS. You'll need to rebuild the snapshots/IDL by running the following command once

```shell
./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore
```

#### `java.io.IOException: No space left on device`

This means you're running out of space on your disk to build. Please free up some space or try a different disk.

#### `Build failed` for task `./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint`

This could mean that you need to update your [Yarn](https://yarnpkg.com/getting-started/install) version

#### `:buildSrc:compileJava` task fails with `package com.linkedin.metadata.models.registry.config does not exist` and `cannot find symbol` error for `Entity`

There are currently two symbolic links within the [buildSrc](https://github.com/datahub-project/datahub/tree/master/buildSrc) directory for the [com.linkedin.metadata.aspect.plugins.config](https://github.com/datahub-project/datahub/blob/master/buildSrc/src/main/java/com/linkedin/metadata/aspect/plugins/config) and [com.linkedin.metadata.models.registry.config](https://github.com/datahub-project/datahub/blob/master/buildSrc/src/main/java/com/linkedin/metadata/models/registry/config
) packages, which points to the corresponding packages in the [entity-registry](https://github.com/datahub-project/datahub/tree/master/entity-registry) subproject.

When the repository is checked out using Windows 10/11 - even if WSL is later used for building using the mounted Windows filesystem in `/mnt/` - the symbolic links might have not been created correctly, instead the symbolic links were checked out as plain files. Although it is technically possible to use the mounted Windows filesystem in `/mnt/` for building in WSL, it is **strongly recommended** to checkout the repository within the Linux filesystem (e.g., in `/home/`) and building it from there, because accessing the Windows filesystem from Linux is relatively slow compared to the Linux filesystem and slows down the whole building process.

To be able to create symbolic links in Windows 10/11 the [Developer Mode](https://learn.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development) has to be enabled first. Then the following commands can be used to enable [symbolic links in Git](https://git-scm.com/docs/git-config#Documentation/git-config.txt-coresymlinks) and recreating the symbolic links:

```shell
# enable core.symlinks config
git config --global core.symlinks true

# check the current core.sysmlinks config and scope
git config --show-scope --show-origin core.symlinks

# in case the core.sysmlinks config is still set locally to false, remove the local config
git config --unset core.symlinks

# reset the current branch to recreate the missing symbolic links (alternatively it is also possibly to switch branches away and back)
git reset --hard
```

See also [here](https://stackoverflow.com/questions/5917249/git-symbolic-links-in-windows/59761201#59761201) for more information on how to enable symbolic links on Windows 10/11 and Git.
