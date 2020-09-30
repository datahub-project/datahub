# DataHub Developer's Guide

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
While it may be possible to build and run DataHub using newer versions of Java, we currently only support [Java 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) (aka Java 8). Plan for Java 11 migration is being discussed in [this issue](https://github.com/linkedin/datahub/issues/1699).

### Getting `cannot find symbol` error for `javax.annotation.Generated`

Similar to the previous issue, please use Java 1.8 to build the project.
You can install multiple version of Java on a single machine and switch between them using the `JAVA_HOME` environment variable. See [this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more details.

### `:metadata-models:generateDataTemplate` task fails with `java.nio.file.InvalidPathException: Illegal char <:> at index XX` or `Caused by: java.lang.IllegalArgumentException: 'other' has different root` error

This is a [known issue](https://github.com/linkedin/rest.li/issues/287) when building the project on Windows due a bug in the Pegasus plugin. Please build on a Mac or Linux instead. 

### Various errors related to `generateDataTemplate` or other `generate` tasks

As we generate quite a few files from the models, it is possible that old generated files may conflict with new model changes. When this happens, a simple `./gradlew clean` should reosolve the issue. 

### `Execution failed for task ':gms:impl:checkRestModel'`

This generally means that an [incompatible change](https://linkedin.github.io/rest.li/modeling/compatibility_check) was introduced to the rest.li API in GMS. You'll need to rebuild the snapshots/IDL by running the following command once
```
./gradlew :gms:impl:build -Prest.model.compatibility=ignore
```

### `java.io.IOException: No space left on device`

This means you're running out of space on your disk to build. Please free up some space or try a different disk.
