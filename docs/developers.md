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

## Common Build Issues

### Getting `Unsupported class file major version 57`

You're probably using a Java version that's too new for gradle. Run the following command to check your Java version
```
java --version
```
While it may be possible to build and run DataHub using newer versions of Java, we currently only support [Java 1.8](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html) (aka Java 8).

### `:metadata-models:generateDataTemplate` task fails with `java.nio.file.InvalidPathException: Illegal char <:> at index XX` error

This is a known issue when building the project on Windows due a bug in the Pegasus plugin. Please build on a Mac or Linux instead. 
