# Build Debugging Guide

For when [Local Development](/docs/developers.md) did not work out smoothly.

## Getting `Unsupported class file major version 57`

You're probably using a Java version that's too new for gradle. Run the following command to check your Java version

```
java --version
```

While it may be possible to build and run DataHub using newer versions of Java, we currently only support [Java 17](https://openjdk.org/projects/jdk/17/) (aka Java 17).

## Getting `cannot find symbol` error for `javax.annotation.Generated`

Similar to the previous issue, please use Java 17 to build the project.
You can install multiple version of Java on a single machine and switch between them using the `JAVA_HOME` environment variable. See [this document](https://docs.oracle.com/cd/E21454_01/html/821-2531/inst_jdk_javahome_t.html) for more details.

## `:metadata-models:generateDataTemplate` task fails with `java.nio.file.InvalidPathException: Illegal char <:> at index XX` or `Caused by: java.lang.IllegalArgumentException: 'other' has different root` error

This is a [known issue](https://github.com/linkedin/rest.li/issues/287) when building the project on Windows due a bug in the Pegasus plugin. Please refer to [Windows Compatibility](/docs/developers.md#windows-compatibility).

## Various errors related to `generateDataTemplate` or other `generate` tasks

As we generate quite a few files from the models, it is possible that old generated files may conflict with new model changes. When this happens, a simple `./gradlew clean` should reosolve the issue.

## `Execution failed for task ':metadata-service:restli-servlet-impl:checkRestModel'`

This generally means that an [incompatible change](https://linkedin.github.io/rest.li/modeling/compatibility_check) was introduced to the rest.li API in GMS. You'll need to rebuild the snapshots/IDL by running the following command once

```
./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore
```

## `java.io.IOException: No space left on device`

This means you're running out of space on your disk to build. Please free up some space or try a different disk.

## `Build failed` for task `./gradlew :datahub-frontend:dist -x yarnTest -x yarnLint`

This could mean that you need to update your [Yarn](https://yarnpkg.com/getting-started/install) version
