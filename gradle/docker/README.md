## Docker plugin notes

All docker images in DataHub are are built using a custom gradle plugin that can be configured via a Docker extension

For a gradle project to produce a docker image, include

```
apply from: "gradle/docker/docker.gradle"
```

in the project's `build.gradle`

Then define the docker extension to configure docker build.

```
docker {
  <extension Params>
}
```

The following are the support extension parameters.

The full name of the resulting image with tag. This is a required parameter.
The overall build system can replace the tag with a custom tag if one is passed during build. Custom tags are typically
passed by the CI when building images for publishing.

```
  name "dockerRepo:tag"
```

The dockerfile to be used to build the image. This is a required parameter.

```
  dockerfile "path/to/Dockerfile"
```

Files to include in the docker image. This is really a gradle CopySpec, so can refer to file or file trees. This can be repeated as many times as needed, this can refer to task outputs

```
  files bootJar.output.files
  files fileTree(rootProject.projectDir) {
    include '.dockerignore'
    include 'project-folder/*'
    include "additional-folder/*"
  }
```

Additional tags to apply -- this is retained for legacy reasons, and currently requires a debug tag to enable debug builds for local dev.
This should eventually get removed and auto-injected when needed.

```
  additionalTag("Debug", "repoName:debug")
```

Map of buildArgs to be used when building the docker image

```
  buildArgs.key = <value>
```

Build args used only for debug builds.

```
  debugBuildArgs.key = <value>
```

Any dependencies to other tasks that produce artifacts required for the image build. This can be repeated as many times

```
  dependsOn <tasks>
```

If there are multiple variants of this image like slim/full, define the build args to be used per variant and a
tag suffix to be used per variant. This enables the build system to build all the variants using the variant specific build args.
The example below create two variants. The tag for each image variant gets the suffix appended to the full tag
defined by the name parameter.

```
  variants = [
    "slim": [suffix: "-slim", args: [APP_ENV: "slim", RELEASE_VERSION: python_docker_version]],
    "full": [suffix: "", args: [APP_ENV: "full", RELEASE_VERSION: python_docker_version]]
  ]
```

If during local developement, only one of these variants are required, define the defaultVariant so that only
that variant is built by default to speed up builds. If a build time project parameter matrixBuilds is passed to
gradle ( -PmatrixBuild=true), all variants are built (and for all platforms). CI will typically use this when
building images to publish.

```
  defaultVariant = "slim"
```
