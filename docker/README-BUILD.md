The docker build project contains a list of quickstart_configs - each of which define a docker compose profile to be used
along with the dependencies to be used.

The quickstart_configs the following configs added for to help CI build images for publishing -- and not really useful for bringing up the services.

- allImages : This contains dependencies of all modules that produce docker images and published into AWS ECR.
- saasImages: this contains all dependencies that need to be published into acrylio registry. This excludes datahub-ingestion and datahub-actions.

The following tasks are generated for each quickstart_config

- `<config>reload`: For each quickstart_config, a corresponding reload task is generated. The task detects if the modules required by that container are out-of-date, builds them and restarts the corresponding docker container. The name of this task is the quickstart_config without the quickstart prefix. For example, for `quickstartDebug`, the reload task is named `debugReload`

- `<config>reloadEnv`: Similar to \*reload, but also re-creates all containers that support reload - to enable new env files to get used. The env files are loaded into the container metadata during container creation, so even if the env file is changed, a container restart does not cause the new environment vars to get used, a re-create is required to update the container metadata

- `buildImages<config>` : Used to build all images required by the quickstart config. For example, buildquickstartdebug builds all images required by quickstartdebug, but typically used in CI using `gw :docker:buildImagesAllImages`. This task uses the docker bake spec to build all the images. If an env var `DOCKER_CACHE` is set to `DEPOT`, this task uses `depot` cli to build the docker images using depot's remote container builder. This is definetly faster in CI, but it may or may not be faster depending on the network roundtrip cost. If DOCKER_CACHE is not set, the bake file is build using `docker buildx`.

The following project properties can be set to alter the build process. Each of these properties is to be passed via `-P<flag>=<value>`

- `tag` : sets the tag to be used for all images. Overrides the tag set in the build project via the name property of the docker extension

- `shaTag`: Used to set an additional tag that has the image sha. This is injected into the variants before the suffix when applicable

- `pythonDockerVersion`: A python format version used to embed in python based docker images. The actual usage is project specific, this is typically used in the buildArgs.

- `dockerRegistry`: Used to override the docker registry to be used.

- `dockerPush`: Does a docker push on the images built. When using depot, the push is performed from the depot remote container builders. Expects `docker login` to have already been called before running the build commmand

- `matrixBuild`: If set to true, builds all variants when applicable and ["linux/amd64", "linux/arm64/v8"] images as part of the build process using the bake spec capabilities for variant matrix. Note, while `docker buildx` does support matrix builds, it does not support matrix + multi-platform builds. Local builds using docker buildx only builds images that default to local platform. Builds using `depot` used in CI do support multi-platform matrix builds.

To use depot, the following setup is required

- env var `DOCKER_CACHE=DEPOT`

- env var `DEPOT_PROJECT_ID=<depot project id>`

- env var `DEPOT_TOKEN=<depot token>`
  This is required if running locally or on a non-depot runner. If using depot runner, and if depot setup task has been run, DEPOT_TOKEN is not required. DEPOT authentication depends on a trust setup in depot.dev between depot and specific github repositories which is automatically configured in depot runners used in that github repo actions.
