This module is used to publish docker images to acryldata's docker repository. 

# Overview

This Docker container contains the Event Sources, Transformers, and Actions that ship with the DataHub Actions CLI by default. 

On deployment, the container simply executes the `datahub actions` command, passing configuration files found for 2 types of Action:

1. **System Actions**: Those actions which are useful for operating the DataHub platform. This includes the `executor` action which is responsible for UI-based ingestion. Configuration files are located inside of the container directory `/etc/datahub/actions/system/conf/`.

2. **User Actions**: Custom Actions which are configuration by mounting an Action configuration file. Configuration files are located inside of the container directory `/etc/datahub/actions/conf/`.

Configuration files for each are found by scanning the container filesystem the locations mentioned above.

# Building the Docker Image 

From the root directory,

```
docker build -f docker/datahub-actions/Dockerfile . --no-cache
```

# Running the Docker Image

```
docker image ls # Grab the container id 
docker run --env-file docker/actions.env --network datahub_network <image-id>
```

# Deploying a Custom Action Config

The Docker image supports mounting custom Action configuration files. Each configuration file will be launched as a separate Action pipeline when the container boots up.

To deploy a custom Action configuration, simply plac

Note that the expectation is that the container contains all required plugins to execute the Action configuration. If the action you want to use does not come pre-packaged with DataHub's Actions CLI, you'll need to install the required plugins inside the Dockerfile itself. 

For example, to mount the "hello_world.yml" configuration located under the `examples` directory of this repository can be achieved via:

```
docker run --env-file docker/actions.env --network datahub_network --mount type=bind,source="$(pwd)"/examples/hello_world.yaml,target=/etc/datahub/actions/conf/hello_world.yaml <image-id>
```

# Overwriting a System Action Config

The Docker image comes pre-packaged with a system Action which is useful
for operating DataHub. Currently this includes only the "executor" action which is required for running UI-based ingestion. 

All system actions have a corresponding config file inside the container. These are located inside the container directory `/etc/datahub/actions/system/conf`. In order to overwrite a default config file with your own (for example, to override the default Executor configuration), simply mount a custom configuration file at the corresponding path when launching the Docker container. 

For example, to provide a custom `custom-executor.yml` file that overwrites the default configuration, you'd mount the file at `/etc/datahub/actions/system/conf/executor.yml`. To mount the "executor.yml" configuration located under the `examples` directory of this repository can be achieved via:

```
docker run --env-file docker/actions.env --network datahub_network --mount type=bind,source="$(pwd)"/examples/executor.yaml,target=/etc/datahub/actions/system/conf/executor.yaml <image-id>
```
