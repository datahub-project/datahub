# Using Docker Images During Development

We've created a special `docker-compose.dev.yml` override file that should configure docker images to be easier to use
during development.

Normally, you'd rebuild your images from scratch with `docker-compose build` (or `docker-compose up --build`). However,
this takes way too long for development. It has to copy the entire repo to each image and rebuild it there.

The `docker-compose.dev.yml` file bypasses this problem by mounting binaries, startup scripts, and other data to
special, slimmed down images (of which the Dockerfile is usually defined in `<service>/debug/Dockerfile`). 

These dev images will use your _locally built code_, so you'll need to build locally with gradle first
(and every time you want to update the instance). Building locally should be much faster than building on Docker.

We highly recommend you just invoke the `docker/dev.sh` script we've included. It is pretty small if you want to read it
to see what it does, but it ends up using our `docker-compose.dev.yml` file.

## Debugging

The default dev images, while set up to use your local code, do not enable debugging by default. To enable debugging,
you need to make two small edits (don't check these changes in!).

- Add the JVM debug flags to the environment file for the service.
- Assign the port in the docker-compose file.

For example, to debug `dathaub-gms`:

```
# Add this line to docker/datahub-gms/env/docker.env. You can change the port and/or change suspend=n to y.
JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n
```

```
# Change the definition in docker/docker-compose.dev.yml to this
  datahub-gms:
    image: linkedin/datahub-gms:debug
    build:
      context: datahub-gms/debug
      dockerfile: Dockerfile
    ports:             # <--- Add this line
      - "5005:5005"    # <--- And this line. Must match port from environment file.
    volumes:
      - ./datahub-gms/start.sh:/datahub/datahub-gms/scripts/start.sh
      - ../gms/war/build/libs/:/datahub/datahub-gms/bin
```


## Tips for People New To Docker

## Accessing Logs

It is highly recommended you use [Docker Desktop's dashboard](https://www.docker.com/products/docker-desktop) to access service logs. If you double click an image it will pull up the logs for you.

### Conflicting containers

If you ran `docker/quickstart.sh` before, your machine may already have a container for DataHub. If you want to run
`docker/dev.sh` instead, ensure that the old container is removed by running `docker container prune`. The opposite also
applies.

> Note this only removes containers, not images. Should still be fast to switch between these once you've launched both
> at least once.

### Running a specific service

`docker-compose up` will launch all services in the configuration, including dependencies, unless they're already
running. If you, for some reason, wish to change this behavior, check out these example commands.

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.overrides.yml -f docker-compose.dev.yml up datahub-gms
```
Will only start `datahub-gms` and its dependencies.

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.overrides.yml -f docker-compose.dev.yml up --no-deps datahub-gms
```
Will only start `datahub-gms`, without dependencies.
