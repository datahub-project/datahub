# Using this devcontainer
This devcontainer is meant to easily set up a Java 11 / Gradle environment that works on a FALCON laptop with WSL2 (v1.2+) installed. In order for it to "just work", it does make a few assumptions:
## Base image is in a private registry
The "parent" image for this devcontainer's Dockerfile is stored in an ACR that requires authentication. These steps should be executed in WSL2 before attempting to launch the devcontainer:
```bash
az login
az acr login --name uchimera --subscription <subscription_name>
```
## SSH_AUTH_SOCK
The WSL2 v1.2+ installer maps the `gpg-agent` SSH Agent to `$HOME/.ssh/agent.sock`. That file is assumed to exist and be a valid SSH agent socket.
## Scratch space
This devcontainer will create a folder named `scratch/` in your `$HOME` directory, if one doesn't already exist. This folder can be used to store temporary / transient data and to share files with other devcontainers that also map the `scratch/` folder.
## Caches
This devcontainer is configured to persist its Gradle, Yarn, nvm, etc. caches on the host (WSL2). That way, re-building the container won't cause overly long build times.
