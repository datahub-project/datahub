# OIDC Proxy Configuration

_Authored on 22/08/2023_

The `datahub-frontend-react` server can be configured to use an http proxy when retrieving the openid-configuration.
This can be needed if your infrastructure is locked down and disallows connectivity by default, using proxies for fine-grained egress control.

## Configure http proxy and non proxy hosts

To do this, you will need to pass a set of environment variables to the datahub-frontend-react container (e.g. in the `docker-compose.yml` file or your kubernetes manifest).

```
HTTP_PROXY_HOST=host of your http proxy
HTTP_PROXY_PORT=port of your http proxy
HTTPS_PROXY_HOST=host of your http(s) proxy used for https connections (often the same as the http proxy)
HTTPS_PROXY_PORT=port of your http(s) proxy used for https connections (often the same as the http proxy)
HTTP_NON_PROXY_HOSTS=localhost|datahub-gms (or any other hosts that you would like to bypass the proxy for, delimited by pipe)
```

## Optional: provide custom truststore

If your upstream proxy performs SSL termination to inspect traffic, this will result in different (self-signed) certificates for HTTPS connections.
The default truststore used in the `datahub-frontend-react` docker image will not trust these kinds of connections.
To address this, you can copy or mount your own truststore (provided by the proxy or network administrators) into the docker container.

Depending on your setup, you have a few options to achieve this:

### Make truststore available in the frontend

#### Option a) Build frontend docker image with your own truststore included

To build a custom image for your frontend, with the certificates built-in, you can use the official frontend image as a base, then copy in your required files.

Example Dockerfile:

```dockerfile
FROM acryldata/datahub-frontend-react:<version>
COPY /truststore-directory /certificates
```

Building this Dockerfile will result in your own custom docker image on your local machine.
You will then be able to tag it, publish it to your own registry, etc.

#### Option b) Mount truststore from your host machine using a docker volume

Adapt your docker-compose.yml to include a new volume mount in the `datahub-frontend-react` container

```docker
  datahub-frontend-react:
    # ...
    volumes:
      # ...
      - /truststore-directory:/certificates
```

### Reference new truststore

Add the following environment values to the `datahub-frontend-react` container:

```
SSL_TRUSTSTORE_FILE=path/to/truststore.jks (e.g. /certificates)
SSL_TRUSTSTORE_TYPE=jks
SSL_TRUSTSTORE_PASSWORD=MyTruststorePassword
```

Once these steps are done, your frontend container will use the new truststore when validating SSL/HTTPS connections.
