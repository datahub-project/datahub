# JaaS Authentication

## Overview

The DataHub frontend server comes with support for plugging in [JaaS](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) modules.
This allows you to use a custom authentication protocol to log your users into DataHub.

By default, we in include sample configuration of a file-based username / password authentication module ([PropertyFileLoginModule](http://archive.eclipse.org/jetty/8.0.0.M3/apidocs/org/eclipse/jetty/plus/jaas/spi/PropertyFileLoginModule.html))
that is configured with a single username / password combination: datahub - datahub.

To change or extend the default behavior, you have multiple options, each dependent on which deployment environment you're operating in.

### Modify user.props file directly (Local Testing)

The first option for customizing file-based users is to modify the file `datahub-frontend/app/conf/user.props` directly.
Once you've added your desired users, you can simply run `./dev.sh` or `./datahub-frontend/run-local-frontend` to validate your
new users can log in.

### Mount a custom user.props file (Docker Compose)

By default, the `datahub-frontend` container will look for a file called `user.props` mounted at the container path
`/datahub-frontend/conf/user.props`. If you wish to launch this container with a custom set of users, you'll need to override the default
file mounting when running using `docker-compose`.

To do so, change the `datahub-frontend-react` service in the docker-compose.yml file containing it to include the custom file:

```
datahub-frontend-react:
    build:
      context: ../
      dockerfile: docker/datahub-frontend/Dockerfile
    image: acryldata/datahub-frontend-react:${DATAHUB_VERSION:-head}
    env_file: datahub-frontend/env/docker.env
    hostname: datahub-frontend-react
    container_name: datahub-frontend-react
    ports:
      - "9002:9002"
    depends_on:
      - datahub-gms
    volumes:
      - ./my-custom-dir/user.props:/datahub-frontend/conf/user.props
```

And then run `docker-compose up` against your compose file.

## Custom JaaS Configuration

In order to change the default JaaS module configuration, you will have to launch the `datahub-frontend-react` container with the custom `jaas.conf` file mounted as a volume
at the location `/datahub-frontend/conf/jaas.conf`.

To do so, change the `datahub-frontend-react` service in the docker-compose.yml file containing it to include the custom file:

```
datahub-frontend-react:
    build:
      context: ../
      dockerfile: docker/datahub-frontend/Dockerfile
    image: acryldata/datahub-frontend-react:${DATAHUB_VERSION:-head}
    env_file: datahub-frontend/env/docker.env
    hostname: datahub-frontend-react
    container_name: datahub-frontend-react
    ports:
      - "9002:9002"
    depends_on:
      - datahub-gms
    volumes:
      - ./my-custom-dir/jaas.conf:/datahub-frontend/conf/jaas.conf
```

And then run `docker-compose up` against your compose file.

## Adding New Users via user.props File

:::note Important
Adding users via the `user.props` will require disabling existence checks on GMS using the `METADATA_SERVICE_AUTH_ENFORCE_EXISTENCE_ENABLED=false` environment variable or using the API to enable the user prior to login.
The directions below demonstrate using the API to enable the user.
:::

To define a set of username / password combinations that should be allowed to log in to DataHub (in addition to the root 'datahub' user),
create a new file called `user.props` at the file path `${HOME}/.datahub/plugins/frontend/auth/user.props` within the `datahub-frontend-react` container
or pod.

This file should contain username:password specifications, with one on each line. For example, to create 2 new users,
with usernames "janesmith" and "johndoe", we would define the following file:

```
// custom user.props
janesmith:janespassword
johndoe:johnspassword
```

### Enabling Users via API

In order to enable the user access with the credential defined in `user.props`, set the `status` aspect on the user with an Admin user. This can be done using an API call or via the [OpenAPI UI interface](/docs/api/openapi/openapi-usage-guide.md).

Example enabling login for the `janesmith` user from the example above. Make sure to update the example with your access token.

```shell
curl -X 'POST' \
  'http://localhost:9002/openapi/v3/entity/corpuser/urn%3Ali%3Acorpuser%3Ajanesmith/status?async=false&systemMetadata=false&createIfEntityNotExists=false&createIfNotExists=true' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <access token>' \
  -d '{
  "value": {
    "removed": false
  }
}'
```

Once you've saved the file, simply start the DataHub containers & navigate to `http://localhost:9002/login`
to verify that your new credentials work.

To change or remove existing login credentials, edit and save the `user.props` file. Then restart DataHub containers.

### User Details and Search

If you add a new username / password to the `user.props` file, no other information about the user will exist
about the user in DataHub (full name, email, bio, etc). This means that you will not be able to search to find the user.

In order for the user to become searchable, simply navigate to the new user's profile page (top-right corner) and click
**Edit Profile**. Add some details like a display name, an email, and more. Then click **Save**. Now you should be able
to find the user via search.

> You can also use our Python Emitter SDK to produce custom information about the new user via the CorpUser metadata entity.

### User URNs

User URNs are unique identifiers for users of DataHub. The usernames defined in the `user.props` file will be used to generate the DataHub user "urn", which uniquely identifies
the user on DataHub. The urn is computed as `urn:li:corpuser:{username}`, where "username" is defined inside your user.props file.

## Advanced Configuration

### Custom user.props File Location

If you want to customize the location of the `user.props` file, or if you're deploying DataHub via Helm, you'll need to mount the custom file to the container.
