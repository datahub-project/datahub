# Adding Users to DataHub

Users can log into DataHub in 2 ways:

1. Static credentials (Simplest)
2. Single Sign-On via [OpenID Connect](https://www.google.com/search?q=openid+connect&oq=openid+connect&aqs=chrome.0.0i131i433i512j0i512l4j69i60l2j69i61.1468j0j7&sourceid=chrome&ie=UTF-8) (For Production Use)

Option 1 is useful for running proof-of-concept exercises, or just getting DataHub up & running quickly. Option 2 is highly recommended for deploying DataHub in production.


# Configuring static credentials

## Create a user.props file

To define a set of username / password combinations that should be allowed to log in to DataHub, create a new file called `user.props` at the file path `${HOME}/.datahub/plugins/auth/user.props`. 
This file should contain username:password combinations, with 1 user per line. For example, to create 2 new users,
with usernames "janesmith" and "johndoe", we would define the following file:

```
janesmith:janespassword
johndoe:johnspassword
```

Once you've saved the file, simply start the DataHub containers & navigate to `http://localhost:9002/login`
to verify that your new credentials work.

To change or remove existing login credentials, edit and save the `user.props` file. Then restart DataHub containers. 

If you want to customize the location of the `user.props` file, or if you're deploying DataHub via Helm, proceed to Step 2.

## (Advanced) Mount custom user.props file to container

This step is only required when mounting custom credentials into a Kubernetes pod (e.g. Helm) **or** if you want to change
the default filesystem location where DataHub checks for a custom `user.props` file (`${HOME}/.datahub/plugins/auth/user.props)`. 

If you are deploying with `datahub docker quickstart`, or running using Docker Compose, you can most likely skip this step.

### Docker Compose

You'll need to modify the `docker-compose.yml` file to mount a container volume mapping your custom user.props to the standard location inside the container 
(`/etc/datahub/plugins/auth/user.props`).

For example, to mount a user.props file that is stored on my local filesystem at `/tmp/datahub/user.props`, we'd modify the YAML for the 
`datahub-web-react` config to look like the following:

```aidl
  datahub-frontend-react:
    build:
      context: ../
      dockerfile: docker/datahub-frontend/Dockerfile
    image: linkedin/datahub-frontend-react:${DATAHUB_VERSION:-head}
    .....
    # The new stuff
    volumes:
      - ${HOME}/.datahub/plugins:/etc/datahub/plugins
      - /tmp/datahub:/etc/datahub/plugins/auth
```

Once you've made this change, restarting DataHub enable authentication for the configured users.

### Helm

You'll need to create a Kubernetes secret, then mount the file as a volume to the `datahub-frontend` pod. 

First, create a secret from your local `user.props` file

```shell
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props>
```

Then, configure your `values.yaml` to add the volume to the `datahub-frontend` container.

```YAML
datahub-frontend:
  ...
  extraVolumes:
    - name: datahub-users
      secret:
        defaultMode: 0444
        secretName:  datahub-users-secret
  extraVolumeMounts:
    - name: datahub-users
      mountPath: /etc/datahub/plugins/auth/user.props
      subPath: user.props
```

Note that if you update the secret you will need to restart the `datahub-frontend` pods so the changes are reflected. To update the secret in-place you can run something like this.

```shell
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props> -o yaml --dry-run=client | kubectl apply -f -
```

## URNs

URNs are identifiers that uniquely identify an Entity on DataHub. The usernames defined in the `user.props` file will be used to generate the DataHub user "urn", which uniquely identifies
the user on DataHub. The urn is computed as:

```
urn:li:corpuser:{username}
```

## Caveats

If you add a new username / password to the `user.props` file, no other information about the user will exist
about the user in DataHub (full name, email, bio, etc). This means that you will not be able to search to find the user.

In order to add information about the user in DataHub, you can use our Python Emitter SDK to produce aspects for the CorpUser,
where the URN will be computed as `urn:li:corpuser:{username}`, where `username` is the identifier defined in the user.props file.

For a more comprehensive overview of how users & groups are managed within DataHub, check out [this video](https://www.youtube.com/watch?v=8Osw6p9vDYY).


# Configuring SSO via OpenID Connect

Setting up SSO via OpenID Connect means that users will be able to login to DataHub via a central Identity Provider such as

- Azure AD
- Okta 
- Keycloak
- Ping!
- Google Identity

and more. 

This option is recommended for production deployments of DataHub. For detailed information about configuring DataHub to use OIDC to
perform authentication, check out [OIDC Authentication](./sso/configure-oidc-react.md). 

## URNs

URNs are identifiers that uniquely identify an Entity on DataHub. The username received from an Identity Provider 
when a user logs into DataHub via OIDC is used to construct a unique identifier for the user on DataHub. The urn is computed as:

```
urn:li:corpuser:<extracted-username>
```

For information about configuring which OIDC claim should be used as the username for Datahub, check out the [OIDC Authentication](./sso/configure-oidc-react.md) doc.


## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!