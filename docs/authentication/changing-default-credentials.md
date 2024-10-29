# Changing the default user credentials

## Default User Credential

The 'datahub' root user is created for you by default. This user is controlled via a [user.props](https://github.com/datahub-project/datahub/blob/master/datahub-frontend/conf/user.props) file which [JaaS Authentication](./guides/jaas.md) is configured to use:

By default, the credential file looks like this for each and every self-hosted DataHub deployment:

```
// default user.props
datahub:datahub
```

Obviously, this is not ideal from a security perspective. It is highly recommended that this file
is changed _prior_ to deploying DataHub to production at your organization.

:::warning
Please note that deleting the `Data Hub` user in the UI **WILL NOT** disable the default user.
You will still be able to log in using the default 'datahub:datahub' credentials.
To safely delete the default credentials, please follow the guide provided below.

:::

## Changing the default user `datahub`

The method for changing the default user depends on how DataHub is deployed.

- [Helm chart](#helm-chart)
  - [Deployment Guide](/docs/deploy/kubernetes.md)
- [Docker-compose](#docker-compose)
  - [Deployment Guide](../../docker/README.md)
- [Quickstart](#quickstart)
  - [Deployment Guide](/docs/quickstart.md)

### Helm chart

You'll need to create a Kubernetes secret, then mount the file as a volume to the datahub-frontend pod.

#### 1. Create a new config file

Create a new version [user.props](https://github.com/datahub-project/datahub/blob/master/datahub-frontend/conf/user.props) which defines the updated password for the datahub user.

To remove the user 'datahub' from the new file, simply omit the username. Please note that you can also choose to leave the file empty.
For example, to change the password for the DataHub root user to 'newpassword', your file would contain the following:

```
// new user.props
datahub:newpassword
```

#### 2. Create a kubernetes secret

Create a secret from your local `user.props` file.

```shell
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props>
```

#### 3. Mount the config file

Configure your [values.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml#LL22C1-L22C1) to add the volume to the datahub-frontend container.

```yaml
datahub-frontend:
  ...
  extraVolumes:
    - name: datahub-users
      secret:
        defaultMode: 0444
        secretName:  datahub-users-secret
  extraVolumeMounts:
    - name: datahub-users
      mountPath: /datahub-frontend/conf/user.props
      subPath: user.props
```

#### 4. Restart Datahub

Restart the DataHub containers or pods to pick up the new configs.
For example, you could run the following command to upgrade the current helm deployment.

```shell
helm upgrade datahub datahub/datahub --values <path_to_values.yaml>
```

Note that if you update the secret you will need to restart the datahub-frontend pods so the changes are reflected. To update the secret in-place you can run something like this.

```
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props> -o yaml --dry-run=client | kubectl apply -f -
```

### Docker-compose

#### 1. Modify a config file

Modify [user.props](https://github.com/datahub-project/datahub/blob/master/datahub-frontend/conf/user.props) which defines the updated password for the datahub user.

To remove the user 'datahub' from the new file, simply omit the username. Please note that you can also choose to leave the file empty.
For example, to change the password for the DataHub root user to 'newpassword', your file would contain the following:

```
// new user.props
datahub:newpassword
```

#### 2. Mount the updated config file

Change the [docker-compose.yaml](https://github.com/datahub-project/datahub/blob/master/docker/docker-compose.yml) to mount an updated user.props file to the following location inside the `datahub-frontend-react` container using a volume:`/datahub-frontend/conf/user.props`

```yaml
  datahub-frontend-react:
  ...
    volumes:
    ...
    - <absolute_path_to_your_custom_user_props_file>:/datahub-frontend/conf/user.props
```

#### 3. Restart DataHub

Restart the DataHub containers or pods to pick up the new configs.

### Quickstart

#### 1. Modify a config file

Modify [user.props](https://github.com/datahub-project/datahub/blob/master/datahub-frontend/conf/user.props) which defines the updated password for the datahub user.

To remove the user 'datahub' from the new file, simply omit the username. Please note that you can also choose to leave the file empty.
For example, to change the password for the DataHub root user to 'newpassword', your file would contain the following:

```
// new user.props
datahub:newpassword
```

#### 2. Mount the updated config file

In [docker-compose file used in quickstart](https://github.com/datahub-project/datahub/blob/master/docker/quickstart/docker-compose.quickstart.yml).
Modify the [datahub-frontend-react block](https://github.com/datahub-project/datahub/blob/master/docker/quickstart/docker-compose.quickstart.yml#L116) to contain the extra volume mount.

```yaml
  datahub-frontend-react:
  ...
    volumes:
    ...
    - <absolute_path_to_your_custom_user_props_file>:/datahub-frontend/conf/user.props
```

#### 3. Restart Datahub

Run the following command.

```
datahub docker quickstart --quickstart-compose-file <your-modified-compose>.yml
```
