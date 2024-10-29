import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DataHub Quickstart Guide

:::tip DataHub Cloud

This guide provides instructions on deploying the open source DataHub locally.
If you're interested in a managed version, [Acryl Data](https://www.acryldata.io) provides a fully managed, premium version of DataHub. <br />
**[Get Started with DataHub Cloud](./managed-datahub/welcome-acryl.md)**

:::

## Prerequisites

- Install **Docker** and **Docker Compose** v2 for your platform.

  | Platform | Application                                                                                                                                     |
  | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
  | Window   | [Docker Desktop](https://www.docker.com/products/docker-desktop/)                                                                               |
  | Mac      | [Docker Desktop](https://www.docker.com/products/docker-desktop/)                                                                               |
  | Linux    | [Docker for Linux](https://docs.docker.com/desktop/install/linux-install/) and [Docker Compose](https://docs.docker.com/compose/install/linux/) |

- **Launch the Docker engine** from command line or the desktop app.
- Ensure you have **Python 3.8+** installed & configured. (Check using `python3 --version`).

:::note Docker Resource Allocation

Make sure to allocate enough hardware resources for Docker engine. <br />
Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap area, and 10GB disk space.

:::

## Install the DataHub CLI

<Tabs>
<TabItem value="pip" label="pip">

```bash
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub version
```

:::note Command Not Found

If you see `command not found`, try running cli commands like `python3 -m datahub version`. <br />
Note that DataHub CLI does not support Python 2.x.

:::

</TabItem>
<TabItem value="poetry" label="poetry">

```bash
poetry add acryl-datahub
poetry shell
datahub version
```

</TabItem>
</Tabs>

## Start DataHub

Run the following CLI command from your terminal.

```bash
datahub docker quickstart
```

This will deploy a DataHub instance using [docker-compose](https://docs.docker.com/compose/).
If you are curious, the `docker-compose.yaml` file is downloaded to your home directory under the `.datahub/quickstart` directory.

If things go well, you should see messages like the ones below:

```shell-session
Fetching docker-compose file https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml from GitHub
Pulling docker images...
Finished pulling docker images!

[+] Running 11/11
⠿ Container zookeeper                  Running                                                                                                                                                         0.0s
⠿ Container elasticsearch              Running                                                                                                                                                         0.0s
⠿ Container broker                     Running                                                                                                                                                         0.0s
⠿ Container schema-registry            Running                                                                                                                                                         0.0s
⠿ Container elasticsearch-setup        Started                                                                                                                                                         0.7s
⠿ Container kafka-setup                Started                                                                                                                                                         0.7s
⠿ Container mysql                      Running                                                                                                                                                         0.0s
⠿ Container datahub-gms                Running                                                                                                                                                         0.0s
⠿ Container mysql-setup                Started                                                                                                                                                         0.7s
⠿ Container datahub-datahub-actions-1  Running                                                                                                                                                         0.0s
⠿ Container datahub-frontend-react     Running                                                                                                                                                         0.0s
.......
✔ DataHub is now running
Ingest some demo data using `datahub docker ingest-sample-data`,
or head to http://localhost:9002 (username: datahub, password: datahub) to play around with the frontend.
Need support? Get in touch on Slack: https://slack.datahubproject.io/
```

:::note Mac M1/M2

On Mac computers with Apple Silicon (M1, M2 etc.), you might see an error like `no matching manifest for linux/arm64/v8 in the manifest list entries`.
This typically means that the datahub cli was not able to detect that you are running it on Apple Silicon.
To resolve this issue, override the default architecture detection by issuing `datahub docker quickstart --arch m1`

:::

### Sign In

Upon completion of this step, you should be able to navigate to the DataHub UI at [http://localhost:9002](http://localhost:9002) in your browser.
You can sign in using the default credentials below.

```json
username: datahub
password: datahub
```

To change the default credentials, please refer to [Change the default user datahub in quickstart](authentication/changing-default-credentials.md#quickstart).

### Ingest Sample Data

To ingest the sample metadata, run the following CLI command from your terminal

```bash
datahub docker ingest-sample-data
```

:::note Token Authentication

If you've enabled [Metadata Service Authentication](authentication/introducing-metadata-service-authentication.md), you'll need to provide a Personal Access Token
using the `--token <token>` parameter in the command.

:::

That's it! Now feel free to play around with DataHub!

---

## Common Operations

### Stop DataHub

To stop DataHub's quickstart, you can issue the following command.

```bash
datahub docker quickstart --stop
```

### Reset DataHub

To cleanse DataHub of all of its state (e.g. before ingesting your own), you can use the CLI `nuke` command.

```bash
datahub docker nuke
```

### Upgrade DataHub

If you have been testing DataHub locally, a new version of DataHub got released and you want to try the new version then you can just issue the quickstart command again. It will pull down newer images and restart your instance without losing any data.

```bash
datahub docker quickstart
```

### Customize installation

If you would like to customize the DataHub installation further, please download the [docker-compose.yaml](https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml) used by the cli tool, modify it as necessary and deploy DataHub by passing the downloaded docker-compose file:

```bash
datahub docker quickstart --quickstart-compose-file <path to compose file>
```

### Back up DataHub

The quickstart image is not recommended for use as a production instance. <br />
However, in case you want to take a backup of your current quickstart state (e.g. you have a demo to your company coming up and you want to create a copy of the quickstart data so you can restore it at a future date), you can supply the `--backup` flag to quickstart.

<Tabs>
<TabItem value="backup" label="Back up (default)">

```bash
datahub docker quickstart --backup
```

This will take a backup of your MySQL image and write it by default to your `~/.datahub/quickstart/` directory as the file `backup.sql`.

</TabItem>
<TabItem value="backup custom" label="Back up to custom directory">

```bash
datahub docker quickstart --backup --backup-file <path to backup file>
```

You can customize the backup file path by passing a `--backup-file` argument.

</TabItem>
</Tabs>

:::caution

Note that the Quickstart backup does not include any timeseries data (dataset statistics, profiles, etc.), so you will lose that information if you delete all your indexes and restore from this backup.

:::

### Restore DataHub

As you might imagine, these backups are restore-able. The following section describes a few different options you have to restore your backup.

<Tabs>
<TabItem value="General" label="General Restoring">

To restore a previous backup, run the following command:

```bash
datahub docker quickstart --restore
```

This command will pick up the `backup.sql` file located under `~/.datahub/quickstart` and restore your primary database as well as the elasticsearch indexes with it.

To supply a specific backup file, use the `--restore-file` option.

```bash
datahub docker quickstart --restore --restore-file /home/my_user/datahub_backups/quickstart_backup_2002_22_01.sql
```

</TabItem>
<TabItem value="Restoring Only Index" label="Restore Only Index">

Another situation that can come up is the index can get corrupt, or be missing some update. In order to re-bootstrap the index from the primary store, you can run this command to sync the index with the primary store.

```bash
datahub docker quickstart --restore-indices
```

</TabItem>

<TabItem value="Restoring Only Primary" label="Restore Only Primary">

Sometimes, you might want to just restore the state of your primary database (MySQL), but not re-index the data. To do this, you have to explicitly disable the restore-indices capability.

```bash
datahub docker quickstart --restore --no-restore-indices
```

</TabItem>
</Tabs>

---

## Next Steps

- [Quickstart Debugging Guide](./troubleshooting/quickstart.md)
- [Ingest metadata through the UI](./ui-ingestion.md)
- [Ingest metadata through the CLI](../metadata-ingestion/README.md)
- [Add Users to DataHub](authentication/guides/add-users.md)
- [Configure OIDC Authentication](authentication/guides/sso/configure-oidc-react.md)
- [Configure JaaS Authentication](authentication/guides/jaas.md)
- [Configure authentication in DataHub's backend](authentication/introducing-metadata-service-authentication.md#configuring-metadata-service-authentication).
- [Change the default user datahub in quickstart](authentication/changing-default-credentials.md#quickstart)

### Move To Production

:::caution

Quickstart is not intended for a production environment. We recommend deploying DataHub to production using Kubernetes.
We provide helpful [Helm Charts](https://artifacthub.io/packages/helm/datahub/datahub) to help you quickly get up and running.
Check out [Deploying DataHub to Kubernetes](./deploy/kubernetes.md) for a step-by-step walkthrough.

:::

The `quickstart` method of running DataHub is intended for local development and a quick way to experience the features that DataHub has to offer.
It is not intended for a production environment. This recommendation is based on the following points.

#### Default Credentials

`quickstart` uses docker compose configuration which includes default credentials for both DataHub, and it's underlying
prerequisite data stores, such as MySQL. Additionally, other components are unauthenticated out of the box. This is a
design choice to make development easier and is not best practice for a production environment.

#### Exposed Ports

DataHub's services, and it's backend data stores use the docker default behavior of binding to all interface addresses.
This makes it useful for development but is not recommended in a production environment.

#### Performance & Management

`quickstart` is limited by the resources available on a single host, there is no ability to scale horizontally.
Rollout of new versions often requires downtime and the configuration is largely pre-determined and not easily managed.
Lastly, by default, `quickstart` follows the most recent builds forcing updates to the latest released and unreleased builds.
