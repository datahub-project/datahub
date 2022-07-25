# DataHub Quickstart Guide

## Deploying DataHub

To deploy a new instance of DataHub, perform the following steps.

1. Install [docker](https://docs.docker.com/install/), [jq](https://stedolan.github.io/jq/download/) and [docker-compose v1 ](https://github.com/docker/compose/blob/master/INSTALL.md) (if
   using Linux). Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs,
   8GB RAM, 2GB Swap area, and 10GB disk space.

2. Launch the Docker Engine from command line or the desktop app.

3. Install the DataHub CLI

   a. Ensure you have Python 3.6+ installed & configured. (Check using `python3 --version`)

   b. Run the following commands in your terminal

   ```
   python3 -m pip install --upgrade pip wheel setuptools
   python3 -m pip uninstall datahub acryl-datahub || true  # sanity check - ok if it fails
   python3 -m pip install --upgrade acryl-datahub
   datahub version
   ```

:::note

   If you see "command not found", try running cli commands with the prefix 'python3 -m' instead like `python3 -m datahub version`
   Note that DataHub CLI does not support Python 2.x.

:::

4. To deploy a DataHub instance locally, run the following CLI command from your terminal

   ```
   datahub docker quickstart
   ```

   This will deploy a DataHub instance using [docker-compose](https://docs.docker.com/compose/).

   Upon completion of this step, you should be able to navigate to the DataHub UI
   at [http://localhost:9002](http://localhost:9002) in your browser. You can sign in using `datahub` as both the
   username and password.


5. To ingest the sample metadata, run the following CLI command from your terminal

   ```
   datahub docker ingest-sample-data
   ```

:::note

If you've enabled [Metadata Service Authentication](authentication/introducing-metadata-service-authentication.md), you'll need to provide a Personal Access Token
using the `--token <token>` parameter in the command.

:::

That's it! Now feel free to play around with DataHub!

## Troubleshooting Issues

<details><summary>
Command not found: datahub
</summary>

If running the datahub cli produces "command not found" errors inside your terminal, your system may be defaulting to an
older version of Python. Try prefixing your `datahub` commands with `python3 -m`:

```
python3 -m datahub docker quickstart
```

Another possibility is that your system PATH does not include pip's `$HOME/.local/bin` directory.  On linux, you can add this to your `~/.bashrc`:

```
if [ -d "$HOME/.local/bin" ] ; then
    PATH="$HOME/.local/bin:$PATH"
fi
```
</details>

<details>
<summary>
Port Conflicts
</summary>

By default the quickstart deploy will require the following ports to be free on your local machine:
  - 3306 for MySQL
  - 9200 for Elasticsearch
  - 9092 for the Kafka broker
  - 8081 for Schema Registry
  - 2181 for ZooKeeper
  - 9002 for the DataHub Web Application (datahub-frontend)
  - 8080 for the DataHub Metadata Service (datahub-gms)

  In case the default ports conflict with software you are already running on your machine, you can override these ports by passing additional flags to the `datahub docker quickstart` command.
  e.g. To override the MySQL port with 53306 (instead of the default 3306), you can say: `datahub docker quickstart --mysql-port 53306`. Use `datahub docker quickstart --help` to see all the supported options.

</details>

<details>
<summary>
Miscellaneous Docker issues
</summary>
There can be misc issues with Docker, like conflicting containers and dangling volumes, that can often be resolved by
pruning your Docker state with the following command. Note that this command removes all unused containers, networks,
images (both dangling and unreferenced), and optionally, volumes.

```
docker system prune
```

</details>

<details>
<summary>
Still stuck?
</summary>
Hop over to our [Slack community](https://slack.datahubproject.io) and ask for help in the [#troubleshoot](https://datahubspace.slack.com/archives/C029A3M079U) channel!
</details>

## Next Steps

### Ingest Metadata

To start pushing your company's metadata into DataHub, take a look at [UI-based Ingestion Guide](./ui-ingestion.md), or to run ingestion using the cli, look at the [Metadata Ingestion Guide](../metadata-ingestion/README.md).

### Invite Users

To add users to your deployment to share with your team check out our [Adding Users to DataHub](authentication/guides/add-users.md)

### Enable Authentication 

To enable SSO, check out [Configuring OIDC Authentication](authentication/guides/sso/configure-oidc-react.md) or [Configuring JaaS Authentication](authentication/guides/jaas.md). 

To enable backend Authentication, check out [authentication in DataHub's backend](authentication/introducing-metadata-service-authentication.md#Configuring Metadata Service Authentication). 

### Move to Production

We recommend deploying DataHub to production using Kubernetes. We provide helpful [Helm Charts](https://artifacthub.io/packages/helm/datahub/datahub) to help you quickly get up and running. Check out [Deploying DataHub to Kubernetes](./deploy/kubernetes.md) for a step-by-step walkthrough. 

## Other Common Operations

### Stopping DataHub

To stop DataHub's quickstart, you can issue the following command.

```
datahub docker quickstart --stop
```

### Resetting DataHub (a.k.a factory reset)

To cleanse DataHub of all of its state (e.g. before ingesting your own), you can use the CLI `nuke` command.

```
datahub docker nuke
```

### Backing up your DataHub Quickstart (experimental)

The quickstart image is not recommended for use as a production instance. See [Moving to production](#move-to-production) for recommendations on setting up your production cluster. However, in case you want to take a backup of your current quickstart state (e.g. you have a demo to your company coming up and you want to create a copy of the quickstart data so you can restore it at a future date), you can supply the `--backup` flag to quickstart. 
```
datahub docker quickstart --backup
```
will take a backup of your MySQL image and write it by default to your `~/.datahub/quickstart/` directory as the file `backup.sql`. You can customize this by passing a `--backup-file` argument. 
e.g. 
```
datahub docker quickstart --backup --backup-file /home/my_user/datahub_backups/quickstart_backup_2002_22_01.sql
```
:::note

Note that the Quickstart backup does not include any timeseries data (dataset statistics, profiles, etc.), so you will lose that information if you delete all your indexes and restore from this backup. 


### Restoring your DataHub Quickstart (experimental)
As you might imagine, these backups are restore-able. The following section describes a few different options you have to restore your backup.

#### Restoring a backup (primary + index) [most common]
To restore a previous backup, run the following command:
```
datahub docker quickstart --restore
```
This command will pick up the `backup.sql` file located under `~/.datahub/quickstart` and restore your primary database as well as the elasticsearch indexes with it.

To supply a specific backup file, use the `--restore-file` option. 
```
datahub docker quickstart --restore --restore-file /home/my_user/datahub_backups/quickstart_backup_2002_22_01.sql
```

#### Restoring only the index [to deal with index out of sync / corruption issues]
Another situation that can come up is the index can get corrupt, or be missing some update. In order to re-bootstrap the index from the primary store, you can run this command to sync the index with the primary store. 
```
datahub docker quickstart --restore-indices
```

#### Restoring a backup (primary but NO index) [rarely used]
Sometimes, you might want to just restore the state of your primary database (MySQL), but not re-index the data. To do this, you have to explicitly disable the restore-indices capability. 

```
datahub docker quickstart --restore --no-restore-indices
```


### Upgrading your local DataHub

If you have been testing DataHub locally, a new version of DataHub got released and you want to try the new version then you can just issue the quickstart command again. It will pull down newer images and restart your instance without losing any data. 

```
datahub docker quickstart
```

### Customization

If you would like to customize the DataHub installation further, please download the [docker-compose.yaml](https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml) used by the cli tool, modify it as necessary and deploy DataHub by passing the downloaded docker-compose file:
```
datahub docker quickstart --quickstart-compose-file <path to compose file>
```

