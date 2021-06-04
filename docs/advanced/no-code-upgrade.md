# No Code Upgrade (In-Place Migration Guide)

## Summary of changes

With the No Code metadata initiative, we've introduced various major changes:

1. New Ebean Aspect table (metadata_aspect_v2)
2. New Elastic Indices (*entityName*index_v2)
3. New edge triples. (Remove fully qualified classpaths from nodes & edges)
4. Dynamic DataPlatform entities (no more hardcoded DataPlatformInfo.json)
5. Dynamic Browse Paths (no more hardcoded browse path creation logic)
6. Addition of Entity Key aspects, dropped requirement for strongly-typed Urns.
7. Addition of @Entity, @Aspect, @Searchable, @Relationship annotations to existing models.

Because of these changes, it is required that your persistence layer be migrated after the NoCode containers have been
deployed.

For more information about the No Code Update, please see [no-code-modeling](./no-code-modeling.md).

## Migration strategy

We are merging these breaking changes into the main branch upfront because we feel they are fundamental to subsequent
changes, providing a more solid foundation upon which exciting new features will be built upon. We will continue to
offer limited support for previous verions of DataHub.

This approach means that companies who actively deploy the latest version of DataHub will need to perform an upgrade to
continue operating DataHub smoothly.

## Upgrade Steps

### Step 1: Pull & deploy latest container images

It is important that the following containers are pulled and deployed simultaneously:

- datahub-frontend-react
- datahub-gms
- datahub-mae-consumer
- datahub-mce-consumer

#### Docker Compose Deployments

From the `docker` directory:

```aidl
docker-compose down --remove-orphans && docker-compose pull && docker-compose -p datahub up --force-recreate
```

#### Helm

Deploying latest helm charts will upgrade all components to version 0.8.0. Once all the pods are up and running, it will
run the datahub-upgrade job, which will run the above docker container to migrate to the new sources.

### Step 2: Execute Migration Job

#### Docker Compose Deployments

The easiest option is to execute the `run_upgrade.sh` script located under `docker/datahub-upgrade/nocode`.

```
cd docker/datahub-upgrade/nocode
./run_upgrade.sh
```

In both cases, the default environment variables will be used (`docker/datahub-upgrade/env/docker.env`). These assume
that your deployment is local. If this is not the case, you'll need to define your own environment variables to tell the
upgrade system where your DataHub containers reside.

You can either

1. Change `docker/datahub-upgrade/env/docker.env` in place and then run one of the above commands OR
2. Define a new ".env" file containing your variables and
   execute `docker pull acryldata/datahub-upgrade && docker run acryldata/datahub-upgrade:latest -u NoCodeDataMigration`

To see the required environment variables, see the [datahub-upgrade](../../docker/datahub-upgrade/README.md)
documentation.

##### How to fix the "listening to port 5005" issue

Fix for this issue have been published to the acryldata/datahub-upgrade:head tag. Please pull latest master and rerun
the upgrade script.

However, we have seen cases where the problematic docker image is cached and docker does not pull the latest version. If
the script fails with the same error after pulling latest master, please run the following command to clear the docker
image cache.

```
docker images -a | grep acryldata/datahub-upgrade | awk '{print $3}' | xargs docker rmi -f
```

#### Helm Deployments

Upgrade to latest helm charts by running the following after pulling latest master.

```(shell)
helm upgrade datahub datahub/ --values datahub/quickstart-values.yaml
```

This will upgrade all pods to version 0.8.0, and once all pods are up and ready, datahub-upgrade job will start, running
the above docker image to migrate to the new stores.

### Step 3 (Optional): Cleaning Up

This step involves removing data from previous versions of DataHub. This step should only be performed once you've
validated that your DataHub deployment is healthy after performing the upgrade. If you're able to search, browse, and
view your Metadata after the upgrade steps have been completed, you should be in good shape.

In advanced DataHub deployments, or cases in which you cannot easily rebuild the state stored in DataHub, it is strongly
advised that you do due diligence prior to running cleanup. This may involve manually inspecting the relational
tables (metadata_aspect_v2), search indices, and graph topology.

#### Docker Compose Deployments

The easiest option is to execute the `run_clean.sh` script located under `docker/datahub-upgrade/nocode`.

```
cd docker/datahub-upgrade/nocode
./run_clean.sh
```

In both cases, the default environment variables will be used (`docker/datahub-upgrade/env/docker.env`). These assume
that your deployment is local. If this is not the case, you'll need to define your own environment variables to tell the
upgrade system where your DataHub containers reside.

You can either

1. Change `docker/datahub-upgrade/env/docker.env` in place and then run one of the above commands OR
2. Define a new ".env" file containing your variables and execute
   `docker pull acryldata/datahub-upgrade && docker run acryldata/datahub-upgrade:latest -u NoCodeDataMigrationCleanup`

To see the required environment variables, see the (datahub-upgrade)[../../docker/datahub-upgrade/README.md]
documentation

#### Helm Deployments

TODO

## Support

The Acryl team will be on standby to assist you in your migration. Please
join [#release-0_8_0](https://datahubspace.slack.com/archives/C0244FHMHJQ) channel and reach out to us if you find
trouble with the upgrade or have feedback on the process. We will work closely to make sure you can continue to operate
DataHub smoothly.
