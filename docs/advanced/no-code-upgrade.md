# No Code Upgrade (In-Place Migration Guide)

## Summary of changes
With the No Code metadata initiative, we've introduced various major changes:

1. New Ebean Aspect table (metadata_aspect_v2)
2. New Elastic Indices
3. New edge triples. (Remove fully qualified classpath)
4. Dynamic DataPlatform entities (no more hardcoded DataPlatformInfo.json)
5. Dynamic Browse Paths (no more hardcoded browse path creation logic)
6. Addition of Entity Key aspects, dropped requirement for strongly-typed Urns. 
7. Addition of @Entity, @Aspect, @Searchable, @Relationship annotations to existing models. 

Because of these changes, it is required that your persistence layer be migrated after the NoCode containers have been deployed.

For more information on the No Code Update, please see [no-code-modeling](./no-code-modeling.md). 

## Upgrade Steps

### Step 1: Pull & deploy latest container images 

It is important that the following containers are pulled and deployed simultaneously:

- datahub-frontend-react
- datahub-gms
- datahub-mae-consumer
- datahub-mce-consumer

#### Docker-compose

From the `docker` directory: 

```aidl
docker-compose down && docker-compose pull && docker-compose -p datahub up --force-recreate
```

#### Helm

TODO

### Step 2: Execute Migration Job

#### Docker-compose 

The easiest option is to execute the `upgrade.sh` script located under `docker/datahub-upgrade/nocode`. 

```
cd docker/datahub-upgrade/nocode
./upgrade.sh
```

Or equivalently,

```docker-compose -f docker-compose.nocode.yml up```

In both cases, the default environment variables will be used (`docker/datahub-upgrade/nocode/env/docker.env`). These 
will leverage the default datahub docker network. 


#### Docker container (Manual)

You can also build & run a docker container from source to apply the upgrade. 

```
cd docker/datahub-upgrade
docker build -t datahub-upgrade -f Dockerfile ../../
docker run datahub-upgrade --env-file nocode/env/docker.env
```


## Migration strategy

As mentioned in the docs, indices created in Elasticsearch 5.x are not readable by Elasticsearch 7.x. Running the upgraded elasticsearch container on the existing esdata volume will fail.

For local development, our recommendation is to run the `docker/nuke.sh` script to remove the existing esdata volume before starting up the containers. Note, all data will be lost.

To migrate without losing data, please refer to the python script and Dockerfile in `contrib/elasticsearch/es7-upgrade`. The script takes source and destination elasticsearch cluster URL and SSL configuration (if applicable) as input. It ports the mappings and settings for all indices in the source cluster to the destination cluster making the necessary changes stated above. Then it transfers all documents in the source cluster to the destination cluster.

You can run the script in a docker container as follows
```
docker build -t migrate-es-7 .
docker run migrate-es-7 -s SOURCE -d DEST [--disable-source-ssl]
                   [--disable-dest-ssl] [--cert-file CERT_FILE]
                   [--key-file KEY_FILE] [--ca-file CA_FILE] [--create-only]
                   [-i INDICES] [--name-override NAME_OVERRIDE]
```

## Plan

We will create an "elasticsearch-5-legacy" branch with the version of master prior to the elasticsearch 7 upgrade. However, we will not be supporting this branch moving forward and all future development will be done using elasticsearch 7.9.3