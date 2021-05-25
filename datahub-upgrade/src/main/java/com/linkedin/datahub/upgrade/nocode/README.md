# No Code Data Migration Upgrade

This upgrade performs the following steps:

0. Determines whether client is eligible for the upgrade. Requirements: 
        - Legacy (v1) SQL table exists AND
        - New (v2) SQL table does not exist OR has 0 rows
   If conditions are not met, the upgrade aborts. 
1. Create the new v2 SQL table if it does not exist
2. Ingest previously-hardcoded DataPlatformInfo aspects into the new table. 
3. Reads from the v1 source of truth SQL table in batches
4. Performs the necessary reverse mapping from aspect classname -> new aspect common name
5. Writes batches back into a new v2 SQL table
6. Forces emission of MAE when writing aspects where version = latest to ensure indices are rebuilt properly 


# Running the script

## Upgrade via Docker Compose

We've packaged the DataHub Upgrade CLI into a docker image that can be easily used to apply upgrades via a docker-compose wrapper. 

This option works well if you have a locally deployed version of DataHub that has the default configurations. It will involve
running a container with a set of default assumptions about where GMS, MAE, and your data stores are located. These assumptions
are reflected in the file `docker/datahub-upgrade/no-code/docker.env`.

Moreover, we've included a Docker compose script to pull + execute the image specifically to apply this upgrade. It is 
located within `docker/docker-compose.upgrade.nocode.yml`. To execute the upgrade, simply run

```aidl
docker-compose -f docker-compose.upgrade.nocode.yml up
```

from within the "docker" folder. 

If your containers are running in non-default locations, feel free to change configurations set in the .env file and 
run the docker command with the newly specified variables. 

## Upgrade from Source

Instead of using the docker-compose wrapper, you may also pull and run the `datahub-upgrade` image manually. 
In doing so, you'll be required to provide configurations to tell the script where it can find your data stores, GMS, and MAE
consumer containers. 

```docker build -t datahub-upgrade-no-code .
docker run datahub-upgrade-no-code
```

to run with the default parameters. 

To change the batch size or batch migrate delay, use the following

```docker build -t datahub-upgrade-no-code .
docker run datahub-upgrade-no-code -a batchSize=<your-batch-size-rows> batchDelayMs=<your-custom-delay-ms>
```

To adjust more advanced configurations, like where GMS, MAE consumer, and your data stores are deployed, simply
update or add new environment variables. The defaults are located in `docker/datahub-upgrade/no-code/docker.env`. 

## Upgrade on Helm 

!TODO!

