# Quickstart Debugging Guide

For when [Quickstart](/docs/quickstart.md) did not work out smoothly.

## Common Problems

<details><summary>
Command not found: datahub
</summary>

If running the datahub cli produces "command not found" errors inside your terminal, your system may be defaulting to an
older version of Python. Try prefixing your `datahub` commands with `python3 -m`:

```bash
python3 -m datahub docker quickstart
```

Another possibility is that your system PATH does not include pip's `$HOME/.local/bin` directory. On linux, you can add this to your `~/.bashrc`:

```bash
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
For the metadata service container (datahub-gms), you need to use an environment variable, `DATAHUB_MAPPED_GMS_PORT`. So for instance to use the port 58080, you would say `DATAHUB_MAPPED_GMS_PORT=58080 datahub docker quickstart`

</details>

<details>
<summary>
no matching manifest for linux/arm64/v8 in the manifest list entries
</summary>
On Mac computers with Apple Silicon (M1, M2 etc.), you might see an error like `no matching manifest for linux/arm64/v8 in the manifest list entries`, this typically means that the datahub cli was not able to detect that you are running it on Apple Silicon. To resolve this issue, override the default architecture detection by issuing `datahub docker quickstart --arch m1`

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

## How can I confirm if all Docker containers are running as expected after a quickstart?

If you set up the `datahub` CLI tool (see [here](../../metadata-ingestion/README.md)), you can use the built-in check utility:

```shell
datahub docker check
```

You can list all Docker containers in your local by running `docker container ls`. You should expect to see a log similar to the below:

```
CONTAINER ID        IMAGE                                                 COMMAND                  CREATED             STATUS              PORTS                                                      NAMES
979830a342ce        acryldata/datahub-mce-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mce-consumer
3abfc72e205d        acryldata/datahub-frontend-react:latest              "datahub-frontend…"   10 hours ago        Up 10 hours         0.0.0.0:9002->9002/tcp                                     datahub-frontend
50b2308a8efd        acryldata/datahub-mae-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mae-consumer
4d6b03d77113        acryldata/datahub-gms:latest                         "bash -c 'dockerize …"   10 hours ago        Up 10 hours         0.0.0.0:8080->8080/tcp                                     datahub-gms
c267c287a235        landoop/schema-registry-ui:latest                     "/run.sh"                10 hours ago        Up 10 hours         0.0.0.0:8000->8000/tcp                                     schema-registry-ui
4b38899cc29a        confluentinc/cp-schema-registry:5.2.1                 "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:8081->8081/tcp                                     schema-registry
37c29781a263        confluentinc/cp-kafka:5.2.1                           "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp           broker
15440d99a510        docker.elastic.co/kibana/kibana:5.6.8                 "/bin/bash /usr/loca…"   10 hours ago        Up 10 hours         0.0.0.0:5601->5601/tcp                                     kibana
943e60f9b4d0        neo4j:4.0.6                                           "/sbin/tini -g -- /d…"   10 hours ago        Up 10 hours         0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   neo4j
6d79b6f02735        confluentinc/cp-zookeeper:5.2.1                       "/etc/confluent/dock…"   10 hours ago        Up 10 hours         2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                 zookeeper
491d9f2b2e9e        docker.elastic.co/elasticsearch/elasticsearch:5.6.8   "/bin/bash bin/es-do…"   10 hours ago        Up 10 hours         0.0.0.0:9200->9200/tcp, 9300/tcp                           elasticsearch
ce14b9758eb3        mysql:8.2
```

Also you can check individual Docker container logs by running `docker logs <<container_name>>`. For `datahub-gms`, you should see a log similar to this at the end of the initialization:

```
2020-02-06 09:20:54.870:INFO:oejs.Server:main: Started @18807ms
```

For `datahub-frontend-react`, you should see a log similar to this at the end of the initialization:

```
09:20:22 [main] INFO  play.core.server.AkkaHttpServer - Listening for HTTP on /0.0.0.0:9002
```

## My elasticsearch or broker container exited with error or was stuck forever

If you're seeing errors like below, chances are you didn't give enough resource to docker. Please make sure to allocate at least 8GB of RAM + 2GB swap space.

```
datahub-gms             | 2020/04/03 14:34:26 Problem with request: Get http://elasticsearch:9200: dial tcp 172.19.0.5:9200: connect: connection refused. Sleeping 1s
broker                  | [2020-04-03 14:34:42,398] INFO Client session timed out, have not heard from server in 6874ms for sessionid 0x10000023fa60002, closing socket connection and attempting reconnect (org.apache.zookeeper.ClientCnxn)
schema-registry         | [2020-04-03 14:34:48,518] WARN Client session timed out, have not heard from server in 20459ms for sessionid 0x10000023fa60007 (org.apache.zookeeper.ClientCnxn)
```

## How can I check if [MXE](../what/mxe.md) Kafka topics are created?

You can use a utility like [kafkacat](https://github.com/edenhill/kafkacat) to list all topics.
You can run below command to see the Kafka topics created in your Kafka broker.

```bash
kafkacat -L -b localhost:9092
```

Confirm that `MetadataChangeEvent`, `MetadataAuditEvent`, `MetadataChangeProposal_v1` and `MetadataChangeLog_v1` topics exist besides the default ones.

## How can I check if search indices are created in Elasticsearch?

You can run below command to see the search indices created in your Elasticsearch.

```bash
curl http://localhost:9200/_cat/indices
```

Confirm that `datasetindex_v2` & `corpuserindex_v2` indices exist besides the default ones. Example response as below:

```bash
yellow open dataset_datasetprofileaspect_v1         HnfYZgyvS9uPebEQDjA1jg 1 1   0  0   208b   208b
yellow open datajobindex_v2                         A561PfNsSFmSg1SiR0Y0qQ 1 1   2  9 34.1kb 34.1kb
yellow open mlmodelindex_v2                         WRJpdj2zT4ePLSAuEvFlyQ 1 1   1 12 24.2kb 24.2kb
yellow open dataflowindex_v2                        FusYIc1VQE-5NaF12uS8dA 1 1   1  3 23.3kb 23.3kb
yellow open mlmodelgroupindex_v2                    QOzAaVx7RJ2ovt-eC0hg1w 1 1   0  0   208b   208b
yellow open datahubpolicyindex_v2                   luXfXRlSRoS2-S_tvfLjHA 1 1   0  0   208b   208b
yellow open corpuserindex_v2                        gbNXtnIJTzqh3vHSZS0Fwg 1 1   2  2 18.4kb 18.4kb
yellow open dataprocessindex_v2                     9fL_4iCNTLyFv8MkDc6nIg 1 1   0  0   208b   208b
yellow open chartindex_v2                           wYKlG5ylQe2dVKHOaswTww 1 1   2  7 29.4kb 29.4kb
yellow open tagindex_v2                             GBQSZEvuRy62kpnh2cu1-w 1 1   2  2 19.7kb 19.7kb
yellow open mlmodeldeploymentindex_v2               UWA2ltxrSDyev7Tmu5OLmQ 1 1   0  0   208b   208b
yellow open dashboardindex_v2                       lUjGAVkRRbuwz2NOvMWfMg 1 1   1  0  9.4kb  9.4kb
yellow open .ds-datahub_usage_event-000001          Q6NZEv1UQ4asNHYRywxy3A 1 1  36  0 54.8kb 54.8kb
yellow open datasetindex_v2                         bWE3mN7IRy2Uj0QzeCt1KQ 1 1   7 47 93.7kb 93.7kb
yellow open mlfeatureindex_v2                       fvjML5xoQpy8oxPIwltm8A 1 1  20 39 59.3kb 59.3kb
yellow open dataplatformindex_v2                    GihumZfvRo27vt9yRpoE_w 1 1   0  0   208b   208b
yellow open glossarynodeindex_v2                    ABKeekWTQ2urPWfGDsS4NQ 1 1   1  1 18.1kb 18.1kb
yellow open graph_service_v1                        k6q7xV8OTIaRIkCjrzdufA 1 1 116 25 77.1kb 77.1kb
yellow open system_metadata_service_v1              9-FKAqp7TY2hs3RQuAtVMw 1 1 303  0 55.9kb 55.9kb
yellow open schemafieldindex_v2                     Mi_lqA-yQnKWSleKEXSWeg 1 1   0  0   208b   208b
yellow open mlfeaturetableindex_v2                  pk98zrSOQhGr5gPYUQwvvQ 1 1   5 14 36.4kb 36.4kb
yellow open glossarytermindex_v2                    NIyi3WWiT0SZr8PtECo0xQ 1 1   3  8 23.1kb 23.1kb
yellow open mlprimarykeyindex_v2                    R1WFxD9sQiapIZcXnDtqMA 1 1   7  6 35.5kb 35.5kb
yellow open corpgroupindex_v2                       AYxVtFAEQ02BsJdahYYvlA 1 1   2  1 13.3kb 13.3kb
yellow open dataset_datasetusagestatisticsaspect_v1 WqPpDCKZRLaMIcYAAkS_1Q 1 1   0  0   208b   208b
```

## How can I check if data has been loaded into MySQL properly?

Once the mysql container is up and running, you should be able to connect to it directly on `localhost:3306` using tools such as [MySQL Workbench](https://www.mysql.com/products/workbench/). You can also run the following command to invoke [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) inside the mysql container.

```
docker exec -it mysql /usr/bin/mysql datahub --user=datahub --password=datahub
```

Inspect the content of `metadata_aspect_v2` table, which contains the ingested aspects for all entities.

## Getting error while starting Docker containers

There can be different reasons why a container fails during initialization. Below are the most common reasons:

### `bind: address already in use`

This error means that the network port (which is supposed to be used by the failed container) is already in use on your system. You need to find and kill the process which is using this specific port before starting the corresponding Docker container. If you don't want to kill the process which is using that port, another option is to change the port number for the Docker container. You need to find and change the [ports](https://docs.docker.com/compose/compose-file/#ports) parameter for the specific Docker container in the `docker-compose.yml` configuration file.

```
Example : On macOS

ERROR: for mysql  Cannot start service mysql: driver failed programming external connectivity on endpoint mysql (5abc99513affe527299514cea433503c6ead9e2423eeb09f127f87e2045db2ca): Error starting userland proxy: listen tcp 0.0.0.0:3306: bind: address already in use

   1) sudo lsof -i :3306
   2) kill -15 <PID found in step1>
```

### `OCI runtime create failed`

If you see an error message like below, please make sure to git update your local repo to HEAD.

```
ERROR: for datahub-mae-consumer  Cannot start service datahub-mae-consumer: OCI runtime create failed: container_linux.go:349: starting container process caused "exec: \"bash\": executable file not found in $PATH": unknown
```

### `failed to register layer: devmapper: Unknown device`

This most means that you're out of disk space (see [#1879](https://github.com/datahub-project/datahub/issues/1879)).

### `ERROR: for kafka-rest-proxy  Get https://registry-1.docker.io/v2/confluentinc/cp-kafka-rest/manifests/5.4.0: EOF`

This is most likely a transient issue with [Docker Registry](https://docs.docker.com/registry/). Retry again later.

## toomanyrequests: too many failed login attempts for username or IP address

Try the following

```bash
rm ~/.docker/config.json
docker login
```

More discussions on the same issue https://github.com/docker/hub-feedback/issues/1250

## Seeing `Table 'datahub.metadata_aspect' doesn't exist` error when logging in

This means the database wasn't properly initialized as part of the quickstart processs (see [#1816](https://github.com/datahub-project/datahub/issues/1816)). Please run the following command to manually initialize it.

```
docker exec -i mysql sh -c 'exec mysql datahub -udatahub -pdatahub' < docker/mysql/init.sql
```

## I've messed up my docker setup. How do I start from scratch?

Run the following script to remove all the containers and volumes created during the quickstart tutorial. Note that you'll also lose all the data as a result.

```
datahub docker nuke
```

## I'm seeing exceptions in DataHub GMS container like "Caused by: java.lang.IllegalStateException: Duplicate key com.linkedin.metadata.entity.ebean.EbeanAspectV2@dd26e011". What do I do?

This is related to a SQL column collation issue. The default collation we previously used (prior to Oct 26, 2021) for URN fields was case-insensitive (utf8mb4_unicode_ci). We've recently moved
to deploying with a case-sensitive collation (utf8mb4_bin) by default. In order to update a deployment that was started before Oct 26, 2021 (v0.8.16 and below) to have the new collation, you must run this command against your SQL DB directly:

```
ALTER TABLE metadata_aspect_v2 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
```

## I've modified the default user.props file to include a custom username and password, but I don't see the new user(s) inside the Users & Groups tab. Why not?

Currently, `user.props` is a file used by the JAAS PropertyFileLoginModule solely for the purpose of **Authentication**. The file is not used as an source from which to
ingest additional metadata about the user. For that, you'll need to ingest some custom information about your new user using the Rest.li APIs or the [Metadata File ingestion source](../generated/ingestion/sources/metadata-file.md).

For an example of a file that ingests user information, check out [single_mce.json](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/mce_files/single_mce.json), which ingests a single user object into DataHub. Notice that the "urn" field provided
will need to align with the custom username you've provided in user.props file. For example, if your user.props file contains:

```
my-custom-user:my-custom-password
```

You'll need to ingest some metadata of the following form to see it inside the DataHub UI:

```
{
  "auditHeader": null,
  "proposedSnapshot": {
    "com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot": {
      "urn": "urn:li:corpuser:my-custom-user",
      "aspects": [
        {
          "com.linkedin.pegasus2avro.identity.CorpUserInfo": {
            "active": true,
            "displayName": {
              "string": "The name of the custom user"
            },
            "email": "my-custom-user-email@example.io",
            "title": {
              "string": "Engineer"
            },
            "managerUrn": null,
            "departmentId": null,
            "departmentName": null,
            "firstName": null,
            "lastName": null,
            "fullName": {
              "string": "My Custom User"
            },
            "countryCode": null
          }
        }
      ]
    }
  },
  "proposedDelta": null
}
```

## I've configured OIDC, but I cannot login. I get continuously redirected. What do I do?

Sorry to hear that!

This phenomena may be due to the size of a Cookie DataHub uses to authenticate its users. If it's too large ( > 4096), then you'll see this behavior. The cookie embeds an encoded version of the information returned by your OIDC Identity Provider - if they return a lot of information, this can be the root cause.

One solution is to use Play Cache to persist this session information for a user. This means the attributes about the user (and their session info) will be stored in an in-memory store in the `datahub-frontend` service, instead of a browser-side cookie.

To configure the Play Cache session store, you can set the env variable "PAC4J_SESSIONSTORE_PROVIDER" as "PlayCacheSessionStore" for the `datahub-frontend` container.

Do note that there are downsides to using the Play Cache. Specifically, it will make `datahub-frontend` a stateful server. If you have multiple instances of `datahub-frontend` deployed, you'll need to ensure that the same user is deterministically routed to the same service container (since the sessions are stored in memory). If you're using a single instance of `datahub-frontend` (the default), then things should "just work".

For more details, please refer to https://github.com/datahub-project/datahub/pull/5114
