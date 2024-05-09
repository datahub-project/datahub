import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CLI Ingestion

Batch ingestion involves extracting metadata from a source system in bulk. Typically, this happens on a predefined schedule using the [Metadata Ingestion ](metadata-ingestion/README.md#install-from-pypi)framework.
The metadata that is extracted includes point-in-time instances of dataset, chart, dashboard, pipeline, user, group, usage, and task metadata.

## Installing DataHub CLI

:::note Required Python Version
Installing DataHub CLI requires Python 3.6+.
:::

<Tabs>
<TabItem value="pypi" label="Install from PyPI for OSS Release" default>

Run the following commands in your terminal:

```
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
python3 -m datahub version
```

Your command line should return the proper version of DataHub upon executing these commands successfully.

</TabItem>
<TabItem value="pip" label="Install from command line with pip">

Determine the version you would like to install and obtain a read access token by requesting a one-time-secret from the Acryl team then run the following command:

```shell
python3 -m pip install acryl-datahub==<VERSION> --index-url https://<TOKEN>:@pypi.fury.io/acryl-data/`
```

</TabItem>
</Tabs>

Check out the [CLI Installation Guide](../docs/cli.md#installation) for more installation options and troubleshooting tips.


## Installing Connector Plugins 

Our CLI follows a plugin architecture. You must install connectors for different data sources individually. For a list of all supported data sources, see [the open source docs](metadata-ingestion/README.md#installing-plugins).
Once you've found the connectors you care about, simply install them using `pip install`.
For example, to install the `mysql` connector, you can run

```shell
pip install --upgrade acryl-datahub[mysql]
```
Check out the [alternative installation options](../docs/cli.md#alternate-installation-options) for more reference.

## Configuring a Recipe

Create a `recipe.yml` file that defines the source and sink for metadata, as shown below.
[Recipes](metadata-ingestion/README.md#recipes) are yaml configuration files that serve as input to the Metadata Ingestion framework. Each recipe file define a single source to read from and a single destination to push the metadata.
The two most important pieces of the file are the _source_ and _sink_ configuration blocks.

The _source_ configuration block defines where to extract metadata from. This can be an OLTP database system, a data warehouse, or something as simple as a file. Each source has custom configuration depending on what is required to access metadata from the source. To see configurations required for each supported source, refer to the [Sources](metadata-ingestion/README.md#sources) documentation.

The _sink_ configuration block defines where to push metadata into. Each sink type requires specific configurations, the details of which are detailed in the [Sinks](metadata-ingestion/README.md#sinks) documentation.
In Acryl DataHub deployments, you _must_ use a sink of type `datahub-rest`, which simply means that metadata will be pushed to the REST endpoints exposed by your DataHub instance. The required configurations for this sink are

1. **server**: the location of the REST API exposed by your instance of DataHub
2. **token**: a unique API key used to authenticate requests to your instance's REST API

The token can be retrieved by logging in as admin. You can go to Settings page and generate a Personal Access Token with your desired expiration date.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/home-(1).png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/settings.png"/>
</p>


To configure your instance of DataHub as the destination for ingestion, set the "server" field of your recipe to point to your Acryl instance's domain suffixed by the path `/gms`, as shown below.
A complete example of a DataHub recipe file, which reads from MySQL and writes into a DataHub instance:


```yaml
# example-recipe.yml

# MySQL source configuration
source:
  type: mysql
  config:
    username: root
    password: password
    host_port: localhost:3306

# Recipe sink configuration.
sink:
  type: "datahub-rest"
  config:
    server: "https://<your domain name>.acryl.io/gms"
    token: <Your API key>
```
:::info Secure Your API Key
Your API key is a signed JSON Web Token that is valid for 6 months from the date of issuance. Please keep this key secure & avoid sharing it.
If your key is compromised for any reason, please reach out to the Acryl team at support@acryl.io.:::
:::

For more information and examples on configuring recipes, please refer to [Recipes](recipe_overview.md).


## Ingesting Metadata
The final step requires invoking the DataHub CLI to ingest metadata based on your recipe configuration file.
To do so, simply run `datahub ingest` with a pointer to your YAML recipe file:
```shell
datahub ingest -c <path/to/recipe.yml>
```

## Scheduling Ingestion

Ingestion can either be run in an ad-hoc manner by a system administrator or scheduled for repeated executions. Most commonly, ingestion will be run on a daily cadence.
To schedule your ingestion job, we recommend using a job schedule like [Apache Airflow](https://airflow.apache.org/). In cases of simpler deployments, a CRON job scheduled on an always-up machine can also work.
Note that each source system will require a separate recipe file. This allows you to schedule ingestion from different sources independently or together.

:::note Real-time Ingestion
Real-time ingestion setup is not recommended for an initial POC as it generally takes longer to configure and is prone to inevitable system errors.
You can find more information on real-time ingestion [here](docs/lineage/airflow.md).
:::

## Reference

Please refer the following pages for advanced guids on CLI ingestion.

- [Reference for `datahub ingest` command](../docs/cli.md#ingest)
- [UI Ingestion Guide](../docs/ui-ingestion.md)

:::Tip Compatibility
DataHub server uses a 3 digit versioning scheme, while the CLI uses a 4 digit scheme. For example, if you're using DataHub server version 0.10.0, you should use CLI version 0.10.0.x, where x is a patch version.
We do this because we do CLI releases at a much higher frequency than server releases, usually every few days vs twice a month.

For ingestion sources, any breaking changes will be highlighted in the [release notes](../docs/how/updating-datahub.md). When fields are deprecated or otherwise changed, we will try to maintain backwards compatibility for two server releases, which is about 4-6 weeks. The CLI will also print warnings whenever deprecated options are used.
:::

