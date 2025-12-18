---
sidebar_position: 1
title: Dataset
slug: /generated/metamodel/entities/dataset
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataset.md
---
# Dataset

The dataset entity is one the most important entities in the metadata model. They represent collections of data that are typically represented as Tables or Views in a database (e.g. BigQuery, Snowflake, Redshift etc.), Streams in a stream-processing environment (Kafka, Pulsar etc.), bundles of data found as Files or Folders in data lake systems (S3, ADLS, etc.).

## Identity

Datasets are identified by three pieces of information:

- The platform that they belong to: this is the specific data technology that hosts this dataset. Examples are `hive`, `bigquery`, `redshift` etc. See [dataplatform](./dataPlatform.md) for more details.
- The name of the dataset in the specific platform. Each platform will have a unique way of naming assets within its system. Usually, names are composed by combining the structural elements of the name and separating them by `.`. e.g. relational datasets are usually named as `<db>.<schema>.<table>`, except for platforms like MySQL which do not have the concept of a `schema`; as a result MySQL datasets are named `<db>.<table>`. In cases where the specific platform can have multiple instances (e.g. there are multiple different instances of MySQL databases that have different data assets in them), names can also include instance ids, making the general pattern for a name `<platform_instance>.<db>.<schema>.<table>`.
- The environment or fabric in which the dataset belongs: this is an additional qualifier available on the identifier, to allow disambiguating datasets that live in Production environments from datasets that live in Non-production environments, such as Staging, QA, etc. The full list of supported environments / fabrics is available in [FabricType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/li-utils/src/main/pegasus/com/linkedin/common/FabricType.pdl).

An example of a dataset identifier is `urn:li:dataset:(urn:li:dataPlatform:redshift,userdb.public.customer_table,PROD)`.

## Important Capabilities

### Schemas

Datasets support flat and nested schemas. Metadata about schemas are contained in the `schemaMetadata` aspect. Schemas are represented as an array of fields, each identified by a specific field path.

#### Field Paths explained

Fields that are either top-level or expressible unambiguously using a `.` based notation can be identified via a v1 path name, whereas fields that are part of a union need further disambiguation using `[type=X]` markers.
Taking a simple nested schema as described below:

```javascript
{
    "type": "record",
    "name": "Customer",
    "fields":[
        {
        "type": "record",
        "name": "address",
        "fields": [
            { "name": "zipcode", "type": string},
            {"name": "street", "type": string}]
        }],
}
```

- v1 field path: `address.zipcode`
- v2 field path: `[version=2.0].[type=struct].address.[type=string].zipcode"`. More examples and a formal specification of a v2 fieldPath can be found [here](docs/advanced/field-path-spec-v2.md).

Understanding field paths is important, because they are the identifiers through which tags, terms, documentation on fields are expressed. Besides the type and name of the field, schemas also contain descriptions attached to the individual fields, as well as information about primary and foreign keys.

The following code snippet shows you how to add a Schema containing 3 fields to a dataset.

<details>
<summary>Python SDK: Add a schema to a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_schema.py
from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    platform="hive",
    name="realestate_db.sales",
    schema=[
        # tuples of (field name / field path, data type, description)
        (
            "address.zipcode",
            "varchar(50)",
            "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
        ),
        ("address.street", "varchar(100)", "Street corresponding to the address"),
        ("last_sold_date", "date", "Date of the last sale date for this property"),
    ],
)

client.entities.upsert(dataset)

```

</details>

### Tags and Glossary Terms

Datasets can have Tags or Terms attached to them. Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms so you understand when you should use which.

#### Adding Tags or Glossary Terms at the top-level to a dataset

At the top-level, tags are added to datasets using the `globalTags` aspect, while terms are added using the `glossaryTerms` aspect.

Here is an example for how to add a tag to a dataset. Note that this involves reading the currently set tags on the dataset and then adding a new one if needed.

<details>
<summary>Python SDK: Add a tag to a dataset at the top-level</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_tag.py
from datahub.sdk import DataHubClient, DatasetUrn, TagUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))
dataset.add_tag(TagUrn("purchase"))

client.entities.update(dataset)

```

</details>

Here is an example of adding a term to a dataset. Note that this involves reading the currently set terms on the dataset and then adding a new one if needed.

<details>
<summary>Python SDK: Add a term to a dataset at the top-level</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_term.py
from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)
dataset.add_term(GlossaryTermUrn("Classification.HighlyConfidential"))

# Or, if you know the term name but not the term urn:
term_urn = client.resolve.term(name="PII")
dataset.add_term(term_urn)

client.entities.update(dataset)

```

</details>

#### Adding Tags or Glossary Terms to columns / fields of a dataset

Tags and Terms can also be attached to an individual column (field) of a dataset. These attachments are done via the `schemaMetadata` aspect by ingestion connectors / transformers and via the `editableSchemaMetadata` aspect by the UI.
This separation allows the writes from the replication of metadata from the source system to be isolated from the edits made in the UI.

Here is an example of how you can add a tag to a field in a dataset using the low-level Python SDK.

<details>
<summary>Python SDK: Add a tag to a column (field) of a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_column_tag.py
from datahub.sdk import DataHubClient, DatasetUrn, TagUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="fct_users_created", env="PROD")
)

dataset["user_name"].add_tag(TagUrn("deprecated"))

client.entities.update(dataset)

```

</details>

Similarly, here is an example of how you would add a term to a field in a dataset using the low-level Python SDK.

<details>
<summary>Python SDK: Add a term to a column (field) of a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_column_term.py
from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

dataset["address.zipcode"].add_term(GlossaryTermUrn("Classification.Location"))

client.entities.update(dataset)

```

</details>

### Ownership

Ownership is associated to a dataset using the `ownership` aspect. Owners can be of a few different types, `DATAOWNER`, `PRODUCER`, `DEVELOPER`, `CONSUMER`, etc. See [OwnershipType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-models/src/main/pegasus/com/linkedin/common/OwnershipType.pdl) for the full list of ownership types and their meanings. Ownership can be inherited from source systems, or additionally added in DataHub using the UI. Ingestion connectors for sources will automatically set owners when the source system supports it.

#### Adding Owners

The following script shows you how to add an owner to a dataset using the low-level Python SDK.

<details>
<summary>Python SDK: Add an owner to a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_owner.py
from datahub.sdk import CorpUserUrn, DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))

# Add owner with the TECHNICAL_OWNER type
dataset.add_owner(CorpUserUrn("jdoe"))

client.entities.update(dataset)

```

</details>

### Fine-grained lineage

Fine-grained lineage at field level can be associated to a dataset in two ways - either directly attached to the `upstreamLineage` aspect of a dataset, or captured as part of the `dataJobInputOutput` aspect of a dataJob.

<details>
<summary>Python SDK: Add fine-grained lineage to a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/add_lineage_dataset_to_dataset_with_query_node.py
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

upstream_urn = DatasetUrn(platform="snowflake", name="upstream_table")
downstream_urn = DatasetUrn(platform="snowflake", name="downstream_table")

transformation_text = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HighValueFilter").getOrCreate()
df = spark.read.table("customers")
high_value = df.filter("lifetime_value > 10000")
high_value.write.saveAsTable("high_value_customers")
"""

client.lineage.add_lineage(
    upstream=upstream_urn,
    downstream=downstream_urn,
    transformation_text=transformation_text,
    column_lineage={"id": ["id", "customer_id"]},
)

# by passing the transformation_text, the query node will be created with the table level lineage.
# transformation_text can be any transformation logic e.g. a spark job, an airflow DAG, python script, etc.
# if you have a SQL query, we recommend using add_dataset_lineage_from_sql instead.
# note that transformation_text itself will not create a column level lineage.

```

</details>

<details>
<summary>Python SDK: Add fine-grained lineage to a datajob</summary>

```python
# Inlined from /metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass


def datasetUrn(tbl):
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(tbl), fld)


# Lineage of fields output by a job
# bar.c1          <-- unknownFunc(bar2.c1, bar4.c1)
# bar.c2          <-- myfunc(bar3.c2)
# {bar.c3,bar.c4} <-- unknownFunc(bar2.c2, bar2.c3, bar3.c1)
# bar.c5          <-- unknownFunc(bar3)
# {bar.c6,bar.c7} <-- unknownFunc(bar4)
# bar2.c9 has no upstream i.e. its values are somehow created independently within this job.

# Note that the semantic of the "transformOperation" value is contextual.
# In above example, it is regarded as some kind of UDF; but it could also be an expression etc.

fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c1"), fldUrn("bar4", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c1")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar3", "c2")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c2")],
        confidenceScore=0.8,
        transformOperation="myfunc",
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bar2", "c2"), fldUrn("bar2", "c3"), fldUrn("bar3", "c1")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c3"), fldUrn("bar", "c4")],
        confidenceScore=0.7,
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar3")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar", "c5")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.DATASET,
        upstreams=[datasetUrn("bar4")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bar", "c6"), fldUrn("bar", "c7")],
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.NONE,
        upstreams=[],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bar2", "c9")],
    ),
]

# The lineage of output col bar.c9 is unknown. So there is no lineage for it above.
# Note that bar2 is an input as well as an output dataset, but some fields are inputs while other fields are outputs.

dataJobInputOutput = DataJobInputOutputClass(
    inputDatasets=[datasetUrn("bar2"), datasetUrn("bar3"), datasetUrn("bar4")],
    outputDatasets=[datasetUrn("bar"), datasetUrn("bar2")],
    inputDatajobs=None,
    inputDatasetFields=[
        fldUrn("bar2", "c1"),
        fldUrn("bar2", "c2"),
        fldUrn("bar2", "c3"),
        fldUrn("bar3", "c1"),
        fldUrn("bar3", "c2"),
        fldUrn("bar4", "c1"),
    ],
    outputDatasetFields=[
        fldUrn("bar", "c1"),
        fldUrn("bar", "c2"),
        fldUrn("bar", "c3"),
        fldUrn("bar", "c4"),
        fldUrn("bar", "c5"),
        fldUrn("bar", "c6"),
        fldUrn("bar", "c7"),
        fldUrn("bar", "c9"),
        fldUrn("bar2", "c9"),
    ],
    fineGrainedLineages=fineGrainedLineages,
)

dataJobLineageMcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_data_job_urn("spark", "Flow1", "Task1"),
    aspect=dataJobInputOutput,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(dataJobLineageMcp)

```

</details>

#### Querying lineage information

The standard [GET APIs to retrieve entities](/docs/metadata-service/#retrieving-entities) can be used to fetch the dataset/datajob created by the above example.
The response will include the fine-grained lineage information as well.

<details>
<summary>Fetch entity snapshot, including fine-grained lineages</summary>

```
curl 'http://localhost:8080/entities/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres,bar,PROD)'
```

```
curl 'http://localhost:8080/entities/urn%3Ali%3AdataJob%3A(urn%3Ali%3AdataFlow%3A(spark,Flow1,prod),Task1)'
```

</details>

The below queries can be used to find the upstream/downstream datasets/fields of a dataset/datajob.

<details>
<summary>Find upstream datasets and fields of a dataset</summary>

```
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Apostgres,bar,PROD)&types=DownstreamOf'

{
    "start": 0,
    "count": 9,
    "relationships": [
        {
            "type": "DownstreamOf",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar4,PROD)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD),c1)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c3)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c2)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD),c2)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar4,PROD),c1)"
        },
        {
            "type": "DownstreamOf",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c1)"
        }
    ],
    "total": 9
}
```

</details>

<details>
<summary>Find the datasets and fields consumed by a datajob i.e. inputs to a datajob</summary>

```
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AdataJob%3A(urn%3Ali%3AdataFlow%3A(spark,Flow1,prod),Task1)&types=Consumes'

{
    "start": 0,
    "count": 9,
    "relationships": [
        {
            "type": "Consumes",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar4,PROD)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar4,PROD),c1)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD),c2)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar3,PROD),c1)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c3)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c2)"
        },
        {
            "type": "Consumes",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c1)"
        }
    ],
    "total": 9
}
```

</details>

<details>
<summary>Find the datasets and fields produced by a datajob i.e. outputs of a datajob</summary>

```
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3AdataJob%3A(urn%3Ali%3AdataFlow%3A(spark,Flow1,prod),Task1)&types=Produces'

{
    "start": 0,
    "count": 11,
    "relationships": [
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD),c9)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c9)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c7)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c6)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c5)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c4)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c3)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c2)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD),c1)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar2,PROD)"
        },
        {
            "type": "Produces",
            "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,bar,PROD)"
        }
    ],
    "total": 11
}
```

</details>

### Documentation, Links etc.

Documentation for Datasets is available via the `datasetProperties` aspect (typically filled out via ingestion connectors when information is already present in the source system) and via the `editableDatasetProperties` aspect (filled out via the UI typically)

Links that contain more knowledge about the dataset (e.g. links to Confluence pages) can be added via the `institutionalMemory` aspect.

Here is a simple script that shows you how to add documentation for a dataset including some links to pages using the low-level Python SDK.

<details>
<summary>Python SDK: Add documentation, links to a dataset</summary>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_documentation.py
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hive", name="realestate_db.sales"))

# Add dataset documentation
documentation = """## The Real Estate Sales Dataset
This is a really important Dataset that contains all the relevant information about sales that have happened organized by address.
"""
dataset.set_description(documentation)

# Add link to institutional memory
dataset.add_link(
    (
        "https://wikipedia.com/real_estate",
        "This is the definition of what real estate means",  # link description
    )
)

client.entities.update(dataset)

```

</details>

## Notable Exceptions

The following overloaded uses of the Dataset entity exist for convenience, but will likely move to fully modeled entity types in the future.

- OpenAPI endpoints: the GET API of OpenAPI endpoints are currently modeled as Datasets, but should really be modeled as a Service/API entity once this is created in the metadata model.
- DataHub's Logical Entities (e.g.. Dataset, Chart, Dashboard) are represented as Datasets, with sub-type Entity. These should really be modeled as Entities in a logical ER model once this is created in the metadata model.

## Aspects

### datasetKey
Key for a Dataset
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetKey"
  },
  "name": "DatasetKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data platform urn associated with the dataset"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldName": "id",
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Unique guid for dataset"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "TEXT_PARTIAL",
        "filterNameOverride": "Environment",
        "queryByDefault": false
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CORP": "Designates corporation fabrics",
          "DEV": "Designates development fabrics",
          "EI": "Designates early-integration fabrics",
          "NON_PROD": "Designates non-production fabrics",
          "PRD": "Alternative Prod spelling",
          "PRE": "Designates pre-production fabrics",
          "PROD": "Designates production fabrics",
          "QA": "Designates quality assurance fabrics",
          "RVW": "Designates review fabrics",
          "SANDBOX": "Designates sandbox fabrics",
          "SBX": "Alternative spelling for sandbox",
          "SIT": "System Integration Testing",
          "STG": "Designates staging fabrics",
          "TEST": "Designates testing fabrics",
          "TST": "Alternative Test spelling",
          "UAT": "Designates user acceptance testing fabrics"
        },
        "name": "FabricType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "DEV",
          "TEST",
          "QA",
          "UAT",
          "EI",
          "PRE",
          "STG",
          "NON_PROD",
          "PROD",
          "CORP",
          "RVW",
          "PRD",
          "TST",
          "SIT",
          "SBX",
          "SANDBOX"
        ],
        "doc": "Fabric group type"
      },
      "name": "origin",
      "doc": "Fabric type where dataset belongs to or where it was generated."
    }
  ],
  "doc": "Key for a Dataset"
}
```
</details>

### datasetProperties
Properties associated with a Dataset
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetProperties"
  },
  "name": "DatasetProperties",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Display name of the Dataset"
    },
    {
      "Searchable": {
        "addToFilters": false,
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "qualifiedName",
      "default": null,
      "doc": "Fully-qualified name of the Dataset"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the dataset"
    },
    {
      "deprecated": "Use ExternalReference.externalUrl field instead.",
      "java": {
        "class": "java.net.URI"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "uri",
      "default": null,
      "doc": "The abstracted URI such as hdfs:///data/tracking/PageViewEvent, file:///dir/file_name. Uri should not include any environment specific properties. Some datasets might not have a standardized uri, which makes this field optional (i.e. kafka topic)."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the event occur"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "Optional: The actor urn involved in the event."
            }
          ],
          "doc": "A standard event timestamp"
        }
      ],
      "name": "created",
      "default": null,
      "doc": "A timestamp documenting when the asset was created in the source Data Platform (not on DataHub)"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.TimeStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "A timestamp documenting when the asset was last modified in the source Data Platform (not on DataHub)"
    },
    {
      "deprecated": "Use GlobalTags aspect instead.",
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "tags",
      "default": [],
      "doc": "[Legacy] Unstructured tags for the dataset. Structured tags can be applied via the `GlobalTags` aspect.\nThis is now deprecated."
    }
  ],
  "doc": "Properties associated with a Dataset"
}
```
</details>

### editableDatasetProperties
EditableDatasetProperties stores editable changes made to dataset properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableDatasetProperties"
  },
  "name": "EditableDatasetProperties",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the dataset"
    },
    {
      "Searchable": {
        "fieldName": "editedName",
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "name",
      "default": null,
      "doc": "Editable display name of the Dataset"
    }
  ],
  "doc": "EditableDatasetProperties stores editable changes made to dataset properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
}
```
</details>

### datasetUpstreamLineage
Fine Grained upstream lineage for fields in a dataset
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetUpstreamLineage"
  },
  "deprecated": "use UpstreamLineage.fineGrainedLineages instead",
  "name": "DatasetUpstreamLineage",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "deprecated": "use FineGrainedLineage instead",
          "name": "DatasetFieldMapping",
          "namespace": "com.linkedin.dataset",
          "fields": [
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "created",
              "doc": "Audit stamp containing who reported the field mapping and when"
            },
            {
              "type": [
                {
                  "type": "enum",
                  "symbolDocs": {
                    "BLACKBOX": "Field transformation expressed as unknown black box function.",
                    "IDENTITY": "Field transformation expressed as Identity function."
                  },
                  "name": "TransformationType",
                  "namespace": "com.linkedin.common.fieldtransformer",
                  "symbols": [
                    "BLACKBOX",
                    "IDENTITY"
                  ],
                  "doc": "Type of the transformation involved in generating destination fields from source fields."
                },
                {
                  "type": "record",
                  "name": "UDFTransformer",
                  "namespace": "com.linkedin.common.fieldtransformer",
                  "fields": [
                    {
                      "type": "string",
                      "name": "udf",
                      "doc": "A UDF mentioning how the source fields got transformed to destination field. This is the FQCN(Fully Qualified Class Name) of the udf."
                    }
                  ],
                  "doc": "Field transformation expressed in UDF"
                }
              ],
              "name": "transformation",
              "doc": "Transfomration function between the fields involved"
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string"
                ]
              },
              "name": "sourceFields",
              "doc": "Source fields from which the fine grained lineage is derived"
            },
            {
              "deprecated": "use SchemaFieldPath and represent as generic Urn instead",
              "java": {
                "class": "com.linkedin.common.urn.DatasetFieldUrn"
              },
              "type": "string",
              "name": "destinationField",
              "doc": "Destination field which is derived from source fields"
            }
          ],
          "doc": "Representation of mapping between fields in source dataset to the field in destination dataset"
        }
      },
      "name": "fieldMappings",
      "doc": "Upstream to downstream field level lineage mappings"
    }
  ],
  "doc": "Fine Grained upstream lineage for fields in a dataset"
}
```
</details>

### upstreamLineage
Upstream lineage of a dataset
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "upstreamLineage"
  },
  "name": "UpstreamLineage",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Upstream",
          "namespace": "com.linkedin.dataset",
          "fields": [
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "auditStamp",
              "default": {
                "actor": "urn:li:corpuser:unknown",
                "impersonator": null,
                "time": 0,
                "message": null
              },
              "doc": "Audit stamp containing who reported the lineage and when."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created the lineage and when."
            },
            {
              "Relationship": {
                "createdActor": "upstreams/*/created/actor",
                "createdOn": "upstreams/*/created/time",
                "entityTypes": [
                  "dataset"
                ],
                "isLineage": true,
                "name": "DownstreamOf",
                "properties": "upstreams/*/properties",
                "updatedActor": "upstreams/*/auditStamp/actor",
                "updatedOn": "upstreams/*/auditStamp/time",
                "via": "upstreams/*/query"
              },
              "Searchable": {
                "fieldName": "upstreams",
                "fieldType": "URN",
                "hasValuesFieldName": "hasUpstreams",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "The upstream dataset the lineage points to"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "COPY": "Direct copy without modification",
                  "TRANSFORMED": "Transformed data with modification (format or content change)",
                  "VIEW": "Represents a view defined on the sources e.g. Hive view defined on underlying hive tables or a Hive table pointing to a HDFS dataset or DALI view defined on multiple sources"
                },
                "name": "DatasetLineageType",
                "namespace": "com.linkedin.dataset",
                "symbols": [
                  "COPY",
                  "TRANSFORMED",
                  "VIEW"
                ],
                "doc": "The various types of supported dataset lineage"
              },
              "name": "type",
              "doc": "The type of the lineage"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "properties",
              "default": null,
              "doc": "A generic properties bag that allows us to store specific information on this graph edge."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "query",
              "default": null,
              "doc": "If the lineage is generated by a query, a reference to the query"
            }
          ],
          "doc": "Upstream lineage information about a dataset including the source reporting the lineage"
        }
      },
      "name": "upstreams",
      "doc": "List of upstream dataset lineage information"
    },
    {
      "Relationship": {
        "/*/upstreams/*": {
          "entityTypes": [
            "dataset",
            "schemaField"
          ],
          "name": "DownstreamOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FineGrainedLineage",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "DATASET": " Indicates that this lineage is originating from upstream dataset(s)",
                    "FIELD_SET": " Indicates that this lineage is originating from upstream field(s)",
                    "NONE": " Indicates that there is no upstream lineage i.e. the downstream field is not a derived field"
                  },
                  "name": "FineGrainedLineageUpstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD_SET",
                    "DATASET",
                    "NONE"
                  ],
                  "doc": "The type of upstream entity in a fine-grained lineage"
                },
                "name": "upstreamType",
                "doc": "The type of upstream entity"
              },
              {
                "Searchable": {
                  "/*": {
                    "fieldName": "fineGrainedUpstreams",
                    "fieldType": "URN",
                    "hasValuesFieldName": "hasFineGrainedUpstreams",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "upstreams",
                "default": null,
                "doc": "Upstream entities in the lineage"
              },
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "FIELD": " Indicates that the lineage is for a single, specific, downstream field",
                    "FIELD_SET": " Indicates that the lineage is for a set of downstream fields"
                  },
                  "name": "FineGrainedLineageDownstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD",
                    "FIELD_SET"
                  ],
                  "doc": "The type of downstream field(s) in a fine-grained lineage"
                },
                "name": "downstreamType",
                "doc": "The type of downstream field(s)"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "downstreams",
                "default": null,
                "doc": "Downstream fields in the lineage"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "transformOperation",
                "default": null,
                "doc": "The transform operation applied to the upstream entities to produce the downstream field(s)"
              },
              {
                "type": "float",
                "name": "confidenceScore",
                "default": 1.0,
                "doc": "The confidence in this lineage between 0 (low confidence) and 1 (high confidence)"
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "query",
                "default": null,
                "doc": "The query that was used to generate this lineage. \nPresent only if the lineage was generated from a detected query."
              }
            ],
            "doc": "A fine-grained lineage from upstream fields/datasets to downstream field(s)"
          }
        }
      ],
      "name": "fineGrainedLineages",
      "default": null,
      "doc": " List of fine-grained lineage information, including field-level lineage"
    }
  ],
  "doc": "Upstream lineage of a dataset"
}
```
</details>

### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```
</details>

### ownership
Ownership information of an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```
</details>

### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted)."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```
</details>

### schemaMetadata
SchemaMetadata to describe metadata related to store schema
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaMetadata"
  },
  "name": "SchemaMetadata",
  "namespace": "com.linkedin.schema",
  "fields": [
    {
      "validate": {
        "strlen": {
          "max": 500,
          "min": 1
        }
      },
      "type": "string",
      "name": "schemaName",
      "doc": "Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DataPlatformUrn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"
    },
    {
      "type": "long",
      "name": "version",
      "doc": "Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DatasetUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "dataset",
      "default": null,
      "doc": "Dataset this schema metadata is associated with."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "cluster",
      "default": null,
      "doc": "The cluster this schema metadata resides from"
    },
    {
      "type": "string",
      "name": "hash",
      "doc": "the SHA1 hash of the schema content"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "EspressoSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native espresso document schema."
            },
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The espresso table schema definition."
            }
          ],
          "doc": "Schema text of an espresso table schema."
        },
        {
          "type": "record",
          "name": "OracleDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for oracle data definition language that describes an oracle table."
        },
        {
          "type": "record",
          "name": "MySqlDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "tableSchema",
              "doc": "The native schema in the dataset's platform. This is a human readable (json blob) table schema."
            }
          ],
          "doc": "Schema holder for MySql data definition language that describes an MySql table."
        },
        {
          "type": "record",
          "name": "PrestoDDL",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."
            }
          ],
          "doc": "Schema holder for presto data definition language that describes a presto view."
        },
        {
          "type": "record",
          "name": "KafkaSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "documentSchema",
              "doc": "The native kafka document schema. This is a human readable avro document schema."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "documentSchemaType",
              "default": null,
              "doc": "The native kafka document schema type. This can be AVRO/PROTOBUF/JSON."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchema",
              "default": null,
              "doc": "The native kafka key schema as retrieved from Schema Registry"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "keySchemaType",
              "default": null,
              "doc": "The native kafka key schema type. This can be AVRO/PROTOBUF/JSON."
            }
          ],
          "doc": "Schema holder for kafka schema."
        },
        {
          "type": "record",
          "name": "BinaryJsonSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema text for binary JSON file format."
            }
          ],
          "doc": "Schema text of binary JSON schema."
        },
        {
          "type": "record",
          "name": "OrcSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "schema",
              "doc": "The native schema for ORC file format."
            }
          ],
          "doc": "Schema text of an ORC schema."
        },
        {
          "type": "record",
          "name": "Schemaless",
          "namespace": "com.linkedin.schema",
          "fields": [],
          "doc": "The dataset has no specific schema associated with it"
        },
        {
          "type": "record",
          "name": "KeyValueSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "keySchema",
              "doc": "The raw schema for the key in the key-value store."
            },
            {
              "type": "string",
              "name": "valueSchema",
              "doc": "The raw schema for the value in the key-value store."
            }
          ],
          "doc": "Schema text of a key-value store schema."
        },
        {
          "type": "record",
          "name": "OtherSchema",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "rawSchema",
              "doc": "The native schema in the dataset's platform."
            }
          ],
          "doc": "Schema holder for undefined schema types."
        }
      ],
      "name": "platformSchema",
      "doc": "The native schema in the dataset's platform."
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SchemaField",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "Searchable": {
                "boostScore": 1.0,
                "fieldName": "fieldPaths",
                "fieldType": "TEXT",
                "queryByDefault": "true"
              },
              "type": "string",
              "name": "fieldPath",
              "doc": "Flattened name of the field. Field is computed from jsonPath field."
            },
            {
              "Deprecated": true,
              "type": [
                "null",
                "string"
              ],
              "name": "jsonPath",
              "default": null,
              "doc": "Flattened name of a field in JSON Path notation."
            },
            {
              "type": "boolean",
              "name": "nullable",
              "default": false,
              "doc": "Indicates if this field is optional or nullable"
            },
            {
              "Searchable": {
                "boostScore": 0.1,
                "fieldName": "fieldDescriptions",
                "fieldType": "TEXT"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "description",
              "default": null,
              "doc": "Description"
            },
            {
              "Deprecated": true,
              "Searchable": {
                "boostScore": 0.2,
                "fieldName": "fieldLabels",
                "fieldType": "TEXT"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "label",
              "default": null,
              "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description.\n\nNote that this field is deprecated and is not surfaced in the UI."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "An AuditStamp corresponding to the creation of this schema field."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An AuditStamp corresponding to the last modification of this schema field."
            },
            {
              "type": {
                "type": "record",
                "name": "SchemaFieldDataType",
                "namespace": "com.linkedin.schema",
                "fields": [
                  {
                    "type": [
                      {
                        "type": "record",
                        "name": "BooleanType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Boolean field type."
                      },
                      {
                        "type": "record",
                        "name": "FixedType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Fixed field type."
                      },
                      {
                        "type": "record",
                        "name": "StringType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "String field type."
                      },
                      {
                        "type": "record",
                        "name": "BytesType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Bytes field type."
                      },
                      {
                        "type": "record",
                        "name": "NumberType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Number data type: long, integer, short, etc.."
                      },
                      {
                        "type": "record",
                        "name": "DateType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Date field type."
                      },
                      {
                        "type": "record",
                        "name": "TimeType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Time field type. This should also be used for datetimes."
                      },
                      {
                        "type": "record",
                        "name": "EnumType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Enum field type."
                      },
                      {
                        "type": "record",
                        "name": "NullType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Null field type."
                      },
                      {
                        "type": "record",
                        "name": "MapType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "keyType",
                            "default": null,
                            "doc": "Key type in a map"
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "valueType",
                            "default": null,
                            "doc": "Type of the value in a map"
                          }
                        ],
                        "doc": "Map field type."
                      },
                      {
                        "type": "record",
                        "name": "ArrayType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              {
                                "type": "array",
                                "items": "string"
                              }
                            ],
                            "name": "nestedType",
                            "default": null,
                            "doc": "List of types this array holds."
                          }
                        ],
                        "doc": "Array field type."
                      },
                      {
                        "type": "record",
                        "name": "UnionType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              "null",
                              {
                                "type": "array",
                                "items": "string"
                              }
                            ],
                            "name": "nestedTypes",
                            "default": null,
                            "doc": "List of types in union type."
                          }
                        ],
                        "doc": "Union field type."
                      },
                      {
                        "type": "record",
                        "name": "RecordType",
                        "namespace": "com.linkedin.schema",
                        "fields": [],
                        "doc": "Record field type."
                      }
                    ],
                    "name": "type",
                    "doc": "Data platform specific types"
                  }
                ],
                "doc": "Schema field data types"
              },
              "name": "type",
              "doc": "Platform independent field type of the field."
            },
            {
              "type": "string",
              "name": "nativeDataType",
              "doc": "The native type of the field in the dataset's platform as declared by platform schema."
            },
            {
              "type": "boolean",
              "name": "recursive",
              "default": false,
              "doc": "There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."
            },
            {
              "Relationship": {
                "/tags/*/tag": {
                  "entityTypes": [
                    "tag"
                  ],
                  "name": "SchemaFieldTaggedWith"
                }
              },
              "Searchable": {
                "/tags/*/attribution/actor": {
                  "fieldName": "fieldTagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/source": {
                  "fieldName": "fieldTagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/time": {
                  "fieldName": "fieldTagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                },
                "/tags/*/tag": {
                  "boostScore": 0.5,
                  "fieldName": "fieldTags",
                  "fieldType": "URN"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "globalTags"
                  },
                  "name": "GlobalTags",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "Relationship": {
                        "/*/tag": {
                          "entityTypes": [
                            "tag"
                          ],
                          "name": "TaggedWith"
                        }
                      },
                      "Searchable": {
                        "/*/tag": {
                          "addToFilters": true,
                          "boostScore": 0.5,
                          "fieldName": "tags",
                          "fieldType": "URN",
                          "filterNameOverride": "Tag",
                          "hasValuesFieldName": "hasTags",
                          "queryByDefault": true
                        }
                      },
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "TagAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.TagUrn"
                              },
                              "type": "string",
                              "name": "tag",
                              "doc": "Urn of the applied tag"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "tagAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "tagAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "tagAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                {
                                  "type": "record",
                                  "name": "MetadataAttribution",
                                  "namespace": "com.linkedin.common",
                                  "fields": [
                                    {
                                      "type": "long",
                                      "name": "time",
                                      "doc": "When this metadata was updated."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": "string",
                                      "name": "actor",
                                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "source",
                                      "default": null,
                                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                                    },
                                    {
                                      "type": {
                                        "type": "map",
                                        "values": "string"
                                      },
                                      "name": "sourceDetail",
                                      "default": {},
                                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                                    }
                                  ],
                                  "doc": "Information about who, why, and how this metadata was applied"
                                }
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
                        }
                      },
                      "name": "tags",
                      "doc": "Tags associated with a given entity"
                    }
                  ],
                  "doc": "Tag aspect used for applying tags to an entity"
                }
              ],
              "name": "globalTags",
              "default": null,
              "doc": "Tags associated with the field"
            },
            {
              "Relationship": {
                "/terms/*/urn": {
                  "entityTypes": [
                    "glossaryTerm"
                  ],
                  "name": "SchemaFieldWithGlossaryTerm"
                }
              },
              "Searchable": {
                "/terms/*/attribution/actor": {
                  "fieldName": "fieldTermAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/source": {
                  "fieldName": "fieldTermAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/time": {
                  "fieldName": "fieldTermAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                },
                "/terms/*/urn": {
                  "boostScore": 0.5,
                  "fieldName": "fieldGlossaryTerms",
                  "fieldType": "URN"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "glossaryTerms"
                  },
                  "name": "GlossaryTerms",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "GlossaryTermAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "Relationship": {
                                "entityTypes": [
                                  "glossaryTerm"
                                ],
                                "name": "TermedWith"
                              },
                              "Searchable": {
                                "addToFilters": true,
                                "fieldName": "glossaryTerms",
                                "fieldType": "URN",
                                "filterNameOverride": "Glossary Term",
                                "hasValuesFieldName": "hasGlossaryTerms",
                                "includeSystemModifiedAt": true,
                                "systemModifiedAtFieldName": "termsModifiedAt"
                              },
                              "java": {
                                "class": "com.linkedin.common.urn.GlossaryTermUrn"
                              },
                              "type": "string",
                              "name": "urn",
                              "doc": "Urn of the applied glossary term"
                            },
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "actor",
                              "default": null,
                              "doc": "The user URN which will be credited for adding associating this term to the entity"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "termAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "termAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "termAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                "com.linkedin.common.MetadataAttribution"
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied glossary term."
                        }
                      },
                      "name": "terms",
                      "doc": "The related business terms"
                    },
                    {
                      "type": "com.linkedin.common.AuditStamp",
                      "name": "auditStamp",
                      "doc": "Audit stamp containing who reported the related business term"
                    }
                  ],
                  "doc": "Related business terms information"
                }
              ],
              "name": "glossaryTerms",
              "default": null,
              "doc": "Glossary terms associated with the field"
            },
            {
              "type": "boolean",
              "name": "isPartOfKey",
              "default": false,
              "doc": "For schema fields that are part of complex keys, set this field to true\nWe do this to easily distinguish between value and key fields"
            },
            {
              "type": [
                "null",
                "boolean"
              ],
              "name": "isPartitioningKey",
              "default": null,
              "doc": "For Datasets which are partitioned, this determines the partitioning key.\nNote that multiple columns can be part of a partitioning key, but currently we do not support\nrendering the ordered partitioning key."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "jsonProps",
              "default": null,
              "doc": "For schema fields that have other properties that are not modeled explicitly,\nuse this field to serialize those properties into a JSON string"
            }
          ],
          "doc": "SchemaField to describe metadata related to dataset schema."
        }
      },
      "name": "fields",
      "doc": "Client provided a list of fields from document schema."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "primaryKeys",
      "default": null,
      "doc": "Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."
    },
    {
      "deprecated": "Use foreignKeys instead.",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "record",
            "name": "ForeignKeySpec",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": [
                  {
                    "type": "record",
                    "name": "DatasetFieldForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.DatasetUrn"
                        },
                        "type": "string",
                        "name": "parentDataset",
                        "doc": "dataset that stores the resource."
                      },
                      {
                        "type": {
                          "type": "array",
                          "items": "string"
                        },
                        "name": "currentFieldPaths",
                        "doc": "List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."
                      },
                      {
                        "type": "string",
                        "name": "parentField",
                        "doc": "SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."
                      }
                    ],
                    "doc": "For non-urn based foregin keys."
                  },
                  {
                    "type": "record",
                    "name": "UrnForeignKey",
                    "namespace": "com.linkedin.schema",
                    "fields": [
                      {
                        "type": "string",
                        "name": "currentFieldPath",
                        "doc": "Field in hosting(current) SchemaMetadata."
                      }
                    ],
                    "doc": "If SchemaMetadata fields make any external references and references are of type com.linkedin.common.Urn or any children, this models can be used to mark it."
                  }
                ],
                "name": "foreignKey",
                "doc": "Foreign key definition in metadata schema."
              }
            ],
            "doc": "Description of a foreign key in a schema."
          }
        }
      ],
      "name": "foreignKeysSpecs",
      "default": null,
      "doc": "Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "ForeignKeyConstraint",
            "namespace": "com.linkedin.schema",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the constraint, likely provided from the source"
              },
              {
                "Relationship": {
                  "/*": {
                    "entityTypes": [
                      "schemaField"
                    ],
                    "name": "ForeignKeyTo"
                  }
                },
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "foreignFields",
                "doc": "Fields the constraint maps to on the foreign dataset"
              },
              {
                "type": {
                  "type": "array",
                  "items": "string"
                },
                "name": "sourceFields",
                "doc": "Fields the constraint maps to on the source dataset"
              },
              {
                "Relationship": {
                  "entityTypes": [
                    "dataset"
                  ],
                  "name": "ForeignKeyToDataset"
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "foreignDataset",
                "doc": "Reference to the foreign dataset for ease of lookup"
              }
            ],
            "doc": "Description of a foreign key constraint in a schema."
          }
        }
      ],
      "name": "foreignKeys",
      "default": null,
      "doc": "List of foreign key constraints for the schema"
    }
  ],
  "doc": "SchemaMetadata to describe metadata related to store schema"
}
```
</details>

### editableSchemaMetadata
EditableSchemaMetadata stores editable changes made to schema metadata. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableSchemaMetadata"
  },
  "name": "EditableSchemaMetadata",
  "namespace": "com.linkedin.schema",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "EditableSchemaFieldInfo",
          "namespace": "com.linkedin.schema",
          "fields": [
            {
              "type": "string",
              "name": "fieldPath",
              "doc": "FieldPath uniquely identifying the SchemaField this metadata is associated with"
            },
            {
              "Searchable": {
                "boostScore": 0.1,
                "fieldName": "editedFieldDescriptions",
                "fieldType": "TEXT"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "description",
              "default": null,
              "doc": "Description"
            },
            {
              "Relationship": {
                "/tags/*/tag": {
                  "entityTypes": [
                    "tag"
                  ],
                  "name": "EditableSchemaFieldTaggedWith"
                }
              },
              "Searchable": {
                "/tags/*/attribution/actor": {
                  "fieldName": "editedFieldTagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/source": {
                  "fieldName": "editedFieldTagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/tags/*/attribution/time": {
                  "fieldName": "editedFieldTagAttributionDates",
                  "fieldType": "DATETIME"
                },
                "/tags/*/tag": {
                  "boostScore": 0.5,
                  "fieldName": "editedFieldTags",
                  "fieldType": "URN"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "globalTags"
                  },
                  "name": "GlobalTags",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "Relationship": {
                        "/*/tag": {
                          "entityTypes": [
                            "tag"
                          ],
                          "name": "TaggedWith"
                        }
                      },
                      "Searchable": {
                        "/*/tag": {
                          "addToFilters": true,
                          "boostScore": 0.5,
                          "fieldName": "tags",
                          "fieldType": "URN",
                          "filterNameOverride": "Tag",
                          "hasValuesFieldName": "hasTags",
                          "queryByDefault": true
                        }
                      },
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "TagAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.TagUrn"
                              },
                              "type": "string",
                              "name": "tag",
                              "doc": "Urn of the applied tag"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "tagAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "tagAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "tagAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                {
                                  "type": "record",
                                  "name": "MetadataAttribution",
                                  "namespace": "com.linkedin.common",
                                  "fields": [
                                    {
                                      "type": "long",
                                      "name": "time",
                                      "doc": "When this metadata was updated."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": "string",
                                      "name": "actor",
                                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "source",
                                      "default": null,
                                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                                    },
                                    {
                                      "type": {
                                        "type": "map",
                                        "values": "string"
                                      },
                                      "name": "sourceDetail",
                                      "default": {},
                                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                                    }
                                  ],
                                  "doc": "Information about who, why, and how this metadata was applied"
                                }
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
                        }
                      },
                      "name": "tags",
                      "doc": "Tags associated with a given entity"
                    }
                  ],
                  "doc": "Tag aspect used for applying tags to an entity"
                }
              ],
              "name": "globalTags",
              "default": null,
              "doc": "Tags associated with the field"
            },
            {
              "Relationship": {
                "/terms/*/urn": {
                  "entityTypes": [
                    "glossaryTerm"
                  ],
                  "name": "EditableSchemaFieldWithGlossaryTerm"
                }
              },
              "Searchable": {
                "/terms/*/attribution/actor": {
                  "fieldName": "editedFieldTermAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/source": {
                  "fieldName": "editedFieldTermAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/terms/*/attribution/time": {
                  "fieldName": "editedFieldTermAttributionDates",
                  "fieldType": "DATETIME"
                },
                "/terms/*/urn": {
                  "boostScore": 0.5,
                  "fieldName": "editedFieldGlossaryTerms",
                  "fieldType": "URN",
                  "includeSystemModifiedAt": true,
                  "systemModifiedAtFieldName": "schemaFieldTermsModifiedAt"
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "Aspect": {
                    "name": "glossaryTerms"
                  },
                  "name": "GlossaryTerms",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "GlossaryTermAssociation",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "Relationship": {
                                "entityTypes": [
                                  "glossaryTerm"
                                ],
                                "name": "TermedWith"
                              },
                              "Searchable": {
                                "addToFilters": true,
                                "fieldName": "glossaryTerms",
                                "fieldType": "URN",
                                "filterNameOverride": "Glossary Term",
                                "hasValuesFieldName": "hasGlossaryTerms",
                                "includeSystemModifiedAt": true,
                                "systemModifiedAtFieldName": "termsModifiedAt"
                              },
                              "java": {
                                "class": "com.linkedin.common.urn.GlossaryTermUrn"
                              },
                              "type": "string",
                              "name": "urn",
                              "doc": "Urn of the applied glossary term"
                            },
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "actor",
                              "default": null,
                              "doc": "The user URN which will be credited for adding associating this term to the entity"
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "context",
                              "default": null,
                              "doc": "Additional context about the association"
                            },
                            {
                              "Searchable": {
                                "/actor": {
                                  "fieldName": "termAttributionActors",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/source": {
                                  "fieldName": "termAttributionSources",
                                  "fieldType": "URN",
                                  "queryByDefault": false
                                },
                                "/time": {
                                  "fieldName": "termAttributionDates",
                                  "fieldType": "DATETIME",
                                  "queryByDefault": false
                                }
                              },
                              "type": [
                                "null",
                                "com.linkedin.common.MetadataAttribution"
                              ],
                              "name": "attribution",
                              "default": null,
                              "doc": "Information about who, why, and how this metadata was applied"
                            }
                          ],
                          "doc": "Properties of an applied glossary term."
                        }
                      },
                      "name": "terms",
                      "doc": "The related business terms"
                    },
                    {
                      "type": "com.linkedin.common.AuditStamp",
                      "name": "auditStamp",
                      "doc": "Audit stamp containing who reported the related business term"
                    }
                  ],
                  "doc": "Related business terms information"
                }
              ],
              "name": "glossaryTerms",
              "default": null,
              "doc": "Glossary terms associated with the field"
            }
          ],
          "doc": "SchemaField to describe metadata related to dataset schema."
        }
      },
      "name": "editableSchemaFieldInfo",
      "doc": "Client provided a list of fields from document schema."
    }
  ],
  "doc": "EditableSchemaMetadata stores editable changes made to schema metadata. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines."
}
```
</details>

### globalTags
Tag aspect used for applying tags to an entity
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tag",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```
</details>

### glossaryTerms
Related business terms information
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```
</details>

### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```
</details>

### dataPlatformInstance
The specific instance of the data platform that this entity belongs to
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```
</details>

### viewProperties
Details about a View. 
e.g. Gets activated when subTypes is view
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "viewProperties"
  },
  "name": "ViewProperties",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "materialized",
      "doc": "Whether the view is materialized"
    },
    {
      "type": "string",
      "name": "viewLogic",
      "doc": "The view logic"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "formattedViewLogic",
      "default": null,
      "doc": "The formatted view logic. This is particularly used for SQL sources, where the SQL\nlogic is formatted for better readability, and with dbt, where this contains the\ncompiled SQL logic."
    },
    {
      "type": "string",
      "name": "viewLanguage",
      "doc": "The view logic language / dialect"
    }
  ],
  "doc": "Details about a View. \ne.g. Gets activated when subTypes is view"
}
```
</details>

### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```
</details>

### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```
</details>

### domains
Links from an Asset to its Domains
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains"
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```
</details>

### applications
Links from an Asset to its Applications
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```
</details>

### container
Link from an asset to its parent container
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
}
```
</details>

### deprecation
Deprecation status of an entity
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
}
```
</details>

### testResults
Information about a Test Result
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```
</details>

### siblings
Siblings information of an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "siblings"
  },
  "name": "Siblings",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "name": "SiblingOf"
        }
      },
      "Searchable": {
        "/*": {
          "addHasValuesToFilters": true,
          "fieldName": "siblings",
          "fieldType": "URN",
          "hasValuesFieldName": "hasSiblings",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "siblings",
      "doc": "List of sibling entities"
    },
    {
      "type": "boolean",
      "name": "primary",
      "doc": "If this is the leader entity of the set of siblings"
    }
  ],
  "doc": "Siblings information of an entity."
}
```
</details>

### embed
Information regarding rendering an embed for an asset.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "embed"
  },
  "name": "Embed",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "renderUrl",
      "default": null,
      "doc": "An embed URL to be rendered inside of an iframe."
    }
  ],
  "doc": "Information regarding rendering an embed for an asset."
}
```
</details>

### incidentsSummary
Summary related incidents on an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
}
```
</details>

### access
Aspect used for associating roles to a dataset or any asset
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "access"
  },
  "name": "Access",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "RoleAssociation",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "role"
                  ],
                  "name": "AssociatedWith"
                },
                "Searchable": {
                  "addToFilters": true,
                  "fieldName": "roles",
                  "fieldType": "URN",
                  "filterNameOverride": "Role",
                  "hasValuesFieldName": "hasRoles",
                  "queryByDefault": false
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "urn",
                "doc": "Urn of the External Role"
              }
            ],
            "doc": "Properties of an applied Role. For now, just an Urn"
          }
        }
      ],
      "name": "roles",
      "default": null,
      "doc": "List of Roles which needs to be associated"
    }
  ],
  "doc": "Aspect used for associating roles to a dataset or any asset"
}
```
</details>

### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```
</details>

### forms
Forms that are assigned to this entity to be filled out
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "AuditStamp",
                        "namespace": "com.linkedin.common",
                        "fields": [
                          {
                            "type": "long",
                            "name": "time",
                            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": "string",
                            "name": "actor",
                            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "impersonator",
                            "default": null,
                            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "message",
                            "default": null,
                            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                          }
                        ],
                        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                      },
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```
</details>

### partitionsSummary
Defines how the data is partitioned for Data Lake tables (e.g. Hive, S3, Iceberg, Delta, Hudi, etc).
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "partitionsSummary"
  },
  "name": "PartitionsSummary",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "PartitionSummary",
          "namespace": "com.linkedin.dataset",
          "fields": [
            {
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "createdTime",
              "default": null,
              "doc": "The created time for a given partition."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "lastModifiedTime",
              "default": null,
              "doc": "The last modified / touched time for a given partition."
            }
          ],
          "doc": "Defines how the data is partitioned"
        }
      ],
      "name": "minPartition",
      "default": null,
      "doc": "The minimum partition as ordered"
    },
    {
      "type": [
        "null",
        "com.linkedin.dataset.PartitionSummary"
      ],
      "name": "maxPartition",
      "default": null,
      "doc": "The maximum partition as ordered"
    }
  ],
  "doc": "Defines how the data is partitioned for Data Lake tables (e.g. Hive, S3, Iceberg, Delta, Hudi, etc)."
}
```
</details>

### versionProperties
Properties about a versioned asset i.e. dataset, ML Model, etc.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionProperties"
  },
  "name": "VersionProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "versionSet"
        ],
        "name": "VersionOf"
      },
      "Searchable": {
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "versionSet",
      "doc": "The linked Version Set entity that ties multiple versioned assets together"
    },
    {
      "Searchable": {
        "/versionTag": {
          "fieldName": "version",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "record",
        "name": "VersionTag",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "string"
            ],
            "name": "versionTag",
            "default": null
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "MetadataAttribution",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When this metadata was updated."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "source",
                    "default": null,
                    "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                  },
                  {
                    "type": {
                      "type": "map",
                      "values": "string"
                    },
                    "name": "sourceDetail",
                    "default": {},
                    "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                  }
                ],
                "doc": "Information about who, why, and how this metadata was applied"
              }
            ],
            "name": "metadataAttribution",
            "default": null
          }
        ],
        "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
      },
      "name": "version",
      "doc": "Label for this versioned asset, is unique within a version set"
    },
    {
      "Searchable": {
        "/*/versionTag": {
          "fieldName": "aliases",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.VersionTag"
      },
      "name": "aliases",
      "default": [],
      "doc": "Associated aliases for this versioned asset"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "comment",
      "default": null,
      "doc": "Comment documenting what this version was created for, changes, or represents"
    },
    {
      "Searchable": {
        "fieldName": "versionSortId",
        "queryByDefault": false
      },
      "type": "string",
      "name": "sortId",
      "doc": "Sort identifier that determines where a version lives in the order of the Version Set.\nWhat this looks like depends on the Version Scheme. For sort ids generated by DataHub we use an 8 character string representation."
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "default": "LEXICOGRAPHIC_STRING",
      "doc": "What versioning scheme `sortId` belongs to.\nDefaults to a plain string that is lexicographically sorted."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "sourceCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when this asset version was created in the source system."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "metadataCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when the metadata for this version was created in DataHub"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": [
        "null",
        "boolean"
      ],
      "name": "isLatest",
      "default": null,
      "doc": "Marks whether this version is currently the latest. Set by a side effect and should not be modified by API."
    }
  ],
  "doc": "Properties about a versioned asset i.e. dataset, ML Model, etc."
}
```
</details>

### icebergCatalogInfo
Iceberg Catalog metadata associated with an Iceberg table/view
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "icebergCatalogInfo"
  },
  "name": "IcebergCatalogInfo",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "metadataPointer",
      "default": null,
      "doc": "When Datahub is the REST Catalog for an Iceberg Table, stores the current metadata pointer.\nIf the Iceberg table is managed by an external catalog, the metadata pointer is not set."
    },
    {
      "type": [
        "null",
        "boolean"
      ],
      "name": "view",
      "default": null
    }
  ],
  "doc": "Iceberg Catalog metadata associated with an Iceberg table/view"
}
```
</details>

### logicalParent
Relates a physical asset to a logical model.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "logicalParent"
  },
  "name": "LogicalParent",
  "namespace": "com.linkedin.logical",
  "fields": [
    {
      "Relationship": {
        "/destinationUrn": {
          "createdActor": "parent/created/actor",
          "createdOn": "parent/created/time",
          "entityTypes": [
            "dataset",
            "schemaField"
          ],
          "name": "PhysicalInstanceOf",
          "properties": "parent/properties",
          "updatedActor": "parent/lastModified/actor",
          "updatedOn": "parent/lastModified/time"
        }
      },
      "Searchable": {
        "/destinationUrn": {
          "addToFilters": true,
          "fieldName": "logicalParent",
          "fieldType": "URN",
          "filterNameOverride": "Physical Instance Of",
          "hasValuesFieldName": "hasLogicalParent",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "Edge",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "sourceUrn",
              "default": null,
              "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "destinationUrn",
              "doc": "Urn of the destination of this relationship edge."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "properties",
              "default": null,
              "doc": "A generic properties bag that allows us to store specific information on this graph edge."
            }
          ],
          "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
        }
      ],
      "name": "parent",
      "default": null
    }
  ],
  "doc": "Relates a physical asset to a logical model."
}
```
</details>

### datasetProfile (Timeseries)
Stats corresponding to datasets
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetProfile",
    "type": "timeseries"
  },
  "name": "DatasetProfile",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "hasValuesFieldName": "hasRowCount"
      },
      "type": [
        "null",
        "long"
      ],
      "name": "rowCount",
      "default": null,
      "doc": "The total number of rows"
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "hasValuesFieldName": "hasColumnCount"
      },
      "type": [
        "null",
        "long"
      ],
      "name": "columnCount",
      "default": null,
      "doc": "The total number of columns (or schema fields)"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DatasetFieldProfile",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": "string",
                "name": "fieldPath"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "uniqueCount",
                "default": null
              },
              {
                "type": [
                  "null",
                  "float"
                ],
                "name": "uniqueProportion",
                "default": null
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "nullCount",
                "default": null
              },
              {
                "type": [
                  "null",
                  "float"
                ],
                "name": "nullProportion",
                "default": null
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "min",
                "default": null
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "max",
                "default": null
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "mean",
                "default": null
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "median",
                "default": null
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "stdev",
                "default": null
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Quantile",
                      "namespace": "com.linkedin.dataset",
                      "fields": [
                        {
                          "type": "string",
                          "name": "quantile"
                        },
                        {
                          "type": "string",
                          "name": "value"
                        }
                      ]
                    }
                  }
                ],
                "name": "quantiles",
                "default": null
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "ValueFrequency",
                      "namespace": "com.linkedin.dataset",
                      "fields": [
                        {
                          "type": "string",
                          "name": "value"
                        },
                        {
                          "type": "long",
                          "name": "frequency"
                        }
                      ]
                    }
                  }
                ],
                "name": "distinctValueFrequencies",
                "default": null
              },
              {
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "Histogram",
                    "namespace": "com.linkedin.dataset",
                    "fields": [
                      {
                        "type": {
                          "type": "array",
                          "items": "string"
                        },
                        "name": "boundaries"
                      },
                      {
                        "type": {
                          "type": "array",
                          "items": "float"
                        },
                        "name": "heights"
                      }
                    ]
                  }
                ],
                "name": "histogram",
                "default": null
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "sampleValues",
                "default": null
              }
            ],
            "doc": "Stats corresponding to fields in a dataset"
          }
        }
      ],
      "name": "fieldProfiles",
      "default": null,
      "doc": "Profiles for each column (or schema field)"
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "hasValuesFieldName": "hasSizeInBytes"
      },
      "type": [
        "null",
        "long"
      ],
      "name": "sizeInBytes",
      "default": null,
      "doc": "Storage size in bytes"
    }
  ],
  "doc": "Stats corresponding to datasets"
}
```
</details>

### datasetUsageStatistics (Timeseries)
Stats corresponding to dataset's usage.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetUsageStatistics",
    "type": "timeseries"
  },
  "name": "DatasetUsageStatistics",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "hasValuesFieldName": "hasUniqueUserCount"
      },
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "uniqueUserCount",
      "default": null,
      "doc": "Unique user count"
    },
    {
      "Searchable": {
        "fieldType": "COUNT",
        "hasValuesFieldName": "hasTotalSqlQueriesCount"
      },
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "totalSqlQueries",
      "default": null,
      "doc": "Total SQL query count"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "topSqlQueries",
      "default": null,
      "doc": "Frequent SQL queries; mostly makes sense for datasets in SQL databases"
    },
    {
      "TimeseriesFieldCollection": {
        "key": "user"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DatasetUserUsageCounts",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "user",
                "doc": "The unique id of the user."
              },
              {
                "TimeseriesField": {},
                "type": "int",
                "name": "count",
                "doc": "Number of times the dataset has been used by the user."
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "string"
                ],
                "name": "userEmail",
                "default": null,
                "doc": "If user_email is set, we attempt to resolve the user's urn upon ingest"
              }
            ],
            "doc": "Records a single user's usage counts for a given resource"
          }
        }
      ],
      "name": "userCounts",
      "default": null,
      "doc": "Users within this bucket, with frequency counts"
    },
    {
      "TimeseriesFieldCollection": {
        "key": "fieldPath"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DatasetFieldUsageCounts",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": "string",
                "name": "fieldPath",
                "doc": "The name of the field."
              },
              {
                "TimeseriesField": {},
                "type": "int",
                "name": "count",
                "doc": "Number of times the field has been used."
              }
            ],
            "doc": "Records field-level usage counts for a given dataset"
          }
        }
      ],
      "name": "fieldCounts",
      "default": null,
      "doc": "Field-level usage stats"
    }
  ],
  "doc": "Stats corresponding to dataset's usage."
}
```
</details>

### operation (Timeseries)
Operational info for an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "operation",
    "type": "timeseries"
  },
  "name": "Operation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "TimeseriesField": {},
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "actor",
      "default": null,
      "doc": "Actor who issued this operation."
    },
    {
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALTER": "Asset was altered",
          "CREATE": "Asset was created",
          "CUSTOM": "Custom asset operation. If this is set, ensure customOperationType is filled out.",
          "DELETE": "Rows were deleted",
          "DROP": "Asset was dropped",
          "INSERT": "Rows were inserted",
          "UPDATE": "Rows were updated"
        },
        "name": "OperationType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "INSERT",
          "UPDATE",
          "DELETE",
          "CREATE",
          "ALTER",
          "DROP",
          "CUSTOM",
          "UNKNOWN"
        ],
        "doc": "Enum to define the operation type when an entity changes."
      },
      "name": "operationType",
      "doc": "Operation type of change."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "string"
      ],
      "name": "customOperationType",
      "default": null,
      "doc": "A custom type of operation. Required if operationType is CUSTOM."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "long"
      ],
      "name": "numAffectedRows",
      "default": null,
      "doc": "How many rows were affected by this operation."
    },
    {
      "TimeseriesFieldCollection": {
        "key": "datasetName"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "affectedDatasets",
      "default": null,
      "doc": "Which other datasets were affected by this operation."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "DATA_PLATFORM": "Rows were updated",
            "DATA_PROCESS": "Provided by a Data Process"
          },
          "name": "OperationSourceType",
          "namespace": "com.linkedin.common",
          "symbols": [
            "DATA_PROCESS",
            "DATA_PLATFORM"
          ],
          "doc": "The source of an operation"
        }
      ],
      "name": "sourceType",
      "default": null,
      "doc": "Source Type"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "customProperties",
      "default": null,
      "doc": "Custom properties"
    },
    {
      "Searchable": {
        "fieldName": "lastOperationTime",
        "fieldType": "DATETIME"
      },
      "TimeseriesField": {
        "fieldType": "DATETIME"
      },
      "type": "long",
      "name": "lastUpdatedTimestamp",
      "doc": "The time at which the operation occurred. Would be better named 'operationTime'"
    },
    {
      "TimeseriesFieldCollection": {
        "key": "query"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "queries",
      "default": null,
      "doc": "Which queries were used in this operation."
    }
  ],
  "doc": "Operational info for an entity."
}
```
</details>

### datasetDeprecation (Deprecated)
Dataset deprecation status
Deprecated! This aspect is deprecated in favor of the more-general-purpose 'Deprecation' aspect.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datasetDeprecation"
  },
  "Deprecated": true,
  "name": "DatasetDeprecation",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the dataset is deprecated by owner."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this dataset."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the dataset deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "actor",
      "default": null,
      "doc": "The corpuser URN which will be credited for modifying this deprecation content."
    }
  ],
  "doc": "Dataset deprecation status\nDeprecated! This aspect is deprecated in favor of the more-general-purpose 'Deprecation' aspect."
}
```
</details>

## Relationships

### Self
These are the relationships to itself, stored in this entity's aspects
- DownstreamOf (via `upstreamLineage.upstreams.dataset`)
- DownstreamOf (via `upstreamLineage.fineGrainedLineages`)
- ForeignKeyToDataset (via `schemaMetadata.foreignKeys.foreignDataset`)
- SiblingOf (via `siblings.siblings`)
- PhysicalInstanceOf (via `logicalParent.parent`)
### Outgoing
These are the relationships stored in this entity's aspects
- DownstreamOf

   - SchemaField via `upstreamLineage.fineGrainedLineages`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- SchemaFieldTaggedWith

   - Tag via `schemaMetadata.fields.globalTags`
- TaggedWith

   - Tag via `schemaMetadata.fields.globalTags.tags`
   - Tag via `editableSchemaMetadata.editableSchemaFieldInfo.globalTags.tags`
   - Tag via `globalTags.tags`
- SchemaFieldWithGlossaryTerm

   - GlossaryTerm via `schemaMetadata.fields.glossaryTerms`
- TermedWith

   - GlossaryTerm via `schemaMetadata.fields.glossaryTerms.terms.urn`
   - GlossaryTerm via `editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms.terms.urn`
   - GlossaryTerm via `glossaryTerms.terms.urn`
- ForeignKeyTo

   - SchemaField via `schemaMetadata.foreignKeys.foreignFields`
- EditableSchemaFieldTaggedWith

   - Tag via `editableSchemaMetadata.editableSchemaFieldInfo.globalTags`
- EditableSchemaFieldWithGlossaryTerm

   - GlossaryTerm via `editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
   - Role via `access.roles.urn`
- IsPartOf

   - Container via `container.container`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
- VersionOf

   - VersionSet via `versionProperties.versionSet`
- PhysicalInstanceOf

   - SchemaField via `logicalParent.parent`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
