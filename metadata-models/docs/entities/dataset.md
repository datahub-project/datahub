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
{{ inline /metadata-ingestion/examples/library/dataset_schema.py show_path_as_comment }}
```
</details>


### Tags and Glossary Terms

Datasets can have Tags or Terms attached to them. Read [this blog](https://blog.datahubproject.io/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms so you understand when you should use which.

#### Adding Tags or Glossary Terms at the top-level to a dataset

At the top-level, tags are added to datasets using the `globalTags` aspect, while terms are added using the `glossaryTerms` aspect.

Here is an example for how to add a tag to a dataset. Note that this involves reading the currently set tags on the dataset and then adding a new one if needed.

<details>
<summary>Python SDK: Add a tag to a dataset at the top-level</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_tag.py show_path_as_comment }}
```
</details>

Here is an example of adding a term to a dataset. Note that this involves reading the currently set terms on the dataset and then adding a new one if needed.
<details>
<summary>Python SDK: Add a term to a dataset at the top-level</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_term.py show_path_as_comment }}
```
</details>

#### Adding Tags or Glossary Terms to columns / fields of a dataset

Tags and Terms can also be attached to an individual column (field) of a dataset. These attachments are done via the `schemaMetadata` aspect by ingestion connectors / transformers and via the `editableSchemaMetadata` aspect by the UI.
This separation allows the writes from the replication of metadata from the source system to be isolated from the edits made in the UI.

Here is an example of how you can add a tag to a field in a dataset using the low-level Python SDK.

<details>
<summary>Python SDK: Add a tag to a column (field) of a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_column_tag.py show_path_as_comment }}
```
</details>

Similarly, here is an example of how you would add a term to a field in a dataset using the low-level Python SDK. 
<details>
<summary>Python SDK: Add a term to a column (field) of a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_column_term.py show_path_as_comment }}
```
</details>

### Ownership

Ownership is associated to a dataset using the `ownership` aspect. Owners can be of a few different types, `DATAOWNER`, `PRODUCER`, `DEVELOPER`, `CONSUMER`, etc. See [OwnershipType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-models/src/main/pegasus/com/linkedin/common/OwnershipType.pdl) for the full list of ownership types and their meanings. Ownership can be inherited from source systems, or additionally added in DataHub using the UI. Ingestion connectors for sources will automatically set owners when the source system supports it.

#### Adding Owners

The following script shows you how to add an owner to a dataset using the low-level Python SDK.

<details>
<summary>Python SDK: Add an owner to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_owner.py show_path_as_comment }}
```
</details>

### Fine-grained lineage
Fine-grained lineage at field level can be associated to a dataset in two ways - either directly attached to the `upstreamLineage` aspect of a dataset, or captured as part of the `dataJobInputOutput` aspect of a dataJob.

<details>
<summary>Python SDK: Add fine-grained lineage to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py show_path_as_comment }}
```
</details>

<details>
<summary>Python SDK: Add fine-grained lineage to a datajob</summary>

```python
{{ inline /metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py show_path_as_comment }}
```
</details>

#### Querying lineage information
The standard [GET APIs to retrieve entities](https://datahubproject.io/docs/metadata-service/#retrieving-entities) can be used to fetch the dataset/datajob created by the above example.
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
{{ inline /metadata-ingestion/examples/library/dataset_add_documentation.py show_path_as_comment }}
```
</details>

## Notable Exceptions

The following overloaded uses of the Dataset entity exist for convenience, but will likely move to fully modeled entity types in the future. 
- OpenAPI endpoints: the GET API of OpenAPI endpoints are currently modeled as Datasets, but should really be modeled as a Service/API entity once this is created in the metadata model.
- DataHub's Logical Entities (e.g.. Dataset, Chart, Dashboard) are represented as Datasets, with sub-type Entity. These should really be modeled as Entities in a logical ER model once this is created in the metadata model.