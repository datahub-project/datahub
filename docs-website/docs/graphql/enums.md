---
id: enums
title: Enums
slug: enums
sidebar_position: 5
---

## AccessLevel

The access level for a Metadata Entity, either public or private

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>PUBLIC</td>
<td>
<p>Publicly available</p>
</td>
</tr>
<tr>
<td>PRIVATE</td>
<td>
<p>Restricted to a subset of viewers</p>
</td>
</tr>
</tbody>
</table>

## ChartQueryType

The type of the Chart Query

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SQL</td>
<td>
<p>Standard ANSI SQL</p>
</td>
</tr>
<tr>
<td>LOOKML</td>
<td>
<p>LookML</p>
</td>
</tr>
</tbody>
</table>

## ChartType

The type of a Chart Entity

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>BAR</td>
<td>
<p>Bar graph</p>
</td>
</tr>
<tr>
<td>PIE</td>
<td>
<p>Pie chart</p>
</td>
</tr>
<tr>
<td>SCATTER</td>
<td>
<p>Scatter plot</p>
</td>
</tr>
<tr>
<td>TABLE</td>
<td>
<p>Table</p>
</td>
</tr>
<tr>
<td>TEXT</td>
<td>
<p>Markdown formatted text</p>
</td>
</tr>
<tr>
<td>LINE</td>
<td>
<p>A line chart</p>
</td>
</tr>
<tr>
<td>AREA</td>
<td>
<p>An area chart</p>
</td>
</tr>
<tr>
<td>HISTOGRAM</td>
<td>
<p>A histogram chart</p>
</td>
</tr>
<tr>
<td>BOX_PLOT</td>
<td>
<p>A box plot chart</p>
</td>
</tr>
</tbody>
</table>

## CostType



<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ORG_COST_TYPE</td>
<td>
<p>Org Cost Type to which the Cost of this entity should be attributed to</p>
</td>
</tr>
</tbody>
</table>

## DatasetLineageType

Deprecated
The type of an edge between two Datasets

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>COPY</td>
<td>
<p>Direct copy without modification</p>
</td>
</tr>
<tr>
<td>TRANSFORMED</td>
<td>
<p>Transformed dataset</p>
</td>
</tr>
<tr>
<td>VIEW</td>
<td>
<p>Represents a view defined on the sources</p>
</td>
</tr>
</tbody>
</table>

## EntityType

A top level Metadata Entity Type

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DATASET</td>
<td>
<p>The Dataset Entity</p>
</td>
</tr>
<tr>
<td>CORP_USER</td>
<td>
<p>The CorpUser Entity</p>
</td>
</tr>
<tr>
<td>CORP_GROUP</td>
<td>
<p>The CorpGroup Entity</p>
</td>
</tr>
<tr>
<td>DATA_PLATFORM</td>
<td>
<p>The DataPlatform Entity</p>
</td>
</tr>
<tr>
<td>DASHBOARD</td>
<td>
<p>The Dashboard Entity</p>
</td>
</tr>
<tr>
<td>CHART</td>
<td>
<p>The Chart Entity</p>
</td>
</tr>
<tr>
<td>DATA_FLOW</td>
<td>
<p>The Data Flow (or Data Pipeline) Entity,</p>
</td>
</tr>
<tr>
<td>DATA_JOB</td>
<td>
<p>The Data Job (or Data Task) Entity</p>
</td>
</tr>
<tr>
<td>TAG</td>
<td>
<p>The Tag Entity</p>
</td>
</tr>
<tr>
<td>GLOSSARY_TERM</td>
<td>
<p>The Glossary Term Entity</p>
</td>
</tr>
<tr>
<td>MLMODEL</td>
<td>
<p>The ML Model Entity</p>
</td>
</tr>
<tr>
<td>MLMODEL_GROUP</td>
<td>
<p>The MLModelGroup Entity</p>
</td>
</tr>
<tr>
<td>MLFEATURE_TABLE</td>
<td>
<p>ML Feature Table Entity</p>
</td>
</tr>
<tr>
<td>MLFEATURE</td>
<td>
<p>The ML Feature Entity</p>
</td>
</tr>
<tr>
<td>MLPRIMARY_KEY</td>
<td>
<p>The ML Primary Key Entity</p>
</td>
</tr>
</tbody>
</table>

## FabricType

An environment identifier for a particular Entity, ie staging or production
Note that this model will soon be deprecated in favor of a more general purpose of notion
of data environment

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DEV</td>
<td>
<p>Designates development fabrics</p>
</td>
</tr>
<tr>
<td>EI</td>
<td>
<p>Designates early integration or staging fabrics</p>
</td>
</tr>
<tr>
<td>PROD</td>
<td>
<p>Designates production fabrics</p>
</td>
</tr>
<tr>
<td>CORP</td>
<td>
<p>Designates corporation fabrics</p>
</td>
</tr>
</tbody>
</table>

## IntendedUserType



<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ENTERPRISE</td>
<td>
<p>Developed for Enterprise Users</p>
</td>
</tr>
<tr>
<td>HOBBY</td>
<td>
<p>Developed for Hobbyists</p>
</td>
</tr>
<tr>
<td>ENTERTAINMENT</td>
<td>
<p>Developed for Entertainment Purposes</p>
</td>
</tr>
</tbody>
</table>

## MLFeatureDataType

The data type associated with an individual Machine Learning Feature

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>USELESS</td>
<td>

</td>
</tr>
<tr>
<td>NOMINAL</td>
<td>

</td>
</tr>
<tr>
<td>ORDINAL</td>
<td>

</td>
</tr>
<tr>
<td>BINARY</td>
<td>

</td>
</tr>
<tr>
<td>COUNT</td>
<td>

</td>
</tr>
<tr>
<td>TIME</td>
<td>

</td>
</tr>
<tr>
<td>INTERVAL</td>
<td>

</td>
</tr>
<tr>
<td>IMAGE</td>
<td>

</td>
</tr>
<tr>
<td>VIDEO</td>
<td>

</td>
</tr>
<tr>
<td>AUDIO</td>
<td>

</td>
</tr>
<tr>
<td>TEXT</td>
<td>

</td>
</tr>
<tr>
<td>MAP</td>
<td>

</td>
</tr>
<tr>
<td>SEQUENCE</td>
<td>

</td>
</tr>
<tr>
<td>SET</td>
<td>

</td>
</tr>
<tr>
<td>CONTINUOUS</td>
<td>

</td>
</tr>
<tr>
<td>BYTE</td>
<td>

</td>
</tr>
<tr>
<td>UNKNOWN</td>
<td>

</td>
</tr>
</tbody>
</table>

## OwnershipSourceType

The origin of Ownership metadata associated with a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>AUDIT</td>
<td>
<p>Auditing system or audit logs</p>
</td>
</tr>
<tr>
<td>DATABASE</td>
<td>
<p>Database, eg GRANTS table</p>
</td>
</tr>
<tr>
<td>FILE_SYSTEM</td>
<td>
<p>File system, eg file or directory owner</p>
</td>
</tr>
<tr>
<td>ISSUE_TRACKING_SYSTEM</td>
<td>
<p>Issue tracking system, eg Jira</p>
</td>
</tr>
<tr>
<td>MANUAL</td>
<td>
<p>Manually provided by a user</p>
</td>
</tr>
<tr>
<td>SERVICE</td>
<td>
<p>Other ownership like service, eg Nuage, ACL service etc</p>
</td>
</tr>
<tr>
<td>SOURCE_CONTROL</td>
<td>
<p>SCM system, eg GIT, SVN</p>
</td>
</tr>
<tr>
<td>OTHER</td>
<td>
<p>Other sources</p>
</td>
</tr>
</tbody>
</table>

## OwnershipType

The type of the ownership relationship between a Person and a Metadata Entity
Note that this field will soon become deprecated due to low usage

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DEVELOPER</td>
<td>
<p>A person or group that is in charge of developing the code</p>
</td>
</tr>
<tr>
<td>DATAOWNER</td>
<td>
<p>A person or group that is owning the data</p>
</td>
</tr>
<tr>
<td>DELEGATE</td>
<td>
<p>A person or a group that overseas the operation, eg a DBA or SRE</p>
</td>
</tr>
<tr>
<td>PRODUCER</td>
<td>
<p>A person, group, or service that produces or generates the data</p>
</td>
</tr>
<tr>
<td>CONSUMER</td>
<td>
<p>A person, group, or service that consumes the data</p>
</td>
</tr>
<tr>
<td>STAKEHOLDER</td>
<td>
<p>A person or a group that has direct business interest</p>
</td>
</tr>
</tbody>
</table>

## PlatformNativeType

Deprecated, do not use this type
The logical type associated with an individual Dataset

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>TABLE</td>
<td>
<p>Table</p>
</td>
</tr>
<tr>
<td>VIEW</td>
<td>
<p>View</p>
</td>
</tr>
<tr>
<td>DIRECTORY</td>
<td>
<p>Directory in file system</p>
</td>
</tr>
<tr>
<td>STREAM</td>
<td>
<p>Stream</p>
</td>
</tr>
<tr>
<td>BUCKET</td>
<td>
<p>Bucket in key value store</p>
</td>
</tr>
</tbody>
</table>

## PlatformType

The category of a specific Data Platform

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>FILE_SYSTEM</td>
<td>
<p>Value for a file system</p>
</td>
</tr>
<tr>
<td>KEY_VALUE_STORE</td>
<td>
<p>Value for a key value store</p>
</td>
</tr>
<tr>
<td>MESSAGE_BROKER</td>
<td>
<p>Value for a message broker</p>
</td>
</tr>
<tr>
<td>OBJECT_STORE</td>
<td>
<p>Value for an object store</p>
</td>
</tr>
<tr>
<td>OLAP_DATASTORE</td>
<td>
<p>Value for an OLAP datastore</p>
</td>
</tr>
<tr>
<td>QUERY_ENGINE</td>
<td>
<p>Value for a query engine</p>
</td>
</tr>
<tr>
<td>RELATIONAL_DB</td>
<td>
<p>Value for a relational database</p>
</td>
</tr>
<tr>
<td>SEARCH_ENGINE</td>
<td>
<p>Value for a search engine</p>
</td>
</tr>
<tr>
<td>OTHERS</td>
<td>
<p>Value for other platforms</p>
</td>
</tr>
</tbody>
</table>

## PolicyState

The state of an Access Policy

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DRAFT</td>
<td>
<p>A Policy that has not been officially created, but in progress
Currently unused</p>
</td>
</tr>
<tr>
<td>ACTIVE</td>
<td>
<p>A Policy that is active and being enforced</p>
</td>
</tr>
<tr>
<td>INACTIVE</td>
<td>
<p>A Policy that is not active or being enforced</p>
</td>
</tr>
</tbody>
</table>

## PolicyType

The type of the Access Policy

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>METADATA</td>
<td>
<p>An access policy that grants privileges pertaining to Metadata Entities</p>
</td>
</tr>
<tr>
<td>PLATFORM</td>
<td>
<p>An access policy that grants top level administrative privileges pertaining to the DataHub Platform itself</p>
</td>
</tr>
</tbody>
</table>

## RelationshipDirection

Direction between a source and destination node

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>INCOMING</td>
<td>
<p>A directed edge pointing at the source Entity</p>
</td>
</tr>
<tr>
<td>OUTGOING</td>
<td>
<p>A directed edge pointing at the destination Entity</p>
</td>
</tr>
</tbody>
</table>

## SchemaFieldDataType

The type associated with a single Dataset schema field

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>BOOLEAN</td>
<td>
<p>A boolean type</p>
</td>
</tr>
<tr>
<td>FIXED</td>
<td>
<p>A fixed bytestring type</p>
</td>
</tr>
<tr>
<td>STRING</td>
<td>
<p>A string type</p>
</td>
</tr>
<tr>
<td>BYTES</td>
<td>
<p>A string of bytes</p>
</td>
</tr>
<tr>
<td>NUMBER</td>
<td>
<p>A number, including integers, floats, and doubles</p>
</td>
</tr>
<tr>
<td>DATE</td>
<td>
<p>A datestrings type</p>
</td>
</tr>
<tr>
<td>TIME</td>
<td>
<p>A timestamp type</p>
</td>
</tr>
<tr>
<td>ENUM</td>
<td>
<p>An enum type</p>
</td>
</tr>
<tr>
<td>NULL</td>
<td>
<p>A NULL type</p>
</td>
</tr>
<tr>
<td>MAP</td>
<td>
<p>A map collection type</p>
</td>
</tr>
<tr>
<td>ARRAY</td>
<td>
<p>An array collection type</p>
</td>
</tr>
<tr>
<td>UNION</td>
<td>
<p>An union type</p>
</td>
</tr>
<tr>
<td>STRUCT</td>
<td>
<p>An complex struct type</p>
</td>
</tr>
</tbody>
</table>

## SourceCodeUrlType



<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ML_MODEL_SOURCE_CODE</td>
<td>
<p>MLModel Source Code</p>
</td>
</tr>
<tr>
<td>TRAINING_PIPELINE_SOURCE_CODE</td>
<td>
<p>Training Pipeline Source Code</p>
</td>
</tr>
<tr>
<td>EVALUATION_PIPELINE_SOURCE_CODE</td>
<td>
<p>Evaluation Pipeline Source Code</p>
</td>
</tr>
</tbody>
</table>

## SubResourceType

A type of Metadata Entity sub resource

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DATASET_FIELD</td>
<td>
<p>A Dataset field or column</p>
</td>
</tr>
</tbody>
</table>

## TimeRange

A time range used in fetching Dataset Usage statistics

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DAY</td>
<td>
<p>Last day</p>
</td>
</tr>
<tr>
<td>WEEK</td>
<td>
<p>Last week</p>
</td>
</tr>
<tr>
<td>MONTH</td>
<td>
<p>Last month</p>
</td>
</tr>
<tr>
<td>QUARTER</td>
<td>
<p>Last quarter</p>
</td>
</tr>
<tr>
<td>YEAR</td>
<td>
<p>Last year</p>
</td>
</tr>
<tr>
<td>ALL</td>
<td>
<p>All time</p>
</td>
</tr>
</tbody>
</table>

## WindowDuration

The duration of a fixed window of time

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DAY</td>
<td>
<p>A one day window</p>
</td>
</tr>
<tr>
<td>WEEK</td>
<td>
<p>A one week window</p>
</td>
</tr>
<tr>
<td>MONTH</td>
<td>
<p>A one month window</p>
</td>
</tr>
<tr>
<td>YEAR</td>
<td>
<p>A one year window</p>
</td>
</tr>
</tbody>
</table>

