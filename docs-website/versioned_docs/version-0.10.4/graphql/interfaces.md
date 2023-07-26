---
id: interfaces
title: Interfaces
slug: interfaces
sidebar_position: 4
---

## Aspect

A versioned aspect, or single group of related metadata, associated with an Entity and having a unique version

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [SchemaMetadata](/docs/graphql/objects#schemametadata)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The version of the aspect, where zero represents the latest version</p>
</td>
</tr>
</tbody>
</table>

## BrowsableEntity

A Metadata Entity which is browsable, or has browse paths.

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [Dataset](/docs/graphql/objects#dataset)
- [Notebook](/docs/graphql/objects#notebook)
- [Dashboard](/docs/graphql/objects#dashboard)
- [Chart](/docs/graphql/objects#chart)
- [DataFlow](/docs/graphql/objects#dataflow)
- [DataJob](/docs/graphql/objects#datajob)
- [MLModel](/docs/graphql/objects#mlmodel)
- [MLModelGroup](/docs/graphql/objects#mlmodelgroup)
- [MLFeatureTable](/docs/graphql/objects#mlfeaturetable)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to an entity. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
</tbody>
</table>

## Entity

A top level Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [AccessTokenMetadata](/docs/graphql/objects#accesstokenmetadata)
- [Dataset](/docs/graphql/objects#dataset)
- [Role](/docs/graphql/objects#role)
- [VersionedDataset](/docs/graphql/objects#versioneddataset)
- [GlossaryTerm](/docs/graphql/objects#glossaryterm)
- [GlossaryNode](/docs/graphql/objects#glossarynode)
- [DataPlatform](/docs/graphql/objects#dataplatform)
- [DataPlatformInstance](/docs/graphql/objects#dataplatforminstance)
- [Container](/docs/graphql/objects#container)
- [SchemaFieldEntity](/docs/graphql/objects#schemafieldentity)
- [CorpUser](/docs/graphql/objects#corpuser)
- [CorpGroup](/docs/graphql/objects#corpgroup)
- [Tag](/docs/graphql/objects#tag)
- [Notebook](/docs/graphql/objects#notebook)
- [Dashboard](/docs/graphql/objects#dashboard)
- [Chart](/docs/graphql/objects#chart)
- [DataFlow](/docs/graphql/objects#dataflow)
- [DataJob](/docs/graphql/objects#datajob)
- [DataProcessInstance](/docs/graphql/objects#dataprocessinstance)
- [Assertion](/docs/graphql/objects#assertion)
- [DataHubPolicy](/docs/graphql/objects#datahubpolicy)
- [MLModel](/docs/graphql/objects#mlmodel)
- [MLModelGroup](/docs/graphql/objects#mlmodelgroup)
- [MLFeature](/docs/graphql/objects#mlfeature)
- [MLPrimaryKey](/docs/graphql/objects#mlprimarykey)
- [MLFeatureTable](/docs/graphql/objects#mlfeaturetable)
- [Domain](/docs/graphql/objects#domain)
- [DataHubRole](/docs/graphql/objects#datahubrole)
- [Post](/docs/graphql/objects#post)
- [DataHubView](/docs/graphql/objects#datahubview)
- [QueryEntity](/docs/graphql/objects#queryentity)
- [DataProduct](/docs/graphql/objects#dataproduct)
- [OwnershipTypeEntity](/docs/graphql/objects#ownershiptypeentity)
- [Test](/docs/graphql/objects#test)
- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A primary key of the Metadata Entity</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>A standard Entity Type</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>List of relationships between the source Entity and some destination entities with a given types</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#relationshipsinput"><code>RelationshipsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

</td>
</tr>
</tbody>
</table>

## EntityWithRelationships

Deprecated, use relationships field instead

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [Dataset](/docs/graphql/objects#dataset)
- [Dashboard](/docs/graphql/objects#dashboard)
- [Chart](/docs/graphql/objects#chart)
- [DataFlow](/docs/graphql/objects#dataflow)
- [DataJob](/docs/graphql/objects#datajob)
- [DataProcessInstance](/docs/graphql/objects#dataprocessinstance)
- [Assertion](/docs/graphql/objects#assertion)
- [MLModel](/docs/graphql/objects#mlmodel)
- [MLModelGroup](/docs/graphql/objects#mlmodelgroup)
- [MLFeature](/docs/graphql/objects#mlfeature)
- [MLPrimaryKey](/docs/graphql/objects#mlprimarykey)
- [MLFeatureTable](/docs/graphql/objects#mlfeaturetable)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A primary key associated with the Metadata Entity</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>A standard Entity Type</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from this entity</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#relationshipsinput"><code>RelationshipsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

</td>
</tr>
<tr>
<td>
lineage<br />
<a href="/docs/graphql/objects#entitylineageresult"><code>EntityLineageResult</code></a>
</td>
<td>
<p>Edges extending from this entity grouped by direction in the lineage graph</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#lineageinput"><code>LineageInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

</td>
</tr>
</tbody>
</table>

## TimeSeriesAspect

A time series aspect, or a group of related metadata associated with an Entity and corresponding to a particular timestamp

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [DataProcessRunEvent](/docs/graphql/objects#dataprocessrunevent)
- [DashboardUsageMetrics](/docs/graphql/objects#dashboardusagemetrics)
- [DatasetProfile](/docs/graphql/objects#datasetprofile)
- [AssertionRunEvent](/docs/graphql/objects#assertionrunevent)
- [Operation](/docs/graphql/objects#operation)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
timestampMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The timestamp associated with the time series aspect in milliseconds</p>
</td>
</tr>
</tbody>
</table>
