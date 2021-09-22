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

## Entity

A top level Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [Dataset](/docs/graphql/objects#dataset)
- [GlossaryTerm](/docs/graphql/objects#glossaryterm)
- [DataPlatform](/docs/graphql/objects#dataplatform)
- [CorpUser](/docs/graphql/objects#corpuser)
- [CorpGroup](/docs/graphql/objects#corpgroup)
- [Tag](/docs/graphql/objects#tag)
- [Dashboard](/docs/graphql/objects#dashboard)
- [Chart](/docs/graphql/objects#chart)
- [DataFlow](/docs/graphql/objects#dataflow)
- [DataJob](/docs/graphql/objects#datajob)
- [MLModel](/docs/graphql/objects#mlmodel)
- [MLModelGroup](/docs/graphql/objects#mlmodelgroup)
- [MLFeature](/docs/graphql/objects#mlfeature)
- [MLPrimaryKey](/docs/graphql/objects#mlprimarykey)
- [MLFeatureTable](/docs/graphql/objects#mlfeaturetable)
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
- [DataJob](/docs/graphql/objects#datajob)
- [MLModel](/docs/graphql/objects#mlmodel)
- [MLModelGroup](/docs/graphql/objects#mlmodelgroup)

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
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<p>Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<p>Entities downstream of the given entity</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Edges extending from this entity</p>

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

## TimeSeriesAspect

A time series aspect, or a group of related metadata associated with an Entity and corresponding to a particular timestamp

<p style={{ marginBottom: "0.4em" }}><strong>Implemented by</strong></p>

- [DatasetProfile](/docs/graphql/objects#datasetprofile)

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

