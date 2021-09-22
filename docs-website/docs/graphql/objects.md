---
id: objects
title: Objects
slug: objects
sidebar_position: 3
---

## ActorFilter

The actors that a DataHub Access Policy applies to

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
users<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>A disjunctive set of users to apply the policy to</p>
</td>
</tr>
<tr>
<td>
groups<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>A disjunctive set of groups to apply the policy to</p>
</td>
</tr>
<tr>
<td>
resourceOwners<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the filter should return TRUE for owners of a particular resource
Only applies to policies of type METADATA, which have a resource associated with them</p>
</td>
</tr>
<tr>
<td>
allUsers<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the filter should apply to all users</p>
</td>
</tr>
<tr>
<td>
allGroups<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the filter should apply to all groups</p>
</td>
</tr>
</tbody>
</table>

## AggregationMetadata

Information about the aggregation that can be used for filtering, included the field value and number of results

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A particular value of a facet field</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The number of search results containing the value</p>
</td>
</tr>
</tbody>
</table>

## AnalyticsConfig

Configurations related to the Analytics Feature

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
enabled<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the Analytics feature is enabled and should be displayed</p>
</td>
</tr>
</tbody>
</table>

## AppConfig

Config loaded at application boot time
This configuration dictates the behavior of the UI, such as which features are enabled or disabled

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
analyticsConfig<br />
<a href="/docs/graphql/objects#analyticsconfig"><code>AnalyticsConfig!</code></a>
</td>
<td>
<p>Configurations related to the Analytics Feature</p>
</td>
</tr>
<tr>
<td>
policiesConfig<br />
<a href="/docs/graphql/objects#policiesconfig"><code>PoliciesConfig!</code></a>
</td>
<td>
<p>Configurations related to the Policies Feature</p>
</td>
</tr>
</tbody>
</table>

## AuditStamp

A time stamp along with an optional actor

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
time<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>When the audited action took place</p>
</td>
</tr>
<tr>
<td>
actor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Who performed the audited action</p>
</td>
</tr>
</tbody>
</table>

## AuthenticatedUser

Information about the currently authenticated user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
corpUser<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser!</code></a>
</td>
<td>
<p>The user information associated with the authenticated user, including properties used in rendering the profile</p>
</td>
</tr>
<tr>
<td>
platformPrivileges<br />
<a href="/docs/graphql/objects#platformprivileges"><code>PlatformPrivileges!</code></a>
</td>
<td>
<p>The privileges assigned to the currently authenticated user, which dictates which parts of the UI they should be able to use</p>
</td>
</tr>
</tbody>
</table>

## AutoCompleteMultipleResults

The results returned on a multi entity autocomplete query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The raw query string</p>
</td>
</tr>
<tr>
<td>
suggestions<br />
<a href="/docs/graphql/objects#autocompleteresultforentity"><code>[AutoCompleteResultForEntity!]!</code></a>
</td>
<td>
<p>The autocompletion suggestions</p>
</td>
</tr>
</tbody>
</table>

## AutoCompleteResultForEntity

An individual auto complete result specific to an individual Metadata Entity Type

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>Entity type</p>
</td>
</tr>
<tr>
<td>
suggestions<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The autocompletion results for specified entity type</p>
</td>
</tr>
</tbody>
</table>

## AutoCompleteResults

The results returned on a single entity autocomplete query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The query string</p>
</td>
</tr>
<tr>
<td>
suggestions<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The autocompletion results</p>
</td>
</tr>
</tbody>
</table>

## BaseData



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
dataset<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Dataset used for the Training or Evaluation of the MLModel</p>
</td>
</tr>
<tr>
<td>
motivation<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Motivation to pick these datasets</p>
</td>
</tr>
<tr>
<td>
preProcessing<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Details of Data Proprocessing</p>
</td>
</tr>
</tbody>
</table>

## BooleanBox



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
booleanValue<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## BrowsePath

A hierarchical entity path

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
path<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The components of the browse path</p>
</td>
</tr>
</tbody>
</table>

## BrowseResultGroup

A group of Entities under a given browse path

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The path name of a group of browse results</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The number of entities within the group</p>
</td>
</tr>
</tbody>
</table>

## BrowseResultMetadata

Metadata about the Browse Paths response

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
path<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The provided path</p>
</td>
</tr>
<tr>
<td>
totalNumEntities<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The total number of entities under the provided browse path</p>
</td>
</tr>
</tbody>
</table>

## BrowseResults

The results of a browse path traversal query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entities<br />
<a href="/docs/graphql/interfaces#entity"><code>[Entity!]!</code></a>
</td>
<td>
<p>The browse results</p>
</td>
</tr>
<tr>
<td>
groups<br />
<a href="/docs/graphql/objects#browseresultgroup"><code>[BrowseResultGroup!]!</code></a>
</td>
<td>
<p>The groups present at the provided browse path</p>
</td>
</tr>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The starting point of paginated results</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of elements included in the results</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of browse results under the path with filters applied</p>
</td>
</tr>
<tr>
<td>
metadata<br />
<a href="/docs/graphql/objects#browseresultmetadata"><code>BrowseResultMetadata!</code></a>
</td>
<td>
<p>Metadata containing resulting browse groups</p>
</td>
</tr>
</tbody>
</table>

## CaveatDetails



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
needsFurtherTesting<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Did the results suggest any further testing</p>
</td>
</tr>
<tr>
<td>
caveatDescription<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Caveat Description</p>
</td>
</tr>
<tr>
<td>
groupsNotRepresented<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Relevant groups that were not represented in the evaluation dataset</p>
</td>
</tr>
</tbody>
</table>

## CaveatsAndRecommendations



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
caveats<br />
<a href="/docs/graphql/objects#caveatdetails"><code>CaveatDetails</code></a>
</td>
<td>
<p>Caveats on using this MLModel</p>
</td>
</tr>
<tr>
<td>
recommendations<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Recommendations on where this MLModel should be used</p>
</td>
</tr>
<tr>
<td>
idealDatasetCharacteristics<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Ideal characteristics of an evaluation dataset for this MLModel</p>
</td>
</tr>
</tbody>
</table>

## Chart

A Chart Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the Chart</p>
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
tool<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The chart tool name
Note that this field will soon be deprecated in favor a unified notion of Data Platform</p>
</td>
</tr>
<tr>
<td>
chartId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>An id unique within the charting tool</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#chartproperties"><code>ChartProperties</code></a>
</td>
<td>
<p>Additional read only properties about the Chart</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#charteditableproperties"><code>ChartEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties about the Chart</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/objects#chartquery"><code>ChartQuery</code></a>
</td>
<td>
<p>Info about the query which is used to render the chart</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the chart</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the chart</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the chart</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#chartinfo"><code>ChartInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only information about the chart</p>
</td>
</tr>
<tr>
<td>
editableInfo<br />
<a href="/docs/graphql/objects#charteditableproperties"><code>ChartEditableProperties</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use editableProperties field instead
Additional read write information about the Chart</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags instead
The structured tags associated with the chart</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities downstream of the given entity</p>
</td>
</tr>
</tbody>
</table>

## ChartEditableProperties

Chart properties that are editable via the UI This represents logical metadata,
as opposed to technical metadata

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Chart</p>
</td>
</tr>
</tbody>
</table>

## ChartInfo

Deprecated, use ChartProperties instead
Additional read only information about the chart

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name of the chart</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the chart</p>
</td>
</tr>
<tr>
<td>
inputs<br />
<a href="/docs/graphql/objects#dataset"><code>[Dataset!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship Consumes instead
Data sources for the chart</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the chart</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#charttype"><code>ChartType</code></a>
</td>
<td>
<p>Access level for the chart</p>
</td>
</tr>
<tr>
<td>
access<br />
<a href="/docs/graphql/enums#accesslevel"><code>AccessLevel</code></a>
</td>
<td>
<p>Access level for the chart</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
<tr>
<td>
lastRefreshed<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time when this chart last refreshed</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation of this chart</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the modification of this chart</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An optional AuditStamp corresponding to the deletion of this chart</p>
</td>
</tr>
</tbody>
</table>

## ChartProperties

Additional read only properties about the chart

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name of the chart</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the chart</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the chart</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#charttype"><code>ChartType</code></a>
</td>
<td>
<p>Access level for the chart</p>
</td>
</tr>
<tr>
<td>
access<br />
<a href="/docs/graphql/enums#accesslevel"><code>AccessLevel</code></a>
</td>
<td>
<p>Access level for the chart</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
<tr>
<td>
lastRefreshed<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time when this chart last refreshed</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation of this chart</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the modification of this chart</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An optional AuditStamp corresponding to the deletion of this chart</p>
</td>
</tr>
</tbody>
</table>

## ChartQuery

The query that was used to populate a Chart

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
rawQuery<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Raw query to build a chart from input datasets</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#chartquerytype"><code>ChartQueryType!</code></a>
</td>
<td>
<p>The type of the chart query</p>
</td>
</tr>
</tbody>
</table>

## CorpGroup

A DataHub Group entity, which represents a Person on the Metadata Entity Graph

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the group</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Group name eg wherehows dev, ask_metadata</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#corpgroupproperties"><code>CorpGroupProperties</code></a>
</td>
<td>
<p>Additional read only properties about the group</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#corpgroupinfo"><code>CorpGroupInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only info about the group</p>
</td>
</tr>
</tbody>
</table>

## CorpGroupInfo

Deprecated, use CorpUserProperties instead
Additional read only info about a group

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name to display when rendering the group</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description provided for the group</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>email of this group</p>
</td>
</tr>
<tr>
<td>
admins<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, do not use
owners of this group</p>
</td>
</tr>
<tr>
<td>
members<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship IsMemberOfGroup instead
List of ldap urn in this group</p>
</td>
</tr>
<tr>
<td>
groups<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, do not use
List of groups urns in this group</p>
</td>
</tr>
</tbody>
</table>

## CorpGroupProperties

Additional read only properties about a group

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description provided for the group</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>email of this group</p>
</td>
</tr>
</tbody>
</table>

## CorpUser

A DataHub User entity, which represents a Person on the Metadata Entity Graph

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the user</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>The standard Entity Type</p>
</td>
</tr>
<tr>
<td>
username<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A username associated with the user
This uniquely identifies the user within DataHub</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#corpuserproperties"><code>CorpUserProperties</code></a>
</td>
<td>
<p>Additional read only properties about the corp user</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#corpusereditableproperties"><code>CorpUserEditableProperties</code></a>
</td>
<td>
<p>Read write properties about the corp user</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the user</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#corpuserinfo"><code>CorpUserInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only info about the corp user</p>
</td>
</tr>
<tr>
<td>
editableInfo<br />
<a href="/docs/graphql/objects#corpusereditableinfo"><code>CorpUserEditableInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use editableProperties field instead
Read write info about the corp user</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use the tags field instead
The structured tags associated with the user</p>
</td>
</tr>
</tbody>
</table>

## CorpUserEditableInfo

Deprecated, use CorpUserEditableProperties instead
Additional read write info about a user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
aboutMe<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>About me section of the user</p>
</td>
</tr>
<tr>
<td>
teams<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Teams that the user belongs to</p>
</td>
</tr>
<tr>
<td>
skills<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Skills that the user possesses</p>
</td>
</tr>
<tr>
<td>
pictureLink<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A URL which points to a picture which user wants to set as a profile photo</p>
</td>
</tr>
</tbody>
</table>

## CorpUserEditableProperties

Additional read write properties about a user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
aboutMe<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>About me section of the user</p>
</td>
</tr>
<tr>
<td>
teams<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Teams that the user belongs to</p>
</td>
</tr>
<tr>
<td>
skills<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Skills that the user possesses</p>
</td>
</tr>
<tr>
<td>
pictureLink<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A URL which points to a picture which user wants to set as a profile photo</p>
</td>
</tr>
</tbody>
</table>

## CorpUserInfo

Deprecated, use CorpUserProperties instead
Additional read only info about a user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
active<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is active</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name of the user</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Email address of the user</p>
</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Title of the user</p>
</td>
</tr>
<tr>
<td>
manager<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser</code></a>
</td>
<td>
<p>Direct manager of the user</p>
</td>
</tr>
<tr>
<td>
departmentId<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>department id the user belong to</p>
</td>
</tr>
<tr>
<td>
departmentName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>department name this user belong to</p>
</td>
</tr>
<tr>
<td>
firstName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>first name of the user</p>
</td>
</tr>
<tr>
<td>
lastName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>last name of the user</p>
</td>
</tr>
<tr>
<td>
fullName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Common name of this user, format is firstName plus lastName</p>
</td>
</tr>
<tr>
<td>
countryCode<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>two uppercase letters country code</p>
</td>
</tr>
</tbody>
</table>

## CorpUserProperties

Additional read only properties about a user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
active<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is active</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name of the user</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Email address of the user</p>
</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Title of the user</p>
</td>
</tr>
<tr>
<td>
manager<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser</code></a>
</td>
<td>
<p>Direct manager of the user</p>
</td>
</tr>
<tr>
<td>
departmentId<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>department id the user belong to</p>
</td>
</tr>
<tr>
<td>
departmentName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>department name this user belong to</p>
</td>
</tr>
<tr>
<td>
firstName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>first name of the user</p>
</td>
</tr>
<tr>
<td>
lastName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>last name of the user</p>
</td>
</tr>
<tr>
<td>
fullName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Common name of this user, format is firstName plus lastName</p>
</td>
</tr>
<tr>
<td>
countryCode<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>two uppercase letters country code</p>
</td>
</tr>
</tbody>
</table>

## Cost



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
costType<br />
<a href="/docs/graphql/enums#costtype"><code>CostType!</code></a>
</td>
<td>
<p>Type of Cost Code</p>
</td>
</tr>
<tr>
<td>
costValue<br />
<a href="/docs/graphql/objects#costvalue"><code>CostValue!</code></a>
</td>
<td>
<p>Code to which the Cost of this entity should be attributed to ie organizational cost ID</p>
</td>
</tr>
</tbody>
</table>

## CostValue



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
costId<br />
<a href="/docs/graphql/scalars#float"><code>Float</code></a>
</td>
<td>
<p>Organizational Cost ID</p>
</td>
</tr>
<tr>
<td>
costCode<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Organizational Cost Code</p>
</td>
</tr>
</tbody>
</table>

## Dashboard

A Dashboard Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the Dashboard</p>
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
tool<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The dashboard tool name
Note that this will soon be deprecated in favor of a standardized notion of Data Platform</p>
</td>
</tr>
<tr>
<td>
dashboardId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>An id unique within the dashboard tool</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#dashboardproperties"><code>DashboardProperties</code></a>
</td>
<td>
<p>Additional read only properties about the dashboard</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#dashboardeditableproperties"><code>DashboardEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties about the dashboard</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the dashboard</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the dashboard</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the dashboard</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#dashboardinfo"><code>DashboardInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only information about the dashboard</p>
</td>
</tr>
<tr>
<td>
editableInfo<br />
<a href="/docs/graphql/objects#dashboardeditableproperties"><code>DashboardEditableProperties</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use editableProperties instead
Additional read write properties about the Dashboard</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
The structured tags associated with the dashboard</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities downstream of the given entity</p>
</td>
</tr>
</tbody>
</table>

## DashboardEditableProperties

Dashboard properties that are editable via the UI This represents logical metadata,
as opposed to technical metadata

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Dashboard</p>
</td>
</tr>
</tbody>
</table>

## DashboardInfo

Deprecated, use DashboardProperties instead
Additional read only info about a Dashboard

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display of the dashboard</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the dashboard</p>
</td>
</tr>
<tr>
<td>
charts<br />
<a href="/docs/graphql/objects#chart"><code>[Chart!]!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship Contains instead
Charts that comprise the dashboard</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the dashboard</p>
</td>
</tr>
<tr>
<td>
access<br />
<a href="/docs/graphql/enums#accesslevel"><code>AccessLevel</code></a>
</td>
<td>
<p>Access level for the dashboard
Note that this will soon be deprecated for low usage</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
<tr>
<td>
lastRefreshed<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time when this dashboard last refreshed</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation of this dashboard</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the modification of this dashboard</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An optional AuditStamp corresponding to the deletion of this dashboard</p>
</td>
</tr>
</tbody>
</table>

## DashboardProperties

Additional read only properties about a Dashboard

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display of the dashboard</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the dashboard</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the dashboard</p>
</td>
</tr>
<tr>
<td>
access<br />
<a href="/docs/graphql/enums#accesslevel"><code>AccessLevel</code></a>
</td>
<td>
<p>Access level for the dashboard
Note that this will soon be deprecated for low usage</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
<tr>
<td>
lastRefreshed<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time when this dashboard last refreshed</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation of this dashboard</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the modification of this dashboard</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An optional AuditStamp corresponding to the deletion of this dashboard</p>
</td>
</tr>
</tbody>
</table>

## DataFlow

A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
to produce an output Dataset Also known as a Data Pipeline

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of a Data Flow</p>
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
orchestrator<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Workflow orchestrator ei Azkaban, Airflow</p>
</td>
</tr>
<tr>
<td>
flowId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Id of the flow</p>
</td>
</tr>
<tr>
<td>
cluster<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Cluster of the flow</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#dataflowproperties"><code>DataFlowProperties</code></a>
</td>
<td>
<p>Additional read only properties about a Data flow</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#datafloweditableproperties"><code>DataFlowEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties about a Data Flow</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the flow</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the dataflow</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the dataflow</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#dataflowinfo"><code>DataFlowInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only information about a Data flow</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
The structured tags associated with the dataflow</p>
</td>
</tr>
<tr>
<td>
dataJobs<br />
<a href="/docs/graphql/objects#dataflowdatajobsrelationships"><code>DataFlowDataJobsRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship IsPartOf instead
Data Jobs</p>
</td>
</tr>
</tbody>
</table>

## DataFlowDataJobsRelationships

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entities<br />
<a href="/docs/graphql/objects#entityrelationshiplegacy"><code>[EntityRelationshipLegacy]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## DataFlowEditableProperties

Data Flow properties that are editable via the UI This represents logical metadata,
as opposed to technical metadata

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Data Flow</p>
</td>
</tr>
</tbody>
</table>

## DataFlowInfo

Deprecated, use DataFlowProperties instead
Additional read only properties about a Data Flow aka Pipeline

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name of the flow</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the flow</p>
</td>
</tr>
<tr>
<td>
project<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional project or namespace associated with the flow</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the DataFlow</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
</tbody>
</table>

## DataFlowProperties

Additional read only properties about a Data Flow aka Pipeline

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name of the flow</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the flow</p>
</td>
</tr>
<tr>
<td>
project<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional project or namespace associated with the flow</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the DataFlow</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
</tbody>
</table>

## DataJob

A Data Job Metadata Entity, representing an individual unit of computation or Task
to produce an output Dataset Always part of a parent Data Flow aka Pipeline

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the Data Job</p>
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
dataFlow<br />
<a href="/docs/graphql/objects#dataflow"><code>DataFlow</code></a>
</td>
<td>
<p>Deprecated, use relationship IsPartOf instead
The associated data flow</p>
</td>
</tr>
<tr>
<td>
jobId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Id of the job</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#datajobproperties"><code>DataJobProperties</code></a>
</td>
<td>
<p>Additional read only properties associated with the Data Job</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#datajobeditableproperties"><code>DataJobEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties associated with the Data Job</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the DataJob</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the job</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the DataJob</p>
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
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#datajobinfo"><code>DataJobInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional read only information about a Data processing job</p>
</td>
</tr>
<tr>
<td>
inputOutput<br />
<a href="/docs/graphql/objects#datajobinputoutput"><code>DataJobInputOutput</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship Produces, Consumes, DownstreamOf instead
Information about the inputs and outputs of a Data processing job</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships Produces, Consumes, DownstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships Produces, Consumes, DownstreamOf instead
Entities downstream of the given entity</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use the tags field instead
The structured tags associated with the DataJob</p>
</td>
</tr>
</tbody>
</table>

## DataJobEditableProperties

Data Job properties that are editable via the UI This represents logical metadata,
as opposed to technical metadata

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Data Job</p>
</td>
</tr>
</tbody>
</table>

## DataJobInfo

Deprecated, use DataJobProperties instead
Additional read only information about a Data Job aka Task

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Job display name</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Job description</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the DataJob</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
</tbody>
</table>

## DataJobInputOutput

The lineage information for a DataJob
TODO Rename this to align with other Lineage models

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
inputDatasets<br />
<a href="/docs/graphql/objects#dataset"><code>[Dataset!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship Consumes instead
Input datasets produced by the data job during processing</p>
</td>
</tr>
<tr>
<td>
outputDatasets<br />
<a href="/docs/graphql/objects#dataset"><code>[Dataset!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship Produces instead
Output datasets produced by the data job during processing</p>
</td>
</tr>
<tr>
<td>
inputDatajobs<br />
<a href="/docs/graphql/objects#datajob"><code>[DataJob!]</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationship DownstreamOf instead
Input datajobs that this data job depends on</p>
</td>
</tr>
</tbody>
</table>

## DataJobProperties

Additional read only properties about a Data Job aka Task

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Job display name</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Job description</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the DataJob</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
</tbody>
</table>

## DataPlatform

A Data Platform represents a specific third party Data System or Tool Examples include
warehouses like Snowflake, orchestrators like Airflow, and dashboarding tools like Looker

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>Urn of the data platform</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Name of the data platform</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#dataplatformproperties"><code>DataPlatformProperties</code></a>
</td>
<td>
<p>Additional read only properties associated with a data platform</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties displayName instead
Display name of the data platform</p>
</td>
</tr>
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#dataplatforminfo"><code>DataPlatformInfo</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
Additional properties associated with a data platform</p>
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

## DataPlatformInfo

Deprecated, use DataPlatformProperties instead
Additional read only information about a Data Platform

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#platformtype"><code>PlatformType!</code></a>
</td>
<td>
<p>The platform category</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name associated with the platform</p>
</td>
</tr>
<tr>
<td>
datasetNameDelimiter<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The delimiter in the dataset names on the data platform</p>
</td>
</tr>
<tr>
<td>
logoUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A logo URL associated with the platform</p>
</td>
</tr>
</tbody>
</table>

## DataPlatformProperties

Additional read only properties about a Data Platform

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#platformtype"><code>PlatformType!</code></a>
</td>
<td>
<p>The platform category</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name associated with the platform</p>
</td>
</tr>
<tr>
<td>
datasetNameDelimiter<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The delimiter in the dataset names on the data platform</p>
</td>
</tr>
<tr>
<td>
logoUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A logo URL associated with the platform</p>
</td>
</tr>
</tbody>
</table>

## Dataset

A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the Dataset</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>The standard Entity Type</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the dataset is defined</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The Dataset display name</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#datasetproperties"><code>DatasetProperties</code></a>
</td>
<td>
<p>An additional set of read only properties</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#dataseteditableproperties"><code>DatasetEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the dataset</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dataset</p>
</td>
</tr>
<tr>
<td>
schemaMetadata<br />
<a href="/docs/graphql/objects#schemametadata"><code>SchemaMetadata</code></a>
</td>
<td>
<p>Schema metadata of the dataset, available by version number</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
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
editableSchemaMetadata<br />
<a href="/docs/graphql/objects#editableschemametadata"><code>EditableSchemaMetadata</code></a>
</td>
<td>
<p>Editable schema metadata of the dataset</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status of the Dataset</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags used for searching dataset</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the dataset</p>
</td>
</tr>
<tr>
<td>
usageStats<br />
<a href="/docs/graphql/objects#usagequeryresult"><code>UsageQueryResult</code></a>
</td>
<td>
<p>Statistics about how this Dataset is used</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resource<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
range<br />
<a href="/docs/graphql/enums#timerange"><code>TimeRange</code></a>
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
datasetProfiles<br />
<a href="/docs/graphql/objects#datasetprofile"><code>[DatasetProfile!]</code></a>
</td>
<td>
<p>Profile Stats resource that retrieves the events in a previous unit of time in descending order
If no start or end time are provided, the most recent events will be returned</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
startTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
endTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
limit<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
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
<tr>
<td>
schema<br />
<a href="/docs/graphql/objects#schema"><code>Schema</code></a>
</td>
<td>
<blockquote>Deprecated: Use `schemaMetadata`</blockquote>

<p>Schema metadata of the dataset</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
External URL associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
origin<br />
<a href="/docs/graphql/enums#fabrictype"><code>FabricType!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated,se the properties field instead
Environment in which the dataset belongs to or where it was generated
Note that this field will soon be deprecated in favor of a more standardized concept of Environment</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use the properties field instead
Read only technical description for dataset</p>
</td>
</tr>
<tr>
<td>
platformNativeType<br />
<a href="/docs/graphql/enums#platformnativetype"><code>PlatformNativeType</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, do not use this field
The logical type of the dataset ie table, stream, etc</p>
</td>
</tr>
<tr>
<td>
uri<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use externalUrl instead
Native Dataset Uri
Uri should not include any environment specific properties</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities downstream of the given entity</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
The structured tags associated with the dataset</p>
</td>
</tr>
</tbody>
</table>

## DatasetDeprecation

Deprecated, use Deprecation instead
Information about Dataset deprecation status
Note that this model will soon be migrated to a more general purpose Entity status

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
deprecated<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the dataset has been deprecated by owner</p>
</td>
</tr>
<tr>
<td>
decommissionTime<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time user plan to decommission this dataset</p>
</td>
</tr>
<tr>
<td>
note<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Additional information about the dataset deprecation plan</p>
</td>
</tr>
<tr>
<td>
actor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The user who will be credited for modifying this deprecation content</p>
</td>
</tr>
</tbody>
</table>

## DatasetEditableProperties

Dataset properties that are editable via the UI This represents logical metadata,
as opposed to technical metadata

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Dataset</p>
</td>
</tr>
</tbody>
</table>

## DatasetFieldProfile

An individual Dataset Field Profile

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
fieldPath<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The standardized path of the field</p>
</td>
</tr>
<tr>
<td>
uniqueCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The unique value count for the field across the Dataset</p>
</td>
</tr>
<tr>
<td>
uniqueProportion<br />
<a href="/docs/graphql/scalars#float"><code>Float</code></a>
</td>
<td>
<p>The proportion of rows with unique values across the Dataset</p>
</td>
</tr>
<tr>
<td>
nullCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The number of NULL row values across the Dataset</p>
</td>
</tr>
<tr>
<td>
nullProportion<br />
<a href="/docs/graphql/scalars#float"><code>Float</code></a>
</td>
<td>
<p>The proportion of rows with NULL values across the Dataset</p>
</td>
</tr>
<tr>
<td>
min<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The min value for the field</p>
</td>
</tr>
<tr>
<td>
max<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The max value for the field</p>
</td>
</tr>
<tr>
<td>
mean<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The mean value for the field</p>
</td>
</tr>
<tr>
<td>
median<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The median value for the field</p>
</td>
</tr>
<tr>
<td>
stdev<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The standard deviation for the field</p>
</td>
</tr>
<tr>
<td>
sampleValues<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>A set of sample values for the field</p>
</td>
</tr>
</tbody>
</table>

## DatasetProfile

A Dataset Profile associated with a Dataset, containing profiling statistics about the Dataset

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [TimeSeriesAspect](/docs/graphql/interfaces#timeseriesaspect)

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
<p>The time at which the profile was reported</p>
</td>
</tr>
<tr>
<td>
rowCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional row count of the Dataset</p>
</td>
</tr>
<tr>
<td>
columnCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional column count of the Dataset</p>
</td>
</tr>
<tr>
<td>
fieldProfiles<br />
<a href="/docs/graphql/objects#datasetfieldprofile"><code>[DatasetFieldProfile!]</code></a>
</td>
<td>
<p>An optional set of per field statistics obtained in the profile</p>
</td>
</tr>
<tr>
<td>
partitionSpec<br />
<a href="/docs/graphql/objects#partitionspec"><code>PartitionSpec</code></a>
</td>
<td>
<p>Information about the partition that was profiled</p>
</td>
</tr>
</tbody>
</table>

## DatasetProperties

Additional read only properties about a Dataset

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
origin<br />
<a href="/docs/graphql/enums#fabrictype"><code>FabricType!</code></a>
</td>
<td>
<p>Environment in which the dataset belongs to or where it was generated
Note that this field will soon be deprecated in favor of a more standardized concept of Environment</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Read only technical description for dataset</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Custom properties of the Dataset</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the Dataset</p>
</td>
</tr>
</tbody>
</table>

## Deprecation

Information about Metadata Entity deprecation status

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
deprecated<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the entity has been deprecated by owner</p>
</td>
</tr>
<tr>
<td>
decommissionTime<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time user plan to decommission this entity</p>
</td>
</tr>
<tr>
<td>
note<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Additional information about the entity deprecation plan</p>
</td>
</tr>
<tr>
<td>
actor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The user who will be credited for modifying this deprecation content</p>
</td>
</tr>
</tbody>
</table>

## DownstreamEntityRelationships

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entities<br />
<a href="/docs/graphql/objects#entityrelationshiplegacy"><code>[EntityRelationshipLegacy]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## DownstreamLineage

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
downstreams<br />
<a href="/docs/graphql/objects#relateddataset"><code>[RelatedDataset!]!</code></a>
</td>
<td>
<p>List of downstream datasets</p>
</td>
</tr>
</tbody>
</table>

## EditableSchemaFieldInfo

Editable schema field metadata ie descriptions, tags, etc

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
fieldPath<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Flattened name of a field identifying the field the editable info is applied to</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Edited description of the field</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
Tags associated with the field</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags associated with the field</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>Glossary terms associated with the field</p>
</td>
</tr>
</tbody>
</table>

## EditableSchemaMetadata

Information about schema metadata that is editable via the UI

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
editableSchemaFieldInfo<br />
<a href="/docs/graphql/objects#editableschemafieldinfo"><code>[EditableSchemaFieldInfo!]!</code></a>
</td>
<td>
<p>Editable schema field metadata</p>
</td>
</tr>
</tbody>
</table>

## EditableTagProperties

Additional read write Tag properties

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A description of the Tag</p>
</td>
</tr>
</tbody>
</table>

## EntityRelationship

A relationship between two entities TODO Migrate all entity relationships to this more generic model

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The type of the relationship</p>
</td>
</tr>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#relationshipdirection"><code>RelationshipDirection!</code></a>
</td>
<td>
<p>The direction of the relationship relative to the source entity</p>
</td>
</tr>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity!</code></a>
</td>
<td>
<p>Entity that is related via lineage</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the last modification of this relationship</p>
</td>
</tr>
</tbody>
</table>

## EntityRelationshipLegacy

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entitywithrelationships"><code>EntityWithRelationships</code></a>
</td>
<td>
<p>Entity that is related via lineage</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the last modification of this relationship</p>
</td>
</tr>
</tbody>
</table>

## EntityRelationshipsResult

A list of relationship information associated with a source Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Start offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Number of results in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationship"><code>[EntityRelationship!]!</code></a>
</td>
<td>
<p>Relationships in the result set</p>
</td>
</tr>
</tbody>
</table>

## EthicalConsiderations



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
data<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Does the model use any sensitive data eg, protected classes</p>
</td>
</tr>
<tr>
<td>
humanLife<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Is the model intended to inform decisions about matters central to human life or flourishing eg, health or safety</p>
</td>
</tr>
<tr>
<td>
mitigations<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>What risk mitigation strategies were used during model development</p>
</td>
</tr>
<tr>
<td>
risksAndHarms<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>What risks may be present in model usage
Try to identify the potential recipients, likelihood, and magnitude of harms
If these cannot be determined, note that they were considered but remain unknown</p>
</td>
</tr>
<tr>
<td>
useCases<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Are there any known model use cases that are especially fraught
This may connect directly to the intended use section</p>
</td>
</tr>
</tbody>
</table>

## FacetMetadata

Contains valid fields to filter search results further on

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
field<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Name of a field present in the search entity</p>
</td>
</tr>
<tr>
<td>
aggregations<br />
<a href="/docs/graphql/objects#aggregationmetadata"><code>[AggregationMetadata!]!</code></a>
</td>
<td>
<p>Aggregated search result counts by value of the field</p>
</td>
</tr>
</tbody>
</table>

## FieldUsageCounts

The usage for a particular Dataset field

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
fieldName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The path of the field</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The count of usages</p>
</td>
</tr>
</tbody>
</table>

## FloatBox



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
floatValue<br />
<a href="/docs/graphql/scalars#float"><code>Float!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## GlobalTags

Tags attached to a particular Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#tagassociation"><code>[TagAssociation!]</code></a>
</td>
<td>
<p>The set of tags attached to the Metadata Entity</p>
</td>
</tr>
</tbody>
</table>

## GlossaryTerm

A Glossary Term, or a node in a Business Glossary representing a standardized domain
data type

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the glossary term</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the dataset</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name of the glossary term</p>
</td>
</tr>
<tr>
<td>
hierarchicalName<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>hierarchicalName of glossary term</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#glossarytermproperties"><code>GlossaryTermProperties</code></a>
</td>
<td>
<p>Additional read only properties associated with the Glossary Term</p>
</td>
</tr>
<tr>
<td>
glossaryTermInfo<br />
<a href="/docs/graphql/objects#glossaryterminfo"><code>GlossaryTermInfo!</code></a>
</td>
<td>
<p>Deprecated, use properties field instead
Details of the Glossary Term</p>
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

## GlossaryTermAssociation

An edge between a Metadata Entity and a Glossary Term Modeled as a struct to permit
additional attributes
TODO Consider whether this query should be serviced by the relationships field

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
term<br />
<a href="/docs/graphql/objects#glossaryterm"><code>GlossaryTerm!</code></a>
</td>
<td>
<p>The glossary term itself</p>
</td>
</tr>
</tbody>
</table>

## GlossaryTermInfo

Deprecated, use GlossaryTermProperties instead
Information about a glossary term

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
definition<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Definition of the glossary term</p>
</td>
</tr>
<tr>
<td>
termSource<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Term Source of the glossary term</p>
</td>
</tr>
<tr>
<td>
sourceRef<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Source Ref of the glossary term</p>
</td>
</tr>
<tr>
<td>
sourceUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Source Url of the glossary term</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Properties of the glossary term</p>
</td>
</tr>
<tr>
<td>
rawSchema<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Schema definition of glossary term</p>
</td>
</tr>
</tbody>
</table>

## GlossaryTermProperties

Additional read only properties about a Glossary Term

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
definition<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Definition of the glossary term</p>
</td>
</tr>
<tr>
<td>
termSource<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Term Source of the glossary term</p>
</td>
</tr>
<tr>
<td>
sourceRef<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Source Ref of the glossary term</p>
</td>
</tr>
<tr>
<td>
sourceUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Source Url of the glossary term</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Properties of the glossary term</p>
</td>
</tr>
<tr>
<td>
rawSchema<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Schema definition of glossary term</p>
</td>
</tr>
</tbody>
</table>

## GlossaryTerms

Glossary Terms attached to a particular Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
terms<br />
<a href="/docs/graphql/objects#glossarytermassociation"><code>[GlossaryTermAssociation!]</code></a>
</td>
<td>
<p>The set of glossary terms attached to the Metadata Entity</p>
</td>
</tr>
</tbody>
</table>

## HyperParameterMap



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
key<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/unions#hyperparametervaluetype"><code>HyperParameterValueType!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## InstitutionalMemory

Institutional memory metadata, meaning internal links and pointers related to an Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
elements<br />
<a href="/docs/graphql/objects#institutionalmemorymetadata"><code>[InstitutionalMemoryMetadata!]!</code></a>
</td>
<td>
<p>List of records that represent the institutional memory or internal documentation of an entity</p>
</td>
</tr>
</tbody>
</table>

## InstitutionalMemoryMetadata

An institutional memory resource about a particular Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
url<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Link to a document or wiki page or another internal resource</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Description of the resource</p>
</td>
</tr>
<tr>
<td>
author<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser!</code></a>
</td>
<td>
<p>The author of this metadata</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation of this resource</p>
</td>
</tr>
</tbody>
</table>

## IntBox



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
intValue<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## IntendedUse



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
primaryUses<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Primary Use cases for the model</p>
</td>
</tr>
<tr>
<td>
primaryUsers<br />
<a href="/docs/graphql/enums#intendedusertype"><code>[IntendedUserType!]</code></a>
</td>
<td>
<p>Primary Intended Users</p>
</td>
</tr>
<tr>
<td>
outOfScopeUses<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Out of scope uses of the MLModel</p>
</td>
</tr>
</tbody>
</table>

## KeyValueSchema

Information about a raw Key Value Schema

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
keySchema<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Raw key schema</p>
</td>
</tr>
<tr>
<td>
valueSchema<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Raw value schema</p>
</td>
</tr>
</tbody>
</table>

## ListPoliciesResult

The result obtained when listing DataHub Access Policies

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The starting offset of the result set returned</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of Policies in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Policies in the result set</p>
</td>
</tr>
<tr>
<td>
policies<br />
<a href="/docs/graphql/objects#policy"><code>[Policy!]!</code></a>
</td>
<td>
<p>The Policies themselves</p>
</td>
</tr>
</tbody>
</table>

## MatchedField

An overview of the field that was matched in the entity search document

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Name of the field that matched</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Value of the field that matched</p>
</td>
</tr>
</tbody>
</table>

## Metrics



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
performanceMeasures<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Measures of ML Model performance</p>
</td>
</tr>
<tr>
<td>
decisionThreshold<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Decision Thresholds used if any</p>
</td>
</tr>
</tbody>
</table>

## MLFeature

An ML Feature Metadata Entity Note that this entity is incubating

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the ML Feature</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name for the ML Feature</p>
</td>
</tr>
<tr>
<td>
featureNamespace<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>MLFeature featureNamespace</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description about the ML Feature</p>
</td>
</tr>
<tr>
<td>
dataType<br />
<a href="/docs/graphql/enums#mlfeaturedatatype"><code>MLFeatureDataType</code></a>
</td>
<td>
<p>MLFeature data type</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
featureProperties<br />
<a href="/docs/graphql/objects#mlfeatureproperties"><code>MLFeatureProperties</code></a>
</td>
<td>
<p>ModelProperties metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the MLFeature</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>Deprecation</p>
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

## MLFeatureProperties



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
dataType<br />
<a href="/docs/graphql/enums#mlfeaturedatatype"><code>MLFeatureDataType</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/objects#versiontag"><code>VersionTag</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
sources<br />
<a href="/docs/graphql/objects#dataset"><code>[Dataset]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLFeatureTable

An ML Feature Table Entity Note that this entity is incubating

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the ML Feature Table</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the MLFeatureTable is defined</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>MLFeatureTable description</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the MLFeatureTable</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#mlfeaturetableproperties"><code>MLFeatureTableProperties</code></a>
</td>
<td>
<p>Additional read only properties associated the the ML Feature Table</p>
</td>
</tr>
<tr>
<td>
featureTableProperties<br />
<a href="/docs/graphql/objects#mlfeaturetableproperties"><code>MLFeatureTableProperties</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
ModelProperties metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the MLFeature</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the MLFeatureTable</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>Deprecation</p>
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

## MLFeatureTableProperties



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
mlFeatures<br />
<a href="/docs/graphql/objects#mlfeature"><code>[MLFeature]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
mlPrimaryKeys<br />
<a href="/docs/graphql/objects#mlprimarykey"><code>[MLPrimaryKey]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLHyperParam



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLMetric



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLModel

An ML Model Metadata Entity Note that this entity is incubating

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the ML model</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>ML model display name</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the MLModel is defined</p>
</td>
</tr>
<tr>
<td>
origin<br />
<a href="/docs/graphql/enums#fabrictype"><code>FabricType!</code></a>
</td>
<td>
<p>Fabric type where mlmodel belongs to or where it was generated</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Human readable description for mlmodel</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
The standard tags for the ML Model</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The standard tags for the ML Model</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the mlmodel</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#mlmodelproperties"><code>MLModelProperties</code></a>
</td>
<td>
<p>Additional read only information about the ML Model</p>
</td>
</tr>
<tr>
<td>
intendedUse<br />
<a href="/docs/graphql/objects#intendeduse"><code>IntendedUse</code></a>
</td>
<td>
<p>Intended use of the mlmodel</p>
</td>
</tr>
<tr>
<td>
factorPrompts<br />
<a href="/docs/graphql/objects#mlmodelfactorprompts"><code>MLModelFactorPrompts</code></a>
</td>
<td>
<p>Factors metadata of the mlmodel</p>
</td>
</tr>
<tr>
<td>
metrics<br />
<a href="/docs/graphql/objects#metrics"><code>Metrics</code></a>
</td>
<td>
<p>Metrics metadata of the mlmodel</p>
</td>
</tr>
<tr>
<td>
evaluationData<br />
<a href="/docs/graphql/objects#basedata"><code>[BaseData!]</code></a>
</td>
<td>
<p>Evaluation Data of the mlmodel</p>
</td>
</tr>
<tr>
<td>
trainingData<br />
<a href="/docs/graphql/objects#basedata"><code>[BaseData!]</code></a>
</td>
<td>
<p>Training Data of the mlmodel</p>
</td>
</tr>
<tr>
<td>
quantitativeAnalyses<br />
<a href="/docs/graphql/objects#quantitativeanalyses"><code>QuantitativeAnalyses</code></a>
</td>
<td>
<p>Quantitative Analyses of the mlmodel</p>
</td>
</tr>
<tr>
<td>
ethicalConsiderations<br />
<a href="/docs/graphql/objects#ethicalconsiderations"><code>EthicalConsiderations</code></a>
</td>
<td>
<p>Ethical Considerations of the mlmodel</p>
</td>
</tr>
<tr>
<td>
caveatsAndRecommendations<br />
<a href="/docs/graphql/objects#caveatsandrecommendations"><code>CaveatsAndRecommendations</code></a>
</td>
<td>
<p>Caveats and Recommendations of the mlmodel</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the mlmodel</p>
</td>
</tr>
<tr>
<td>
sourceCode<br />
<a href="/docs/graphql/objects#sourcecode"><code>SourceCode</code></a>
</td>
<td>
<p>Source Code</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the mlmodel</p>
</td>
</tr>
<tr>
<td>
cost<br />
<a href="/docs/graphql/objects#cost"><code>Cost</code></a>
</td>
<td>
<p>Cost Aspect of the mlmodel</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>Deprecation</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities downstream of the given entity</p>
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

## MLModelFactorPrompts



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
relevantFactors<br />
<a href="/docs/graphql/objects#mlmodelfactors"><code>[MLModelFactors!]</code></a>
</td>
<td>
<p>What are foreseeable salient factors for which MLModel performance may vary, and how were these determined</p>
</td>
</tr>
<tr>
<td>
evaluationFactors<br />
<a href="/docs/graphql/objects#mlmodelfactors"><code>[MLModelFactors!]</code></a>
</td>
<td>
<p>Which factors are being reported, and why were these chosen</p>
</td>
</tr>
</tbody>
</table>

## MLModelFactors



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groups<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Distinct categories with similar characteristics that are present in the evaluation data instances</p>
</td>
</tr>
<tr>
<td>
instrumentation<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Instrumentation used for MLModel</p>
</td>
</tr>
<tr>
<td>
environment<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Environment in which the MLModel is deployed</p>
</td>
</tr>
</tbody>
</table>

## MLModelGroup

An ML Model Group Metadata Entity
Note that this entity is incubating

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the ML Model Group</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name for the Entity</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the MLModelGroup is defined</p>
</td>
</tr>
<tr>
<td>
origin<br />
<a href="/docs/graphql/enums#fabrictype"><code>FabricType!</code></a>
</td>
<td>
<p>Fabric type where MLModelGroup belongs to or where it was generated</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Human readable description for MLModelGroup</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#mlmodelgroupproperties"><code>MLModelGroupProperties</code></a>
</td>
<td>
<p>Additional read only properties about the ML Model Group</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the MLModelGroup</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>Deprecation</p>
</td>
</tr>
<tr>
<td>
upstreamLineage<br />
<a href="/docs/graphql/objects#upstreamentityrelationships"><code>UpstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities upstream of the given entity</p>
</td>
</tr>
<tr>
<td>
downstreamLineage<br />
<a href="/docs/graphql/objects#downstreamentityrelationships"><code>DownstreamEntityRelationships</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use relationships DownstreamOf, UpstreamOf instead
Entities downstream of the given entity</p>
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

## MLModelGroupProperties



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/objects#versiontag"><code>VersionTag</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLModelProperties



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
date<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
hyperParameters<br />
<a href="/docs/graphql/objects#hyperparametermap"><code>HyperParameterMap</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
hyperParams<br />
<a href="/docs/graphql/objects#mlhyperparam"><code>[MLHyperParam]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
trainingMetrics<br />
<a href="/docs/graphql/objects#mlmetric"><code>[MLMetric]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
mlFeatures<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
groups<br />
<a href="/docs/graphql/objects#mlmodelgroup"><code>[MLModelGroup]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLPrimaryKey

An ML Primary Key Entity Note that this entity is incubating

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the ML Primary Key</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name</p>
</td>
</tr>
<tr>
<td>
featureNamespace<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>MLPrimaryKey featureNamespace</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>MLPrimaryKey description</p>
</td>
</tr>
<tr>
<td>
dataType<br />
<a href="/docs/graphql/enums#mlfeaturedatatype"><code>MLFeatureDataType</code></a>
</td>
<td>
<p>MLPrimaryKey data type</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#mlprimarykeyproperties"><code>MLPrimaryKeyProperties</code></a>
</td>
<td>
<p>Additional read only properties of the ML Primary Key</p>
</td>
</tr>
<tr>
<td>
primaryKeyProperties<br />
<a href="/docs/graphql/objects#mlprimarykeyproperties"><code>MLPrimaryKeyProperties</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties field instead
MLPrimaryKeyProperties</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the MLPrimaryKey</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the MLPrimaryKey</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the MLPrimaryKey</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>Deprecation</p>
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

## MLPrimaryKeyProperties



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
dataType<br />
<a href="/docs/graphql/enums#mlfeaturedatatype"><code>MLFeatureDataType</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/objects#versiontag"><code>VersionTag</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
sources<br />
<a href="/docs/graphql/objects#dataset"><code>[Dataset]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## Owner

An owner of a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owner<br />
<a href="/docs/graphql/unions#ownertype"><code>OwnerType!</code></a>
</td>
<td>
<p>Owner object</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType!</code></a>
</td>
<td>
<p>The type of the ownership</p>
</td>
</tr>
<tr>
<td>
source<br />
<a href="/docs/graphql/objects#ownershipsource"><code>OwnershipSource</code></a>
</td>
<td>
<p>Source information for the ownership</p>
</td>
</tr>
</tbody>
</table>

## Ownership

Ownership information about a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owners<br />
<a href="/docs/graphql/objects#owner"><code>[Owner!]</code></a>
</td>
<td>
<p>List of owners of the entity</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>Audit stamp containing who last modified the record and when</p>
</td>
</tr>
</tbody>
</table>

## OwnershipSource

Information about the source of Ownership metadata about a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#ownershipsourcetype"><code>OwnershipSourceType!</code></a>
</td>
<td>
<p>The type of the source</p>
</td>
</tr>
<tr>
<td>
url<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional reference URL for the source</p>
</td>
</tr>
</tbody>
</table>

## PartitionSpec

Information about the partition being profiled

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
partition<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The partition identifier</p>
</td>
</tr>
<tr>
<td>
timePartition<br />
<a href="/docs/graphql/objects#timewindow"><code>TimeWindow</code></a>
</td>
<td>
<p>The optional time window partition information</p>
</td>
</tr>
</tbody>
</table>

## PlatformPrivileges

The platform privileges that the currently authenticated user has

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
viewAnalytics<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to view analytics</p>
</td>
</tr>
<tr>
<td>
managePolicies<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to manage policies</p>
</td>
</tr>
</tbody>
</table>

## PoliciesConfig

Configurations related to the Policies Feature

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
enabled<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the policies feature is enabled and should be displayed in the UI</p>
</td>
</tr>
<tr>
<td>
platformPrivileges<br />
<a href="/docs/graphql/objects#privilege"><code>[Privilege!]!</code></a>
</td>
<td>
<p>A list of platform privileges to display in the Policy Builder experience</p>
</td>
</tr>
<tr>
<td>
resourcePrivileges<br />
<a href="/docs/graphql/objects#resourceprivileges"><code>[ResourcePrivileges!]!</code></a>
</td>
<td>
<p>A list of resource privileges to display in the Policy Builder experience</p>
</td>
</tr>
</tbody>
</table>

## Policy

An DataHub Platform Access Policy Access Policies determine who can perform what actions against which resources on the platform

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
<p>The primary key of the Policy</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#policytype"><code>PolicyType!</code></a>
</td>
<td>
<p>The type of the Policy</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Policy</p>
</td>
</tr>
<tr>
<td>
state<br />
<a href="/docs/graphql/enums#policystate"><code>PolicyState!</code></a>
</td>
<td>
<p>The present state of the Policy</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description of the Policy</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/objects#resourcefilter"><code>ResourceFilter</code></a>
</td>
<td>
<p>The resources that the Policy privileges apply to</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The privileges that the Policy grants</p>
</td>
</tr>
<tr>
<td>
actors<br />
<a href="/docs/graphql/objects#actorfilter"><code>ActorFilter!</code></a>
</td>
<td>
<p>The actors that the Policy grants privileges to</p>
</td>
</tr>
<tr>
<td>
editable<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the Policy is editable, ie system policies, or not</p>
</td>
</tr>
</tbody>
</table>

## Privilege

An individual DataHub Access Privilege

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Standardized privilege type, serving as a unique identifier for a privilege eg EDIT_ENTITY</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name to appear when displaying the privilege, eg Edit Entity</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A description of the privilege to display</p>
</td>
</tr>
</tbody>
</table>

## QuantitativeAnalyses



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
unitaryResults<br />
<a href="/docs/graphql/unions#resultstype"><code>ResultsType</code></a>
</td>
<td>
<p>Link to a dashboard with results showing how the model performed with respect to each factor</p>
</td>
</tr>
<tr>
<td>
intersectionalResults<br />
<a href="/docs/graphql/unions#resultstype"><code>ResultsType</code></a>
</td>
<td>
<p>Link to a dashboard with results showing how the model performed with respect to the intersection of evaluated factors</p>
</td>
</tr>
</tbody>
</table>

## RelatedDataset

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
dataset<br />
<a href="/docs/graphql/objects#dataset"><code>Dataset!</code></a>
</td>
<td>
<p>The upstream dataset the lineage points to</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#datasetlineagetype"><code>DatasetLineageType!</code></a>
</td>
<td>
<p>The type of the lineage</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>When the lineage was created</p>
</td>
</tr>
</tbody>
</table>

## ResourceFilter

The resources that a DataHub Access Policy applies to

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The type of the resource the policy should apply to Not required because in the future we want to support filtering by type OR by domain</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>A list of specific resource urns to apply the filter to</p>
</td>
</tr>
<tr>
<td>
allResources<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether of not to apply the filter to all resources of the type</p>
</td>
</tr>
</tbody>
</table>

## ResourcePrivileges

A privilege associated with a particular resource type
A resource is most commonly a DataHub Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resourceType<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Resource type associated with the Access Privilege, eg dataset</p>
</td>
</tr>
<tr>
<td>
resourceTypeDisplayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name to used for displaying the resourceType</p>
</td>
</tr>
<tr>
<td>
entityType<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType</code></a>
</td>
<td>
<p>An optional entity type to use when performing search and navigation to the entity</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#privilege"><code>[Privilege!]!</code></a>
</td>
<td>
<p>A list of privileges that are supported against this resource</p>
</td>
</tr>
</tbody>
</table>

## Schema

Deprecated, use SchemaMetadata instead
Metadata about a Dataset schema

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Dataset this schema metadata is associated with</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Schema name</p>
</td>
</tr>
<tr>
<td>
platformUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Platform this schema metadata is associated with</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The version of the GMS Schema metadata</p>
</td>
</tr>
<tr>
<td>
cluster<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The cluster this schema metadata is derived from</p>
</td>
</tr>
<tr>
<td>
hash<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The SHA1 hash of the schema content</p>
</td>
</tr>
<tr>
<td>
platformSchema<br />
<a href="/docs/graphql/unions#platformschema"><code>PlatformSchema</code></a>
</td>
<td>
<p>The native schema in the datasets platform, schemaless if it was not provided</p>
</td>
</tr>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#schemafield"><code>[SchemaField!]!</code></a>
</td>
<td>
<p>Client provided a list of fields from value schema</p>
</td>
</tr>
<tr>
<td>
primaryKeys<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Client provided list of fields that define primary keys to access record</p>
</td>
</tr>
</tbody>
</table>

## SchemaField

Information about an individual field in a Dataset schema

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
fieldPath<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Flattened name of the field computed from jsonPath field</p>
</td>
</tr>
<tr>
<td>
jsonPath<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Flattened name of a field in JSON Path notation</p>
</td>
</tr>
<tr>
<td>
nullable<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Indicates if this field is optional or nullable</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the field</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#schemafielddatatype"><code>SchemaFieldDataType!</code></a>
</td>
<td>
<p>Platform independent field type of the field</p>
</td>
</tr>
<tr>
<td>
nativeDataType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The native type of the field in the datasets platform as declared by platform schema</p>
</td>
</tr>
<tr>
<td>
recursive<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the field references its own type recursively</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use tags field instead
Tags associated with the field</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags associated with the field</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>Glossary terms associated with the field</p>
</td>
</tr>
<tr>
<td>
isPartOfKey<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether the field is part of a key schema</p>
</td>
</tr>
</tbody>
</table>

## SchemaMetadata

Metadata about a Dataset schema

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Aspect](/docs/graphql/interfaces#aspect)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
aspectVersion<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The logical version of the schema metadata, where zero represents the latest version
with otherwise monotonic ordering starting at one</p>
</td>
</tr>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Dataset this schema metadata is associated with</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Schema name</p>
</td>
</tr>
<tr>
<td>
platformUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Platform this schema metadata is associated with</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The version of the GMS Schema metadata</p>
</td>
</tr>
<tr>
<td>
cluster<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The cluster this schema metadata is derived from</p>
</td>
</tr>
<tr>
<td>
hash<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The SHA1 hash of the schema content</p>
</td>
</tr>
<tr>
<td>
platformSchema<br />
<a href="/docs/graphql/unions#platformschema"><code>PlatformSchema</code></a>
</td>
<td>
<p>The native schema in the datasets platform, schemaless if it was not provided</p>
</td>
</tr>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#schemafield"><code>[SchemaField!]!</code></a>
</td>
<td>
<p>Client provided a list of fields from value schema</p>
</td>
</tr>
<tr>
<td>
primaryKeys<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Client provided list of fields that define primary keys to access record</p>
</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time at which the schema metadata information was created</p>
</td>
</tr>
</tbody>
</table>

## SearchResult

An individual search result hit

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity!</code></a>
</td>
<td>
<p>The resolved DataHub Metadata Entity matching the search query</p>
</td>
</tr>
<tr>
<td>
matchedFields<br />
<a href="/docs/graphql/objects#matchedfield"><code>[MatchedField!]!</code></a>
</td>
<td>
<p>Matched field hint</p>
</td>
</tr>
</tbody>
</table>

## SearchResults

Results returned by issuing a search query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of entities included in the result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of search results matching the query and filters</p>
</td>
</tr>
<tr>
<td>
searchResults<br />
<a href="/docs/graphql/objects#searchresult"><code>[SearchResult!]!</code></a>
</td>
<td>
<p>The search result entities</p>
</td>
</tr>
<tr>
<td>
facets<br />
<a href="/docs/graphql/objects#facetmetadata"><code>[FacetMetadata!]</code></a>
</td>
<td>
<p>Candidate facet aggregations used for search filtering</p>
</td>
</tr>
</tbody>
</table>

## SourceCode



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
sourceCode<br />
<a href="/docs/graphql/objects#sourcecodeurl"><code>[SourceCodeUrl!]</code></a>
</td>
<td>
<p>Source Code along with types</p>
</td>
</tr>
</tbody>
</table>

## SourceCodeUrl



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#sourcecodeurltype"><code>SourceCodeUrlType!</code></a>
</td>
<td>
<p>Source Code Url Types</p>
</td>
</tr>
<tr>
<td>
sourceCodeUrl<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Source Code Url</p>
</td>
</tr>
</tbody>
</table>

## Status

The status of a particular Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
removed<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the entity is removed or not</p>
</td>
</tr>
</tbody>
</table>

## StringBox



<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
stringValue<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## StringMapEntry

An entry in a string string map represented as a tuple

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
key<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The key of the map entry</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The value fo the map entry</p>
</td>
</tr>
</tbody>
</table>

## TableSchema

Information about a raw Table Schema

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
schema<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Raw table schema</p>
</td>
</tr>
</tbody>
</table>

## Tag

A Tag Entity, which can be associated with other Metadata Entities and subresources

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)

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
<p>The primary key of the TAG</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name of the tag</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#editabletagproperties"><code>EditableTagProperties</code></a>
</td>
<td>
<p>Additional read write properties about the Tag</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the dataset</p>
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
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use editableProperties field instead
Description of the tag</p>
</td>
</tr>
</tbody>
</table>

## TagAssociation

An edge between a Metadata Entity and a Tag Modeled as a struct to permit
additional attributes
TODO Consider whether this query should be serviced by the relationships field

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tag<br />
<a href="/docs/graphql/objects#tag"><code>Tag!</code></a>
</td>
<td>
<p>The tag itself</p>
</td>
</tr>
</tbody>
</table>

## TimeWindow

A time window with a finite start and end time

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
startTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The start time of the time window</p>
</td>
</tr>
<tr>
<td>
durationMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The end time of the time window</p>
</td>
</tr>
</tbody>
</table>

## UpstreamEntityRelationships

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entities<br />
<a href="/docs/graphql/objects#entityrelationshiplegacy"><code>[EntityRelationshipLegacy]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## UpstreamLineage

Deprecated, use relationships query instead

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
upstreams<br />
<a href="/docs/graphql/objects#relateddataset"><code>[RelatedDataset!]!</code></a>
</td>
<td>
<p>List of upstream datasets</p>
</td>
</tr>
</tbody>
</table>

## UsageAggregation

An aggregation of Dataset usage statistics

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
bucket<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time window start time</p>
</td>
</tr>
<tr>
<td>
duration<br />
<a href="/docs/graphql/enums#windowduration"><code>WindowDuration</code></a>
</td>
<td>
<p>The time window span</p>
</td>
</tr>
<tr>
<td>
resource<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The resource urn associated with the usage information, eg a Dataset urn</p>
</td>
</tr>
<tr>
<td>
metrics<br />
<a href="/docs/graphql/objects#usageaggregationmetrics"><code>UsageAggregationMetrics</code></a>
</td>
<td>
<p>The rolled up usage metrics</p>
</td>
</tr>
</tbody>
</table>

## UsageAggregationMetrics

Rolled up metrics about Dataset usage over time

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
uniqueUserCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The unique number of users who have queried the dataset within the time range</p>
</td>
</tr>
<tr>
<td>
users<br />
<a href="/docs/graphql/objects#userusagecounts"><code>[UserUsageCounts]</code></a>
</td>
<td>
<p>Usage statistics within the time range by user</p>
</td>
</tr>
<tr>
<td>
totalSqlQueries<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of queries issued against the dataset within the time range</p>
</td>
</tr>
<tr>
<td>
topSqlQueries<br />
<a href="/docs/graphql/scalars#string"><code>[String]</code></a>
</td>
<td>
<p>A set of common queries issued against the dataset within the time range</p>
</td>
</tr>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#fieldusagecounts"><code>[FieldUsageCounts]</code></a>
</td>
<td>
<p>Per field usage statistics within the time range</p>
</td>
</tr>
</tbody>
</table>

## UsageQueryResult

The result of a Dataset usage query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
buckets<br />
<a href="/docs/graphql/objects#usageaggregation"><code>[UsageAggregation]</code></a>
</td>
<td>
<p>A set of relevant time windows for use in displaying usage statistics</p>
</td>
</tr>
<tr>
<td>
aggregations<br />
<a href="/docs/graphql/objects#usagequeryresultaggregations"><code>UsageQueryResultAggregations</code></a>
</td>
<td>
<p>A set of rolled up aggregations about the Dataset usage</p>
</td>
</tr>
</tbody>
</table>

## UsageQueryResultAggregations

A set of rolled up aggregations about the Dataset usage

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
uniqueUserCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The count of unique Dataset users within the queried time range</p>
</td>
</tr>
<tr>
<td>
users<br />
<a href="/docs/graphql/objects#userusagecounts"><code>[UserUsageCounts]</code></a>
</td>
<td>
<p>The specific per user usage counts within the queried time range</p>
</td>
</tr>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#fieldusagecounts"><code>[FieldUsageCounts]</code></a>
</td>
<td>
<p>The specific per field usage counts within the queried time range</p>
</td>
</tr>
<tr>
<td>
totalSqlQueries<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of queries executed within the queried time range
Note that this field will likely be deprecated in favor of a totalQueries field</p>
</td>
</tr>
</tbody>
</table>

## UserUsageCounts

Information about individual user usage of a Dataset

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
user<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser</code></a>
</td>
<td>
<p>The user of the Dataset</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The number of queries issued by the user</p>
</td>
</tr>
<tr>
<td>
userEmail<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The extracted user email
Note that this field will soon be deprecated and merged with user</p>
</td>
</tr>
</tbody>
</table>

## VersionTag

The technical version associated with a given Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
versionTag<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

