---
id: inputObjects
title: Input objects
slug: inputObjects
sidebar_position: 7
---

## ActorFilterInput

Input required when creating or updating an Access Policies Determines which actors the Policy applies to

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

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

## AutoCompleteInput

Input for performing an auto completion query against a single Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType</code></a>
</td>
<td>
<p>Entity type to be autocompleted against</p>
</td>
</tr>
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
field<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional entity field name to autocomplete on</p>
</td>
</tr>
<tr>
<td>
limit<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The maximum number of autocomplete results to be returned</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Faceted filters applied to autocomplete results</p>
</td>
</tr>
</tbody>
</table>

## AutoCompleteMultipleInput

Input for performing an auto completion query against a a set of Metadata Entities

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
types<br />
<a href="/docs/graphql/enums#entitytype"><code>[EntityType!]</code></a>
</td>
<td>
<p>Entity types to be autocompleted against
Optional, if none supplied, all searchable types will be autocompleted against</p>
</td>
</tr>
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
field<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional field to autocomplete against</p>
</td>
</tr>
<tr>
<td>
limit<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The maximum number of autocomplete results</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Faceted filters applied to autocomplete results</p>
</td>
</tr>
</tbody>
</table>

## BrowseInput

Input required for browse queries

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>The browse entity type</p>
</td>
</tr>
<tr>
<td>
path<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>The browse path</p>
</td>
</tr>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The starting point of paginated results</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The number of elements included in the results</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Faceted filters applied to browse results</p>
</td>
</tr>
</tbody>
</table>

## BrowsePathsInput

Inputs for fetching the browse paths for a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>The browse entity type</p>
</td>
</tr>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The entity urn</p>
</td>
</tr>
</tbody>
</table>

## ChartEditablePropertiesUpdate

Update to writable Chart fields

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Writable description aka documentation for a Chart</p>
</td>
</tr>
</tbody>
</table>

## ChartUpdateInput

Arguments provided to update a Chart Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Update to ownership</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Deprecated, use tags field instead
Update to global tags</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Update to tags</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/inputObjects#charteditablepropertiesupdate"><code>ChartEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## DashboardEditablePropertiesUpdate

Update to writable Dashboard fields

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Writable description aka documentation for a Dashboard</p>
</td>
</tr>
</tbody>
</table>

## DashboardUpdateInput

Arguments provided to update a Chart Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Update to ownership</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Deprecated, use tags field instead
Update to global tags</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Update to tags</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/inputObjects#dashboardeditablepropertiesupdate"><code>DashboardEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## DataFlowEditablePropertiesUpdate

Update to writable Data Flow fields

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Writable description aka documentation for a Data Flow</p>
</td>
</tr>
</tbody>
</table>

## DataFlowUpdateInput

Arguments provided to update a Data Flow aka Pipeline Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Update to ownership</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Deprecated, use tags field instead
Update to global tags</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Update to tags</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/inputObjects#datafloweditablepropertiesupdate"><code>DataFlowEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## DataJobEditablePropertiesUpdate

Update to writable Data Job fields

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Writable description aka documentation for a Data Job</p>
</td>
</tr>
</tbody>
</table>

## DataJobUpdateInput

Arguments provided to update a Data Job aka Task Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Update to ownership</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Deprecated, use tags field instead
Update to global tags</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Update to tags</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/inputObjects#datajobeditablepropertiesupdate"><code>DataJobEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## DatasetDeprecationUpdate

An update for the deprecation information for a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
deprecated<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the dataset is deprecated</p>
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
</tbody>
</table>

## DatasetEditablePropertiesUpdate

Update to writable Dataset fields

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Writable description aka documentation for a Dataset</p>
</td>
</tr>
</tbody>
</table>

## DatasetUpdateInput

Arguments provided to update a Dataset Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Update to ownership</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/inputObjects#datasetdeprecationupdate"><code>DatasetDeprecationUpdate</code></a>
</td>
<td>
<p>Update to deprecation status</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/inputObjects#institutionalmemoryupdate"><code>InstitutionalMemoryUpdate</code></a>
</td>
<td>
<p>Update to institutional memory, ie documentation</p>
</td>
</tr>
<tr>
<td>
globalTags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Deprecated, use tags field instead
Update to global tags</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Update to tags</p>
</td>
</tr>
<tr>
<td>
editableSchemaMetadata<br />
<a href="/docs/graphql/inputObjects#editableschemametadataupdate"><code>EditableSchemaMetadataUpdate</code></a>
</td>
<td>
<p>Update to editable schema metadata of the dataset</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/inputObjects#dataseteditablepropertiesupdate"><code>DatasetEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## EditableSchemaFieldInfoUpdate

Update to writable schema field metadata

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

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
<a href="/docs/graphql/inputObjects#globaltagsupdate"><code>GlobalTagsUpdate</code></a>
</td>
<td>
<p>Tags associated with the field</p>
</td>
</tr>
</tbody>
</table>

## EditableSchemaMetadataUpdate

Update to editable schema metadata of the dataset

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
editableSchemaFieldInfo<br />
<a href="/docs/graphql/inputObjects#editableschemafieldinfoupdate"><code>[EditableSchemaFieldInfoUpdate!]!</code></a>
</td>
<td>
<p>Update to writable schema field metadata</p>
</td>
</tr>
</tbody>
</table>

## FacetFilterInput

Facet filters to apply to search results

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
field<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Name of field to filter by</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Value of the field to filter by</p>
</td>
</tr>
</tbody>
</table>

## GlobalTagsUpdate

Deprecated, use addTag or removeTag mutation instead
Update to the Tags associated with a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tags<br />
<a href="/docs/graphql/inputObjects#tagassociationupdate"><code>[TagAssociationUpdate!]</code></a>
</td>
<td>
<p>The new set of tags</p>
</td>
</tr>
</tbody>
</table>

## InstitutionalMemoryMetadataUpdate

An institutional memory to add to a Metadata Entity
TODO Add a USER or GROUP actor enum

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

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
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the resource</p>
</td>
</tr>
<tr>
<td>
author<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The corp user urn of the author of the metadata</p>
</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time at which this metadata was created</p>
</td>
</tr>
</tbody>
</table>

## InstitutionalMemoryUpdate

An update for the institutional memory information for a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
elements<br />
<a href="/docs/graphql/inputObjects#institutionalmemorymetadataupdate"><code>[InstitutionalMemoryMetadataUpdate!]!</code></a>
</td>
<td>
<p>The individual references in the institutional memory</p>
</td>
</tr>
</tbody>
</table>

## ListPoliciesInput

Input required when listing DataHub Access Policies

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The starting offset of the result set returned</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The maximum number of Policies to be returned in the result set</p>
</td>
</tr>
</tbody>
</table>

## OwnershipUpdate

An update for the ownership information for a Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owners<br />
<a href="/docs/graphql/inputObjects#ownerupdate"><code>[OwnerUpdate!]!</code></a>
</td>
<td>
<p>The updated list of owners</p>
</td>
</tr>
</tbody>
</table>

## OwnerUpdate

An owner to add to a Metadata Entity
TODO Add a USER or GROUP actor enum

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owner<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The owner URN, either a corpGroup or corpuser</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType!</code></a>
</td>
<td>
<p>The owner type</p>
</td>
</tr>
</tbody>
</table>

## PolicyUpdateInput

Input provided when creating or updating an Access Policy

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#policytype"><code>PolicyType!</code></a>
</td>
<td>
<p>The Policy Type</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The Policy name</p>
</td>
</tr>
<tr>
<td>
state<br />
<a href="/docs/graphql/enums#policystate"><code>PolicyState!</code></a>
</td>
<td>
<p>The Policy state</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A Policy description</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcefilterinput"><code>ResourceFilterInput</code></a>
</td>
<td>
<p>The set of resources that the Policy privileges apply to</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The set of privileges that the Policy grants</p>
</td>
</tr>
<tr>
<td>
actors<br />
<a href="/docs/graphql/inputObjects#actorfilterinput"><code>ActorFilterInput!</code></a>
</td>
<td>
<p>The set of actors that the Policy privileges are granted to</p>
</td>
</tr>
</tbody>
</table>

## RelationshipsInput

Input for the list relationships field of an Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
types<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The types of relationships to query, representing an OR</p>
</td>
</tr>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#relationshipdirection"><code>RelationshipDirection!</code></a>
</td>
<td>
<p>The direction of the relationship, either incoming or outgoing from the source entity</p>
</td>
</tr>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The starting offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The number of results to be returned</p>
</td>
</tr>
</tbody>
</table>

## ResourceFilterInput

Input required when creating or updating an Access Policies Determines which resources the Policy applies to

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The type of the resource the policy should apply to
Not required because in the future we want to support filtering by type OR by domain</p>
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

## SearchInput

Input arguments for a full text search query

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>The Metadata Entity type to be searched against</p>
</td>
</tr>
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
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The number of entities to include in result set</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Facet filters to apply to search results</p>
</td>
</tr>
</tbody>
</table>

## TagAssociationInput

Input provided when updating the association between a Metadata Entity and a Tag

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tagUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Tag to add or remove</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The target Metadata Entity to add or remove the Tag to</p>
</td>
</tr>
<tr>
<td>
subResourceType<br />
<a href="/docs/graphql/enums#subresourcetype"><code>SubResourceType</code></a>
</td>
<td>
<p>An optional type of a sub resource to attach the Tag to</p>
</td>
</tr>
<tr>
<td>
subResource<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional sub resource identifier to attach the Tag to</p>
</td>
</tr>
</tbody>
</table>

## TagAssociationUpdate

Deprecated, use addTag or removeTag mutation instead
A tag update to be applied

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tag<br />
<a href="/docs/graphql/inputObjects#tagupdateinput"><code>TagUpdateInput!</code></a>
</td>
<td>
<p>The tag being applied</p>
</td>
</tr>
</tbody>
</table>

## TagUpdateInput

Deprecated, use addTag or removeTag mutations instead
An update for a particular Tag entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Tag</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name of a Tag</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the tag</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/inputObjects#ownershipupdate"><code>OwnershipUpdate</code></a>
</td>
<td>
<p>Ownership metadata of the tag</p>
</td>
</tr>
</tbody>
</table>

## TermAssociationInput

Input provided when updating the association between a Metadata Entity and a Glossary Term

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
termUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Glossary Term to add or remove</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The target Metadata Entity to add or remove the Glossary Term from</p>
</td>
</tr>
<tr>
<td>
subResourceType<br />
<a href="/docs/graphql/enums#subresourcetype"><code>SubResourceType</code></a>
</td>
<td>
<p>An optional type of a sub resource to attach the Glossary Term to</p>
</td>
</tr>
<tr>
<td>
subResource<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional sub resource identifier to attach the Glossary Term to</p>
</td>
</tr>
</tbody>
</table>

