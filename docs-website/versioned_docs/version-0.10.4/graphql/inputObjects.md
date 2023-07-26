---
id: inputObjects
title: Input objects
slug: inputObjects
sidebar_position: 7
---

## AcceptRoleInput

Input provided when accepting a DataHub role using an invite token

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
inviteToken<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The token needed to accept the role</p>
</td>
</tr>
</tbody>
</table>

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
resourceOwnersTypes<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Set of OwnershipTypes to apply the policy to (if resourceOwners field is set to True)</p>
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

## AddGroupMembersInput

Input required to add members to an external DataHub group

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groupUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The group to add members to</p>
</td>
</tr>
<tr>
<td>
userUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The members to add to the group</p>
</td>
</tr>
</tbody>
</table>

## AddLinkInput

Input provided when adding the association between a Metadata Entity and a Link

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
linkUrl<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The url of the link to add or remove</p>
</td>
</tr>
<tr>
<td>
label<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A label to attach to the link</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource or entity to attach the link to, for example a dataset urn</p>
</td>
</tr>
</tbody>
</table>

## AddNativeGroupMembersInput

Input required to add members to a native DataHub group

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groupUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The group to add members to</p>
</td>
</tr>
<tr>
<td>
userUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The members to add to the group</p>
</td>
</tr>
</tbody>
</table>

## AddOwnerInput

Input provided when adding the association between a Metadata Entity and an user or group owner

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownerUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Owner to add or remove</p>
</td>
</tr>
<tr>
<td>
ownerEntityType<br />
<a href="/docs/graphql/enums#ownerentitytype"><code>OwnerEntityType!</code></a>
</td>
<td>
<p>The owner type, either a user or group</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>The ownership type for the new owner. If none is provided, then a new NONE will be added.
Deprecated - Use ownershipTypeUrn field instead.</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the ownership type entity.</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource or entity to attach or remove the owner from, for example a dataset urn</p>
</td>
</tr>
</tbody>
</table>

## AddOwnersInput

Input provided when adding multiple associations between a Metadata Entity and an user or group owner

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owners<br />
<a href="/docs/graphql/inputObjects#ownerinput"><code>[OwnerInput!]!</code></a>
</td>
<td>
<p>The primary key of the Owner to add or remove</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource or entity to attach or remove the owner from, for example a dataset urn</p>
</td>
</tr>
</tbody>
</table>

## AddTagsInput

Input provided when adding tags to an asset

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tagUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Tags</p>
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

## AddTermsInput

Input provided when adding Terms to an asset

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
termUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
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

## AggregateAcrossEntitiesInput

Input arguments for a full text search query across entities to get aggregations

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
<p>Entity types to be searched. If this is not provided, all entities will be searched.</p>
</td>
</tr>
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
facets<br />
<a href="/docs/graphql/scalars#string"><code>[String]</code></a>
</td>
<td>
<p>The list of facets to get aggregations for. If list is empty or null, get aggregations for all facets
Sub-aggregations can be specified with the unicode character ␞ (U+241E) as a delimiter between the subtypes.
e.g. _entityType␞owners</p>
</td>
</tr>
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## AndFilterInput

A list of disjunctive criterion for the filter. (or operation to combine filters)

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
and<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>A list of and criteria the filter applies to the query</p>
</td>
</tr>
</tbody>
</table>

## AspectParams

Params to configure what list of aspects should be fetched by the aspects property

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
autoRenderOnly<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Only fetch auto render aspects</p>
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
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
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
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
</tbody>
</table>

## BatchAddOwnersInput

Input provided when adding owners to a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
owners<br />
<a href="/docs/graphql/inputObjects#ownerinput"><code>[OwnerInput!]!</code></a>
</td>
<td>
<p>The primary key of the owners</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The ownership type to remove, optional. By default will remove regardless of ownership type.</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to attach the owners to</p>
</td>
</tr>
</tbody>
</table>

## BatchAddTagsInput

Input provided when adding tags to a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tagUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Tags</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput!]!</code></a>
</td>
<td>
<p>The target assets to attach the tags to</p>
</td>
</tr>
</tbody>
</table>

## BatchAddTermsInput

Input provided when adding glossary terms to a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
termUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Glossary Terms</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to attach the glossary terms to</p>
</td>
</tr>
</tbody>
</table>

## BatchAssignRoleInput

Input provided when batch assigning a role to a list of users

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
roleUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the role to assign to the actors. If undefined, will remove the role.</p>
</td>
</tr>
<tr>
<td>
actors<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The urns of the actors to assign the role to</p>
</td>
</tr>
</tbody>
</table>

## BatchDatasetUpdateInput

Arguments provided to batch update Dataset entities

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
<p>Primary key of the Dataset to which the update will be applied</p>
</td>
</tr>
<tr>
<td>
update<br />
<a href="/docs/graphql/inputObjects#datasetupdateinput"><code>DatasetUpdateInput!</code></a>
</td>
<td>
<p>Arguments provided to update the Dataset</p>
</td>
</tr>
</tbody>
</table>

## BatchGetStepStatesInput

Input arguments required for fetching step states

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ids<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The unique ids for the steps to retrieve</p>
</td>
</tr>
</tbody>
</table>

## BatchRemoveOwnersInput

Input provided when removing owners from a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownerUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the owners</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The ownership type to remove, optional. By default will remove regardless of ownership type.</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to remove the owners from</p>
</td>
</tr>
</tbody>
</table>

## BatchRemoveTagsInput

Input provided when removing tags from a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tagUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Tags</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to remove the tags from</p>
</td>
</tr>
</tbody>
</table>

## BatchRemoveTermsInput

Input provided when removing glossary terms from a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
termUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Glossary Terms</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to remove the glossary terms from</p>
</td>
</tr>
</tbody>
</table>

## BatchSetDataProductInput

Input properties required for batch setting a DataProduct on other entities

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
dataProductUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the data product you are setting on a group of resources.
If this is null, the Data Product will be unset for the given resources.</p>
</td>
</tr>
<tr>
<td>
resourceUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The urns of the entities the given data product should be set on</p>
</td>
</tr>
</tbody>
</table>

## BatchSetDomainInput

Input provided when adding tags to a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
domainUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The primary key of the Domain, or null if the domain will be unset</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput!]!</code></a>
</td>
<td>
<p>The target assets to attach the Domain</p>
</td>
</tr>
</tbody>
</table>

## BatchUpdateDeprecationInput

Input provided when updating the deprecation status for a batch of assets.

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
<p>Whether the Entity is marked as deprecated.</p>
</td>
</tr>
<tr>
<td>
decommissionTime<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Optional - The time user plan to decommission this entity</p>
</td>
</tr>
<tr>
<td>
note<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - Additional information about the entity deprecation plan</p>
</td>
</tr>
<tr>
<td>
resources<br />
<a href="/docs/graphql/inputObjects#resourcerefinput"><code>[ResourceRefInput]!</code></a>
</td>
<td>
<p>The target assets to attach the tags to</p>
</td>
</tr>
</tbody>
</table>

## BatchUpdateSoftDeletedInput

Input provided when updating the soft-deleted status for a batch of assets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The urns of the assets to soft delete</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether to mark the asset as soft-deleted (hidden)</p>
</td>
</tr>
</tbody>
</table>

## BatchUpdateStepStatesInput

Input arguments required for updating step states

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
states<br />
<a href="/docs/graphql/inputObjects#stepstateinput"><code>[StepStateInput!]!</code></a>
</td>
<td>
<p>Set of step states. If the id does not exist, it will be created.</p>
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
<blockquote>Deprecated: Use `orFilters`- they are more expressive</blockquote>

<p>Deprecated in favor of the more expressive orFilters field
Facet filters to apply to search results. These will be &#39;AND&#39;-ed together.</p>
</td>
</tr>
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
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

## BrowseV2Input

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
<p>The browse path V2 - a list with each entry being part of the browse path V2</p>
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
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The search query string</p>
</td>
</tr>
</tbody>
</table>

## CancelIngestionExecutionRequestInput

Input for cancelling an execution request input

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ingestionSourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the ingestion source</p>
</td>
</tr>
<tr>
<td>
executionRequestUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the specific execution request to cancel</p>
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

## ContainerEntitiesInput

Input required to fetch the entities inside of a container.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional query filter for particular entities inside the container</p>
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
<p>Optional Facet filters to apply to the result set</p>
</td>
</tr>
</tbody>
</table>

## CorpGroupUpdateInput

Arguments provided to update a CorpGroup Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>DataHub description of the group</p>
</td>
</tr>
<tr>
<td>
slack<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Slack handle for the group</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Email address for the group</p>
</td>
</tr>
</tbody>
</table>

## CorpUserUpdateInput

Arguments provided to update a CorpUser Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name to show on DataHub</p>
</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Title to show on DataHub</p>
</td>
</tr>
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
<tr>
<td>
slack<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The slack handle of the user</p>
</td>
</tr>
<tr>
<td>
phone<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Phone number for the user</p>
</td>
</tr>
<tr>
<td>
email<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Email address for the user</p>
</td>
</tr>
</tbody>
</table>

## CreateAccessTokenInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#accesstokentype"><code>AccessTokenType!</code></a>
</td>
<td>
<p>The type of the Access Token.</p>
</td>
</tr>
<tr>
<td>
actorUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The actor associated with the Access Token.</p>
</td>
</tr>
<tr>
<td>
duration<br />
<a href="/docs/graphql/enums#accesstokenduration"><code>AccessTokenDuration!</code></a>
</td>
<td>
<p>The duration for which the Access Token is valid.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the token to be generated.</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the token if defined.</p>
</td>
</tr>
</tbody>
</table>

## CreateDataProductInput

Input required for creating a DataProduct.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
properties<br />
<a href="/docs/graphql/inputObjects#createdataproductpropertiesinput"><code>CreateDataProductPropertiesInput!</code></a>
</td>
<td>
<p>Properties about the Query</p>
</td>
</tr>
<tr>
<td>
domainUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Domain</p>
</td>
</tr>
</tbody>
</table>

## CreateDataProductPropertiesInput

Input properties required for creating a DataProduct

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A display name for the DataProduct</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description for the DataProduct</p>
</td>
</tr>
</tbody>
</table>

## CreateDomainInput

Input required to create a new Domain.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name for the Domain</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional description for the Domain</p>
</td>
</tr>
</tbody>
</table>

## CreateGlossaryEntityInput

Input required to create a new Glossary Entity - a Node or a Term.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name for the Node or Term</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description for the Node or Term</p>
</td>
</tr>
<tr>
<td>
parentNode<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional parent node urn for the Glossary Node or Term</p>
</td>
</tr>
</tbody>
</table>

## CreateGroupInput

Input for creating a new group

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional! A custom id to use as the primary key identifier for the group. If not provided, a random UUID will be generated as the id.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name of the group</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description of the group</p>
</td>
</tr>
</tbody>
</table>

## CreateIngestionExecutionRequestInput

Input for creating an execution request input

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ingestionSourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the ingestion source to execute</p>
</td>
</tr>
</tbody>
</table>

## CreateInviteTokenInput

Input provided when creating an invite token

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
roleUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the role to create the invite token for</p>
</td>
</tr>
</tbody>
</table>

## CreateNativeUserResetTokenInput

Input required to generate a password reset token for a native user.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
userUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the user to reset the password of</p>
</td>
</tr>
</tbody>
</table>

## CreateOwnershipTypeInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Custom Ownership Type</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The description of the Custom Ownership Type</p>
</td>
</tr>
</tbody>
</table>

## CreatePostInput

Input provided when creating a Post

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
postType<br />
<a href="/docs/graphql/enums#posttype"><code>PostType!</code></a>
</td>
<td>
<p>The type of post</p>
</td>
</tr>
<tr>
<td>
content<br />
<a href="/docs/graphql/inputObjects#updatepostcontentinput"><code>UpdatePostContentInput!</code></a>
</td>
<td>
<p>The content of the post</p>
</td>
</tr>
</tbody>
</table>

## CreateQueryInput

Input required for creating a Query. Requires the 'Edit Queries' privilege for all query subjects.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
properties<br />
<a href="/docs/graphql/inputObjects#createquerypropertiesinput"><code>CreateQueryPropertiesInput!</code></a>
</td>
<td>
<p>Properties about the Query</p>
</td>
</tr>
<tr>
<td>
subjects<br />
<a href="/docs/graphql/inputObjects#createquerysubjectinput"><code>[CreateQuerySubjectInput!]!</code></a>
</td>
<td>
<p>Subjects for the query</p>
</td>
</tr>
</tbody>
</table>

## CreateQueryPropertiesInput

Input properties required for creating a Query

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional display name for the Query</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description for the Query</p>
</td>
</tr>
<tr>
<td>
statement<br />
<a href="/docs/graphql/inputObjects#querystatementinput"><code>QueryStatementInput!</code></a>
</td>
<td>
<p>The Query contents</p>
</td>
</tr>
</tbody>
</table>

## CreateQuerySubjectInput

Input required for creating a Query. For now, only datasets are supported.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the dataset that is the subject of the query</p>
</td>
</tr>
</tbody>
</table>

## CreateSecretInput

Input arguments for creating a new Secret

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the secret for reference in ingestion recipes</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The value of the secret, to be encrypted and stored</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description for the secret</p>
</td>
</tr>
</tbody>
</table>

## CreateTagInput

Input required to create a new Tag

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional! A custom id to use as the primary key identifier for the Tag. If not provided, a random UUID will be generated as the id.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Display name for the Tag</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional description for the Tag</p>
</td>
</tr>
</tbody>
</table>

## CreateTestConnectionRequestInput

Input for creating a test connection request

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
recipe<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A JSON-encoded recipe</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Advanced: The version of the ingestion framework to use</p>
</td>
</tr>
</tbody>
</table>

## CreateTestInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Advanced: a custom id for the test.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Test</p>
</td>
</tr>
<tr>
<td>
category<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The category of the Test (user defined)</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the test</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/inputObjects#testdefinitioninput"><code>TestDefinitionInput!</code></a>
</td>
<td>
<p>The test definition</p>
</td>
</tr>
</tbody>
</table>

## CreateViewInput

Input provided when creating a DataHub View

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
viewType<br />
<a href="/docs/graphql/enums#datahubviewtype"><code>DataHubViewType!</code></a>
</td>
<td>
<p>The type of View</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the View</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description of the View</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/inputObjects#datahubviewdefinitioninput"><code>DataHubViewDefinitionInput!</code></a>
</td>
<td>
<p>The view definition itself</p>
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

Arguments provided to update a Dashboard Entity

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

## DataHubViewDefinitionInput

Input required for creating a DataHub View Definition

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entityTypes<br />
<a href="/docs/graphql/enums#entitytype"><code>[EntityType!]!</code></a>
</td>
<td>
<p>A set of entity types that the view applies for. If left empty, then ALL entities will be in scope.</p>
</td>
</tr>
<tr>
<td>
filter<br />
<a href="/docs/graphql/inputObjects#datahubviewfilterinput"><code>DataHubViewFilterInput!</code></a>
</td>
<td>
<p>A set of filters to apply.</p>
</td>
</tr>
</tbody>
</table>

## DataHubViewFilterInput

Input required for creating a DataHub View Definition

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
operator<br />
<a href="/docs/graphql/enums#logicaloperator"><code>LogicalOperator!</code></a>
</td>
<td>
<p>The operator used to combine the filters.</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]!</code></a>
</td>
<td>
<p>A set of filters combined via an operator. If left empty, then no filters will be applied.</p>
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

## DataProductEntitiesInput

Input required to fetch the entities inside of a Data Product.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional query filter for particular entities inside the Data Product</p>
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
<p>Optional Facet filters to apply to the result set</p>
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

## DescriptionUpdateInput

Incubating. Updates the description of a resource. Currently supports DatasetField descriptions only

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
<p>The new description</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the resource to attach the description to, eg dataset urn</p>
</td>
</tr>
<tr>
<td>
subResourceType<br />
<a href="/docs/graphql/enums#subresourcetype"><code>SubResourceType</code></a>
</td>
<td>
<p>An optional sub resource type</p>
</td>
</tr>
<tr>
<td>
subResource<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A sub resource identifier, eg dataset field path</p>
</td>
</tr>
</tbody>
</table>

## DomainEntitiesInput

Input required to fetch the entities inside of a Domain.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional query filter for particular entities inside the domain</p>
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
<p>Optional Facet filters to apply to the result set</p>
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

## EntityCountInput

Input for the get entity counts endpoint

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

</td>
</tr>
</tbody>
</table>

## EntityRequestContext

Context that defines an entity page requesting recommendations

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
<p>Type of the enity being displayed</p>
</td>
</tr>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the entity being displayed</p>
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
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: Prefer `values` for single elements</blockquote>

<p>Value of the field to filter by. Deprecated in favor of <code>values</code>, which should accept a single element array for a
value</p>
</td>
</tr>
<tr>
<td>
values<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Values, one of which the intended field should match.</p>
</td>
</tr>
<tr>
<td>
negated<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>If the filter should or should not be matched</p>
</td>
</tr>
<tr>
<td>
condition<br />
<a href="/docs/graphql/enums#filteroperator"><code>FilterOperator</code></a>
</td>
<td>
<p>Condition for the values. How to If unset, assumed to be equality</p>
</td>
</tr>
</tbody>
</table>

## FilterInput

A set of filter criteria

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
and<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]!</code></a>
</td>
<td>
<p>A list of conjunctive filters</p>
</td>
</tr>
</tbody>
</table>

## GetAccessTokenInput

Input required to fetch a new Access Token.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#accesstokentype"><code>AccessTokenType!</code></a>
</td>
<td>
<p>The type of the Access Token.</p>
</td>
</tr>
<tr>
<td>
actorUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The actor associated with the Access Token.</p>
</td>
</tr>
<tr>
<td>
duration<br />
<a href="/docs/graphql/enums#accesstokenduration"><code>AccessTokenDuration!</code></a>
</td>
<td>
<p>The duration for which the Access Token is valid.</p>
</td>
</tr>
</tbody>
</table>

## GetGrantedPrivilegesInput

Input for getting granted privileges

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
actorUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the actor</p>
</td>
</tr>
<tr>
<td>
resourceSpec<br />
<a href="/docs/graphql/inputObjects#resourcespec"><code>ResourceSpec</code></a>
</td>
<td>
<p>Spec to identify resource. If empty, gets privileges granted to the actor</p>
</td>
</tr>
</tbody>
</table>

## GetInviteTokenInput

Input provided when getting an invite token

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
roleUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the role to get the invite token for</p>
</td>
</tr>
</tbody>
</table>

## GetQuickFiltersInput

Input for getting Quick Filters

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
</tbody>
</table>

## GetRootGlossaryEntitiesInput

Input required when getting Business Glossary entities

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

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
<p>The number of Glossary Entities in the returned result set</p>
</td>
</tr>
</tbody>
</table>

## GetSchemaBlameInput

Input for getting schema changes computed at a specific version.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The dataset urn</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Changes after this version are not shown. If not provided, this is the latestVersion.</p>
</td>
</tr>
</tbody>
</table>

## GetSchemaVersionListInput

Input for getting list of schema versions.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The dataset urn</p>
</td>
</tr>
</tbody>
</table>

## GetSecretValuesInput

Input arguments for retrieving the plaintext values of a set of secrets

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
secrets<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>A list of secret names</p>
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

## LineageEdge

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
downstreamUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the source entity. This urn is downstream of the destinationUrn.</p>
</td>
</tr>
<tr>
<td>
upstreamUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the destination entity. This urn is upstream of the destinationUrn</p>
</td>
</tr>
</tbody>
</table>

## LineageInput

Input for the list lineage property of an Entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#lineagedirection"><code>LineageDirection!</code></a>
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
<tr>
<td>
separateSiblings<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Optional flag to not merge siblings in the response. They are merged by default.</p>
</td>
</tr>
<tr>
<td>
startTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional starting time to filter on</p>
</td>
</tr>
<tr>
<td>
endTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional ending time to filter on</p>
</td>
</tr>
</tbody>
</table>

## ListAccessTokenInput

Input arguments for listing access tokens

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

## ListDomainsInput

Input required when listing DataHub Domains

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
<p>The maximum number of Domains to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListGlobalViewsInput

Input provided when listing DataHub Global Views

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
<p>The maximum number of Views to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListGroupsInput

Input required when listing DataHub Groups

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
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListIngestionSourcesInput

Input arguments for listing Ingestion Sources

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
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListMyViewsInput

Input provided when listing DataHub Views

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
<p>The maximum number of Views to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
<tr>
<td>
viewType<br />
<a href="/docs/graphql/enums#datahubviewtype"><code>DataHubViewType</code></a>
</td>
<td>
<p>Optional - List the type of View to filter for.</p>
</td>
</tr>
</tbody>
</table>

## ListOwnershipTypesInput

Input required for listing custom ownership types entities

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
<p>The starting offset of the result set returned, default is 0</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The maximum number of Custom Ownership Types to be returned in the result set, default is 20</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Optional Facet filters to apply to the result set</p>
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
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListPostsInput

Input provided when listing existing posts

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
<p>The maximum number of Roles to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListQueriesInput

Input required for listing query entities

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
<p>The maximum number of Queries to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A raw search query</p>
</td>
</tr>
<tr>
<td>
source<br />
<a href="/docs/graphql/enums#querysource"><code>QuerySource</code></a>
</td>
<td>
<p>An optional source for the query</p>
</td>
</tr>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional Urn for the parent dataset that the query is associated with.</p>
</td>
</tr>
</tbody>
</table>

## ListRecommendationsInput

Input arguments for fetching UI recommendations

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
userUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of the actor requesting recommendations</p>
</td>
</tr>
<tr>
<td>
requestContext<br />
<a href="/docs/graphql/inputObjects#recommendationrequestcontext"><code>RecommendationRequestContext</code></a>
</td>
<td>
<p>Context provider by the caller requesting recommendations</p>
</td>
</tr>
<tr>
<td>
limit<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Max number of modules to return</p>
</td>
</tr>
</tbody>
</table>

## ListRolesInput

Input provided when listing existing roles

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
<p>The maximum number of Roles to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListSecretsInput

Input for listing DataHub Secrets

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
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional search query</p>
</td>
</tr>
</tbody>
</table>

## ListTestsInput

Input required when listing DataHub Tests

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
<p>The maximum number of Domains to be returned in the result set</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional query string to match on</p>
</td>
</tr>
</tbody>
</table>

## ListUsersInput

Input required when listing DataHub Users

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
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional search query</p>
</td>
</tr>
</tbody>
</table>

## MetadataAnalyticsInput

Input to fetch metadata analytics charts

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entityType<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType</code></a>
</td>
<td>
<p>Entity type to fetch analytics for (If empty, queries across all entities)</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Urn of the domain to fetch analytics for (If empty or GLOBAL, queries across all domains)</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Search query to filter down result (If empty, does not apply any search query)</p>
</td>
</tr>
</tbody>
</table>

## NotebookEditablePropertiesUpdate

Update to writable Notebook fields

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
<p>Writable description aka documentation for a Notebook</p>
</td>
</tr>
</tbody>
</table>

## NotebookUpdateInput

Arguments provided to update a Notebook Entity

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
<a href="/docs/graphql/inputObjects#notebookeditablepropertiesupdate"><code>NotebookEditablePropertiesUpdate</code></a>
</td>
<td>
<p>Update to editable properties</p>
</td>
</tr>
</tbody>
</table>

## OwnerInput

Input provided when adding an owner to an asset

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownerUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Owner to add or remove</p>
</td>
</tr>
<tr>
<td>
ownerEntityType<br />
<a href="/docs/graphql/enums#ownerentitytype"><code>OwnerEntityType!</code></a>
</td>
<td>
<p>The owner type, either a user or group</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>The ownership type for the new owner. If none is provided, then a new NONE will be added.
Deprecated - Use ownershipTypeUrn field instead.</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the ownership type entity.</p>
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
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>The owner type. Deprecated - Use ownershipTypeUrn field instead.</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the ownership type entity.</p>
</td>
</tr>
</tbody>
</table>

## PolicyMatchCriterionInput

Criterion to define relationship between field and values

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
<p>The name of the field that the criterion refers to
e.g. entity_type, entity_urn, domain</p>
</td>
</tr>
<tr>
<td>
values<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>Values. Matches criterion if any one of the values matches condition (OR-relationship)</p>
</td>
</tr>
<tr>
<td>
condition<br />
<a href="/docs/graphql/enums#policymatchcondition"><code>PolicyMatchCondition!</code></a>
</td>
<td>
<p>The name of the field that the criterion refers to</p>
</td>
</tr>
</tbody>
</table>

## PolicyMatchFilterInput

Filter object that encodes a complex filter logic with OR + AND

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
criteria<br />
<a href="/docs/graphql/inputObjects#policymatchcriterioninput"><code>[PolicyMatchCriterionInput!]</code></a>
</td>
<td>
<p>List of criteria to apply</p>
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

## QueryStatementInput

Input required for creating a Query Statement

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The query text</p>
</td>
</tr>
<tr>
<td>
language<br />
<a href="/docs/graphql/enums#querylanguage"><code>QueryLanguage!</code></a>
</td>
<td>
<p>The query language</p>
</td>
</tr>
</tbody>
</table>

## RecommendationRequestContext

Context that defines the page requesting recommendations
i.e. for search pages, the query/filters. for entity pages, the entity urn and tab

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
scenario<br />
<a href="/docs/graphql/enums#scenariotype"><code>ScenarioType!</code></a>
</td>
<td>
<p>Scenario in which the recommendations will be displayed</p>
</td>
</tr>
<tr>
<td>
searchRequestContext<br />
<a href="/docs/graphql/inputObjects#searchrequestcontext"><code>SearchRequestContext</code></a>
</td>
<td>
<p>Additional context for defining the search page requesting recommendations</p>
</td>
</tr>
<tr>
<td>
entityRequestContext<br />
<a href="/docs/graphql/inputObjects#entityrequestcontext"><code>EntityRequestContext</code></a>
</td>
<td>
<p>Additional context for defining the entity page requesting recommendations</p>
</td>
</tr>
</tbody>
</table>

## RelatedTermsInput

Input provided when adding Terms to an asset

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
<p>The Glossary Term urn to add or remove this relationship to/from</p>
</td>
</tr>
<tr>
<td>
termUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The primary key of the Glossary Term to add or remove</p>
</td>
</tr>
<tr>
<td>
relationshipType<br />
<a href="/docs/graphql/enums#termrelationshiptype"><code>TermRelationshipType!</code></a>
</td>
<td>
<p>The type of relationship we&#39;re adding or removing to/from for a Glossary Term</p>
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

## RemoveGroupMembersInput

Input required to remove members from an external DataHub group

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groupUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The group to remove members from</p>
</td>
</tr>
<tr>
<td>
userUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The members to remove from the group</p>
</td>
</tr>
</tbody>
</table>

## RemoveLinkInput

Input provided when removing the association between a Metadata Entity and a Link

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
linkUrl<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The url of the link to add or remove, which uniquely identifies the Link</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource or entity to attach the link to, for example a dataset urn</p>
</td>
</tr>
</tbody>
</table>

## RemoveNativeGroupMembersInput

Input required to remove members from a native DataHub group

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groupUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The group to remove members from</p>
</td>
</tr>
<tr>
<td>
userUrns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>The members to remove from the group</p>
</td>
</tr>
</tbody>
</table>

## RemoveOwnerInput

Input provided when removing the association between a Metadata Entity and an user or group owner

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
ownerUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the Owner to add or remove</p>
</td>
</tr>
<tr>
<td>
ownershipTypeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The ownership type to remove, optional. By default will remove regardless of ownership type.</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource or entity to attach or remove the owner from, for example a dataset urn</p>
</td>
</tr>
</tbody>
</table>

## ReportOperationInput

Input provided to report an asset operation

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
<p>The urn of the asset (e.g. dataset) to report the operation for</p>
</td>
</tr>
<tr>
<td>
operationType<br />
<a href="/docs/graphql/enums#operationtype"><code>OperationType!</code></a>
</td>
<td>
<p>The type of operation that was performed. Required</p>
</td>
</tr>
<tr>
<td>
customOperationType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A custom type of operation. Required if operation type is CUSTOM.</p>
</td>
</tr>
<tr>
<td>
sourceType<br />
<a href="/docs/graphql/enums#operationsourcetype"><code>OperationSourceType!</code></a>
</td>
<td>
<p>The source or reporter of the operation</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/inputObjects#stringmapentryinput"><code>[StringMapEntryInput!]</code></a>
</td>
<td>
<p>A list of key-value parameters to include</p>
</td>
</tr>
<tr>
<td>
partition<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional partition identifier</p>
</td>
</tr>
<tr>
<td>
numAffectedRows<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Optional: The number of affected rows</p>
</td>
</tr>
<tr>
<td>
timestampMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Optional: Provide a timestamp associated with the operation. If not provided, one will be generated for you based
on the current time.</p>
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
<a href="/docs/graphql/scalars#string"><code>String</code></a>
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
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether of not to apply the filter to all resources of the type</p>
</td>
</tr>
<tr>
<td>
filter<br />
<a href="/docs/graphql/inputObjects#policymatchfilterinput"><code>PolicyMatchFilterInput</code></a>
</td>
<td>
<p>Whether of not to apply the filter to all resources of the type</p>
</td>
</tr>
</tbody>
</table>

## ResourceRefInput

Reference to a resource to apply an action to

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the resource being referenced</p>
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

## ResourceSpec

Spec to identify resource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resourceType<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>Resource type</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Resource urn</p>
</td>
</tr>
</tbody>
</table>

## RollbackIngestionInput

Input for rolling back an ingestion execution

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
runId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>An ingestion run ID</p>
</td>
</tr>
</tbody>
</table>

## ScrollAcrossEntitiesInput

Input arguments for a full text search query across entities, specifying a starting pointer. Allows paging beyond 10k results

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
<p>Entity types to be searched. If this is not provided, all entities will be searched.</p>
</td>
</tr>
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
scrollId<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The starting point of paginated results, an opaque ID the backend understands as a pointer</p>
</td>
</tr>
<tr>
<td>
keepAlive<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s</p>
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
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## ScrollAcrossLineageInput

Input arguments for a search query over the results of a multi-hop graph query, uses scroll API

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Urn of the source node</p>
</td>
</tr>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#lineagedirection"><code>LineageDirection!</code></a>
</td>
<td>
<p>The direction of the relationship, either incoming or outgoing from the source entity</p>
</td>
</tr>
<tr>
<td>
types<br />
<a href="/docs/graphql/enums#entitytype"><code>[EntityType!]</code></a>
</td>
<td>
<p>Entity types to be searched. If this is not provided, all entities will be searched.</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The query string</p>
</td>
</tr>
<tr>
<td>
scrollId<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The starting point of paginated results, an opaque ID the backend understands as a pointer</p>
</td>
</tr>
<tr>
<td>
keepAlive<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s</p>
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
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
startTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional starting time to filter on</p>
</td>
</tr>
<tr>
<td>
endTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional ending time to filter on</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## SearchAcrossEntitiesInput

Input arguments for a full text search query across entities

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
<p>Entity types to be searched. If this is not provided, all entities will be searched.</p>
</td>
</tr>
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
<blockquote>Deprecated: Use `orFilters`- they are more expressive</blockquote>

<p>Deprecated in favor of the more expressive orFilters field
Facet filters to apply to search results. These will be &#39;AND&#39;-ed together.</p>
</td>
</tr>
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
viewUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - A View to apply when generating results</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## SearchAcrossLineageInput

Input arguments for a search query over the results of a multi-hop graph query

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Urn of the source node</p>
</td>
</tr>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#lineagedirection"><code>LineageDirection!</code></a>
</td>
<td>
<p>The direction of the relationship, either incoming or outgoing from the source entity</p>
</td>
</tr>
<tr>
<td>
types<br />
<a href="/docs/graphql/enums#entitytype"><code>[EntityType!]</code></a>
</td>
<td>
<p>Entity types to be searched. If this is not provided, all entities will be searched.</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The query string</p>
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
<blockquote>Deprecated: Use `orFilters`- they are more expressive</blockquote>

<p>Deprecated in favor of the more expressive orFilters field
Facet filters to apply to search results. These will be &#39;AND&#39;-ed together.</p>
</td>
</tr>
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
startTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional starting time to filter on</p>
</td>
</tr>
<tr>
<td>
endTimeMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>An optional ending time to filter on</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## SearchFlags

Set of flags to control search behavior

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
skipCache<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether to skip cache</p>
</td>
</tr>
<tr>
<td>
maxAggValues<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The maximum number of values in an facet aggregation</p>
</td>
</tr>
<tr>
<td>
fulltext<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Structured or unstructured fulltext query</p>
</td>
</tr>
<tr>
<td>
skipHighlighting<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether to skip highlighting</p>
</td>
</tr>
<tr>
<td>
skipAggregates<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether to skip aggregates/facets</p>
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
<blockquote>Deprecated: Use `orFilters`- they are more expressive</blockquote>

<p>Deprecated in favor of the more expressive orFilters field
Facet filters to apply to search results. These will be &#39;AND&#39;-ed together.</p>
</td>
</tr>
<tr>
<td>
orFilters<br />
<a href="/docs/graphql/inputObjects#andfilterinput"><code>[AndFilterInput!]</code></a>
</td>
<td>
<p>A list of disjunctive criterion for the filter. (or operation to combine filters)</p>
</td>
</tr>
<tr>
<td>
searchFlags<br />
<a href="/docs/graphql/inputObjects#searchflags"><code>SearchFlags</code></a>
</td>
<td>
<p>Flags controlling search options</p>
</td>
</tr>
</tbody>
</table>

## SearchRequestContext

Context that defines a search page requesting recommendatinos

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Search query</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/inputObjects#facetfilterinput"><code>[FacetFilterInput!]</code></a>
</td>
<td>
<p>Faceted filters applied to search results</p>
</td>
</tr>
</tbody>
</table>

## StepStateInput

The input required to update the state of a step

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The globally unique id for the step</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/inputObjects#stringmapentryinput"><code>[StringMapEntryInput]!</code></a>
</td>
<td>
<p>The new properties for the step</p>
</td>
</tr>
</tbody>
</table>

## StringMapEntryInput

String map entry input

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

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

## TestDefinitionInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
json<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The string representation of the Test</p>
</td>
</tr>
</tbody>
</table>

## UpdateCorpUserViewsSettingsInput

Input required to update a users settings.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
defaultView<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The URN of the View that serves as this user&#39;s personal default.
If not provided, any existing default view will be removed.</p>
</td>
</tr>
</tbody>
</table>

## UpdateDataProductInput

Input properties required for update a DataProduct

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A display name for the DataProduct</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description for the DataProduct</p>
</td>
</tr>
</tbody>
</table>

## UpdateDeprecationInput

Input provided when setting the Deprecation status for an Entity.

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
<p>The urn of the Entity to set deprecation for.</p>
</td>
</tr>
<tr>
<td>
deprecated<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the Entity is marked as deprecated.</p>
</td>
</tr>
<tr>
<td>
decommissionTime<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Optional - The time user plan to decommission this entity</p>
</td>
</tr>
<tr>
<td>
note<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional - Additional information about the entity deprecation plan</p>
</td>
</tr>
</tbody>
</table>

## UpdateEmbedInput

Input required to set or clear information related to rendering a Data Asset inside of DataHub.

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
<p>The URN associated with the Data Asset to update. Only dataset, dashboard, and chart urns are currently supported.</p>
</td>
</tr>
<tr>
<td>
renderUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Set or clear a URL used to render an embedded asset.</p>
</td>
</tr>
</tbody>
</table>

## UpdateGlobalViewsSettingsInput

Input required to update Global View Settings.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
defaultView<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The URN of the View that serves as the Global, or organization-wide, default.
If this field is not provided, the existing Global Default will be cleared.</p>
</td>
</tr>
</tbody>
</table>

## UpdateIngestionSourceConfigInput

Input parameters for creating / updating an Ingestion Source

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
recipe<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A JSON-encoded recipe</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The version of DataHub Ingestion Framework to use when executing the recipe.</p>
</td>
</tr>
<tr>
<td>
executorId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The id of the executor to use for executing the recipe</p>
</td>
</tr>
<tr>
<td>
debugMode<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not to run ingestion in debug mode</p>
</td>
</tr>
</tbody>
</table>

## UpdateIngestionSourceInput

Input arguments for creating / updating an Ingestion Source

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A name associated with the ingestion source</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe.</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description associated with the ingestion source</p>
</td>
</tr>
<tr>
<td>
schedule<br />
<a href="/docs/graphql/inputObjects#updateingestionsourcescheduleinput"><code>UpdateIngestionSourceScheduleInput</code></a>
</td>
<td>
<p>An optional schedule for the ingestion source. If not provided, the source is only available for run on-demand.</p>
</td>
</tr>
<tr>
<td>
config<br />
<a href="/docs/graphql/inputObjects#updateingestionsourceconfiginput"><code>UpdateIngestionSourceConfigInput!</code></a>
</td>
<td>
<p>A set of type-specific ingestion source configurations</p>
</td>
</tr>
</tbody>
</table>

## UpdateIngestionSourceScheduleInput

Input arguments for creating / updating the schedule of an Ingestion Source

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
interval<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The cron-formatted interval describing when the job should be executed</p>
</td>
</tr>
<tr>
<td>
timezone<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the timezone in which the cron interval should be scheduled (e.g. America/Los Angeles)</p>
</td>
</tr>
</tbody>
</table>

## UpdateLineageInput

Input required in order to upsert lineage edges

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
edgesToAdd<br />
<a href="/docs/graphql/inputObjects#lineageedge"><code>[LineageEdge]!</code></a>
</td>
<td>
<p>New lineage edges to upsert</p>
</td>
</tr>
<tr>
<td>
edgesToRemove<br />
<a href="/docs/graphql/inputObjects#lineageedge"><code>[LineageEdge]!</code></a>
</td>
<td>
<p>Lineage edges to remove. Takes precedence over edgesToAdd - so edges existing both edgesToAdd
and edgesToRemove will be removed.</p>
</td>
</tr>
</tbody>
</table>

## UpdateMediaInput

Input provided for filling in a post content

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#mediatype"><code>MediaType!</code></a>
</td>
<td>
<p>The type of media</p>
</td>
</tr>
<tr>
<td>
location<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The location of the media (a URL)</p>
</td>
</tr>
</tbody>
</table>

## UpdateNameInput

Input for updating the name of an entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The new name</p>
</td>
</tr>
<tr>
<td>
urn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the resource to update the name for</p>
</td>
</tr>
</tbody>
</table>

## UpdateOwnershipTypeInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name of the Custom Ownership Type</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description of the Custom Ownership Type</p>
</td>
</tr>
</tbody>
</table>

## UpdateParentNodeInput

Input for updating the parent node of a resource. Currently only GlossaryNodes and GlossaryTerms have parentNodes.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
parentNode<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The new parent node urn. If parentNode is null, this will remove the parent from this entity</p>
</td>
</tr>
<tr>
<td>
resourceUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The primary key of the resource to update the parent node for</p>
</td>
</tr>
</tbody>
</table>

## UpdatePostContentInput

Input provided for filling in a post content

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
contentType<br />
<a href="/docs/graphql/enums#postcontenttype"><code>PostContentType!</code></a>
</td>
<td>
<p>The type of post content</p>
</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The title of the post</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional content of the post</p>
</td>
</tr>
<tr>
<td>
link<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional link that the post is associated with</p>
</td>
</tr>
<tr>
<td>
media<br />
<a href="/docs/graphql/inputObjects#updatemediainput"><code>UpdateMediaInput</code></a>
</td>
<td>
<p>Optional media contained in the post</p>
</td>
</tr>
</tbody>
</table>

## UpdateQueryInput

Input required for updating an existing Query. Requires the 'Edit Queries' privilege for all query subjects.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
properties<br />
<a href="/docs/graphql/inputObjects#updatequerypropertiesinput"><code>UpdateQueryPropertiesInput</code></a>
</td>
<td>
<p>Properties about the Query</p>
</td>
</tr>
<tr>
<td>
subjects<br />
<a href="/docs/graphql/inputObjects#updatequerysubjectinput"><code>[UpdateQuerySubjectInput!]</code></a>
</td>
<td>
<p>Subjects for the query</p>
</td>
</tr>
</tbody>
</table>

## UpdateQueryPropertiesInput

Input properties required for creating a Query. Any non-null fields will be updated if provided.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional display name for the Query</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description for the Query</p>
</td>
</tr>
<tr>
<td>
statement<br />
<a href="/docs/graphql/inputObjects#querystatementinput"><code>QueryStatementInput</code></a>
</td>
<td>
<p>The Query contents</p>
</td>
</tr>
</tbody>
</table>

## UpdateQuerySubjectInput

Input required for creating a Query. For now, only datasets are supported.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the dataset that is the subject of the query</p>
</td>
</tr>
</tbody>
</table>

## UpdateTestInput

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Test</p>
</td>
</tr>
<tr>
<td>
category<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The category of the Test (user defined)</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the test</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/inputObjects#testdefinitioninput"><code>TestDefinitionInput!</code></a>
</td>
<td>
<p>The test definition</p>
</td>
</tr>
</tbody>
</table>

## UpdateUserSettingInput

Input for updating a user setting

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/enums#usersetting"><code>UserSetting!</code></a>
</td>
<td>
<p>The name of the setting</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>The new value of the setting</p>
</td>
</tr>
</tbody>
</table>

## UpdateViewInput

Input provided when updating a DataHub View

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name of the View</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional description of the View</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/inputObjects#datahubviewdefinitioninput"><code>DataHubViewDefinitionInput</code></a>
</td>
<td>
<p>The view definition itself</p>
</td>
</tr>
</tbody>
</table>
