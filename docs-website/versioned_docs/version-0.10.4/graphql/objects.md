---
id: objects
title: Objects
slug: objects
sidebar_position: 3
---

## Access

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
roles<br />
<a href="/docs/graphql/objects#roleassociation"><code>[RoleAssociation!]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## AccessToken

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
accessToken<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The access token itself</p>
</td>
</tr>
<tr>
<td>
metadata<br />
<a href="/docs/graphql/objects#accesstokenmetadata"><code>AccessTokenMetadata</code></a>
</td>
<td>
<p>Metadata about the generated token</p>
</td>
</tr>
</tbody>
</table>

## AccessTokenMetadata

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
<p>The primary key of the access token</p>
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
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The unique identifier of the token.</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the token, if it exists.</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description of the token if defined.</p>
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
ownerUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The actor who created the Access Token.</p>
</td>
</tr>
<tr>
<td>
createdAt<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The time when token was generated at.</p>
</td>
</tr>
<tr>
<td>
expiresAt<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Time when token will be expired.</p>
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
</tbody>
</table>

## Actor

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
users<br />
<a href="/docs/graphql/objects#roleuser"><code>[RoleUser!]</code></a>
</td>
<td>
<p>List of users for which the role is provisioned</p>
</td>
</tr>
</tbody>
</table>

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
roles<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>A disjunctive set of roles to apply the policy to</p>
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
resolvedOwnershipTypes<br />
<a href="/docs/graphql/objects#ownershiptypeentity"><code>[OwnershipTypeEntity!]</code></a>
</td>
<td>
<p>Set of OwnershipTypes to apply the policy to (if resourceOwners field is set to True), resolved.</p>
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
<tr>
<td>
resolvedUsers<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<p>The list of users on the Policy, resolved.</p>
</td>
</tr>
<tr>
<td>
resolvedGroups<br />
<a href="/docs/graphql/objects#corpgroup"><code>[CorpGroup!]</code></a>
</td>
<td>
<p>The list of groups on the Policy, resolved.</p>
</td>
</tr>
<tr>
<td>
resolvedRoles<br />
<a href="/docs/graphql/objects#datahubrole"><code>[DataHubRole!]</code></a>
</td>
<td>
<p>The list of roles on the Policy, resolved.</p>
</td>
</tr>
</tbody>
</table>

## AggregateResults

Results returned from aggregateAcrossEntities

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
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
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>Entity corresponding to the facet field</p>
</td>
</tr>
</tbody>
</table>

## AnalyticsChartGroup

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groupId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
charts<br />
<a href="/docs/graphql/unions#analyticschart"><code>[AnalyticsChart!]!</code></a>
</td>
<td>

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
appVersion<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>App version</p>
</td>
</tr>
<tr>
<td>
authConfig<br />
<a href="/docs/graphql/objects#authconfig"><code>AuthConfig!</code></a>
</td>
<td>
<p>Auth-related configurations</p>
</td>
</tr>
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
<tr>
<td>
identityManagementConfig<br />
<a href="/docs/graphql/objects#identitymanagementconfig"><code>IdentityManagementConfig!</code></a>
</td>
<td>
<p>Configurations related to the User &amp; Group management</p>
</td>
</tr>
<tr>
<td>
managedIngestionConfig<br />
<a href="/docs/graphql/objects#managedingestionconfig"><code>ManagedIngestionConfig!</code></a>
</td>
<td>
<p>Configurations related to UI-based ingestion</p>
</td>
</tr>
<tr>
<td>
lineageConfig<br />
<a href="/docs/graphql/objects#lineageconfig"><code>LineageConfig!</code></a>
</td>
<td>
<p>Configurations related to Lineage</p>
</td>
</tr>
<tr>
<td>
visualConfig<br />
<a href="/docs/graphql/objects#visualconfig"><code>VisualConfig!</code></a>
</td>
<td>
<p>Configurations related to visual appearance, allows styling the UI without rebuilding the bundle</p>
</td>
</tr>
<tr>
<td>
telemetryConfig<br />
<a href="/docs/graphql/objects#telemetryconfig"><code>TelemetryConfig!</code></a>
</td>
<td>
<p>Configurations related to tracking users in the app</p>
</td>
</tr>
<tr>
<td>
testsConfig<br />
<a href="/docs/graphql/objects#testsconfig"><code>TestsConfig!</code></a>
</td>
<td>
<p>Configurations related to DataHub tests</p>
</td>
</tr>
<tr>
<td>
viewsConfig<br />
<a href="/docs/graphql/objects#viewsconfig"><code>ViewsConfig!</code></a>
</td>
<td>
<p>Configurations related to DataHub Views</p>
</td>
</tr>
<tr>
<td>
featureFlags<br />
<a href="/docs/graphql/objects#featureflagsconfig"><code>FeatureFlagsConfig!</code></a>
</td>
<td>
<p>Feature flags telling the UI whether a feature is enabled or not</p>
</td>
</tr>
</tbody>
</table>

## AspectRenderSpec

Details for the frontend on how the raw aspect should be rendered

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
displayType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Format the aspect should be displayed in for the UI. Powered by the renderSpec annotation on the aspect model</p>
</td>
</tr>
<tr>
<td>
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Name to refer to the aspect type by for the UI. Powered by the renderSpec annotation on the aspect model</p>
</td>
</tr>
<tr>
<td>
key<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Field in the aspect payload to index into for rendering.</p>
</td>
</tr>
</tbody>
</table>

## Assertion

An assertion represents a programmatic validation, check, or test performed periodically against another Entity.

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
<p>The primary key of the Assertion</p>
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
<p>Standardized platform urn where the assertion is evaluated</p>
</td>
</tr>
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#assertioninfo"><code>AssertionInfo</code></a>
</td>
<td>
<p>Details about assertion</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
</td>
</tr>
<tr>
<td>
runEvents<br />
<a href="/docs/graphql/objects#assertionruneventsresult"><code>AssertionRunEventsResult</code></a>
</td>
<td>
<p>Lifecycle events detailing individual runs of this assertion. If startTimeMillis &amp; endTimeMillis are not provided, the most
recent events will be returned.</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
status<br />
<a href="/docs/graphql/enums#assertionrunstatus"><code>AssertionRunStatus</code></a>
</td>
<td>

</td>
</tr>
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
filter<br />
<a href="/docs/graphql/inputObjects#filterinput"><code>FilterInput</code></a>
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

## AssertionInfo

Type of assertion. Assertion types can evolve to span Datasets, Flows (Pipelines), Models, Features etc.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#assertiontype"><code>AssertionType!</code></a>
</td>
<td>
<p>Top-level type of the assertion.</p>
</td>
</tr>
<tr>
<td>
datasetAssertion<br />
<a href="/docs/graphql/objects#datasetassertioninfo"><code>DatasetAssertionInfo</code></a>
</td>
<td>
<p>Dataset-specific assertion information</p>
</td>
</tr>
</tbody>
</table>

## AssertionResult

The result of evaluating an assertion.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#assertionresulttype"><code>AssertionResultType!</code></a>
</td>
<td>
<p>The final result, e.g. either SUCCESS or FAILURE.</p>
</td>
</tr>
<tr>
<td>
rowCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Number of rows for evaluated batch</p>
</td>
</tr>
<tr>
<td>
missingCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Number of rows with missing value for evaluated batch</p>
</td>
</tr>
<tr>
<td>
unexpectedCount<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Number of rows with unexpected value for evaluated batch</p>
</td>
</tr>
<tr>
<td>
actualAggValue<br />
<a href="/docs/graphql/scalars#float"><code>Float</code></a>
</td>
<td>
<p>Observed aggregate value for evaluated batch</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>URL where full results are available</p>
</td>
</tr>
<tr>
<td>
nativeResults<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Native results / properties of evaluation</p>
</td>
</tr>
</tbody>
</table>

## AssertionRunEvent

An event representing an event in the assertion evaluation lifecycle.

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
<p>The time at which the assertion was evaluated</p>
</td>
</tr>
<tr>
<td>
assertionUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of assertion which is evaluated</p>
</td>
</tr>
<tr>
<td>
asserteeUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Urn of entity on which the assertion is applicable</p>
</td>
</tr>
<tr>
<td>
runId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Native (platform-specific) identifier for this run</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/enums#assertionrunstatus"><code>AssertionRunStatus!</code></a>
</td>
<td>
<p>The status of the assertion run as per this timeseries event.</p>
</td>
</tr>
<tr>
<td>
batchSpec<br />
<a href="/docs/graphql/objects#batchspec"><code>BatchSpec</code></a>
</td>
<td>
<p>Specification of the batch which this run is evaluating</p>
</td>
</tr>
<tr>
<td>
partitionSpec<br />
<a href="/docs/graphql/objects#partitionspec"><code>PartitionSpec</code></a>
</td>
<td>
<p>Information about the partition that was evaluated</p>
</td>
</tr>
<tr>
<td>
runtimeContext<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Runtime parameters of evaluation</p>
</td>
</tr>
<tr>
<td>
result<br />
<a href="/docs/graphql/objects#assertionresult"><code>AssertionResult</code></a>
</td>
<td>
<p>Results of assertion, present if the status is COMPLETE</p>
</td>
</tr>
</tbody>
</table>

## AssertionRunEventsResult

Result returned when fetching run events for an assertion.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of run events returned</p>
</td>
</tr>
<tr>
<td>
failed<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of failed run events</p>
</td>
</tr>
<tr>
<td>
succeeded<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of succeeded run events</p>
</td>
</tr>
<tr>
<td>
runEvents<br />
<a href="/docs/graphql/objects#assertionrunevent"><code>[AssertionRunEvent!]!</code></a>
</td>
<td>
<p>The run events themselves</p>
</td>
</tr>
</tbody>
</table>

## AssertionStdParameter

Parameter for AssertionStdOperator.

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
<p>The parameter value</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#assertionstdparametertype"><code>AssertionStdParameterType!</code></a>
</td>
<td>
<p>The type of the parameter</p>
</td>
</tr>
</tbody>
</table>

## AssertionStdParameters

Parameters for AssertionStdOperators

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
value<br />
<a href="/docs/graphql/objects#assertionstdparameter"><code>AssertionStdParameter</code></a>
</td>
<td>
<p>The value parameter of an assertion</p>
</td>
</tr>
<tr>
<td>
maxValue<br />
<a href="/docs/graphql/objects#assertionstdparameter"><code>AssertionStdParameter</code></a>
</td>
<td>
<p>The maxValue parameter of an assertion</p>
</td>
</tr>
<tr>
<td>
minValue<br />
<a href="/docs/graphql/objects#assertionstdparameter"><code>AssertionStdParameter</code></a>
</td>
<td>
<p>The minValue parameter of an assertion</p>
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

## AuthConfig

Configurations related to auth

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
tokenAuthEnabled<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether token-based auth is enabled.</p>
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
<tr>
<td>
entities<br />
<a href="/docs/graphql/interfaces#entity"><code>[Entity!]!</code></a>
</td>
<td>
<p>A list of entities to render in autocomplete</p>
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
<tr>
<td>
entities<br />
<a href="/docs/graphql/interfaces#entity"><code>[Entity!]!</code></a>
</td>
<td>
<p>A list of entities to render in autocomplete</p>
</td>
</tr>
</tbody>
</table>

## BarChart

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
bars<br />
<a href="/docs/graphql/objects#namedbar"><code>[NamedBar!]!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## BarSegment

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
label<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>

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

## BatchGetStepStatesResult

Result returned when fetching step state

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
results<br />
<a href="/docs/graphql/objects#stepstateresult"><code>[StepStateResult!]!</code></a>
</td>
<td>
<p>The step states</p>
</td>
</tr>
</tbody>
</table>

## BatchSpec

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
nativeBatchId<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The native identifier as specified by the system operating on the batch.</p>
</td>
</tr>
<tr>
<td>
query<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A query that identifies a batch of data</p>
</td>
</tr>
<tr>
<td>
limit<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Any limit to the number of rows in the batch, if applied</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Custom properties of the Batch</p>
</td>
</tr>
</tbody>
</table>

## BatchUpdateStepStatesResult

Result returned when fetching step state

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
results<br />
<a href="/docs/graphql/objects#updatestepstateresult"><code>[UpdateStepStateResult!]!</code></a>
</td>
<td>
<p>Results for each step</p>
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

## BrowsePathEntry

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
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>An optional entity associated with this browse entry. This will usually be a container entity.
If this entity is not populated, the name must be used.</p>
</td>
</tr>
</tbody>
</table>

## BrowsePathV2

A hierarchical entity path V2

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
path<br />
<a href="/docs/graphql/objects#browsepathentry"><code>[BrowsePathEntry!]!</code></a>
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

## BrowseResultGroupV2

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
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>An optional entity associated with this browse group. This will usually be a container entity.
If this entity is not populated, the name must be used.</p>
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
<tr>
<td>
hasSubGroups<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether or not there are any more groups underneath this group</p>
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

## BrowseResultsV2

The results of a browse path V2 traversal query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
groups<br />
<a href="/docs/graphql/objects#browseresultgroupv2"><code>[BrowseResultGroupV2!]!</code></a>
</td>
<td>
<p>The groups present at the provided browse path V2</p>
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
<p>The number of groups included in the results</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of browse groups under the path with filters applied</p>
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

## Cell

For consumption by UI only

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

</td>
</tr>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
linkParams<br />
<a href="/docs/graphql/objects#linkparams"><code>LinkParams</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## ChangeAuditStamps

Captures information about who created/last modified/deleted the entity and when

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the creation</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An AuditStamp corresponding to the modification</p>
</td>
</tr>
<tr>
<td>
deleted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An optional AuditStamp corresponding to the deletion</p>
</td>
</tr>
</tbody>
</table>

## Chart

A Chart Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
</td>
</tr>
<tr>
<td>
container<br />
<a href="/docs/graphql/objects#container"><code>Container</code></a>
</td>
<td>
<p>The parent container in which the entity resides</p>
</td>
</tr>
<tr>
<td>
parentContainers<br />
<a href="/docs/graphql/objects#parentcontainersresult"><code>ParentContainersResult</code></a>
</td>
<td>
<p>Recursively get the lineage of containers for this entity</p>
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
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the chart</p>
</td>
</tr>
<tr>
<td>
embed<br />
<a href="/docs/graphql/objects#embed"><code>Embed</code></a>
</td>
<td>
<p>Embed information about the Chart</p>
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
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dashboard</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the dashboard</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Chart</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
</td>
</tr>
<tr>
<td>
statsSummary<br />
<a href="/docs/graphql/objects#chartstatssummary"><code>ChartStatsSummary</code></a>
</td>
<td>
<p>Not yet implemented.</p>
<p>Experimental - Summary operational &amp; usage statistics about a Chart</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the chart. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
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
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the chart is defined</p>
</td>
</tr>
<tr>
<td>
inputFields<br />
<a href="/docs/graphql/objects#inputfields"><code>InputFields</code></a>
</td>
<td>
<p>Input fields to power the chart</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## ChartCell

A Notebook cell which contains chart as content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
cellTitle<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Title of the cell</p>
</td>
</tr>
<tr>
<td>
cellId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id for the cell.</p>
</td>
</tr>
<tr>
<td>
changeAuditStamps<br />
<a href="/docs/graphql/objects#changeauditstamps"><code>ChangeAuditStamps</code></a>
</td>
<td>
<p>Captures information about who created/last modified/deleted this TextCell and when</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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

## ChartStatsSummary

Experimental - subject to change. A summary of usage metrics about a Chart.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
viewCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total view count for the chart</p>
</td>
</tr>
<tr>
<td>
viewCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The view count in the last 30 days</p>
</td>
</tr>
<tr>
<td>
uniqueUserCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The unique user count in the past 30 days</p>
</td>
</tr>
<tr>
<td>
topUsersLast30Days<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<p>The top users in the past 30 days</p>
</td>
</tr>
</tbody>
</table>

## Container

A container of other Metadata Entities

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
<p>The primary key of the container</p>
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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform.</p>
</td>
</tr>
<tr>
<td>
container<br />
<a href="/docs/graphql/objects#container"><code>Container</code></a>
</td>
<td>
<p>Fetch an Entity Container by primary key (urn)</p>
</td>
</tr>
<tr>
<td>
parentContainers<br />
<a href="/docs/graphql/objects#parentcontainersresult"><code>ParentContainersResult</code></a>
</td>
<td>
<p>Recursively get the lineage of containers for this entity</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#containerproperties"><code>ContainerProperties</code></a>
</td>
<td>
<p>Read-only properties that originate in the source data platform</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#containereditableproperties"><code>ContainerEditableProperties</code></a>
</td>
<td>
<p>Read-write properties that originate in DataHub</p>
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
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dataset</p>
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
subTypes<br />
<a href="/docs/graphql/objects#subtypes"><code>SubTypes</code></a>
</td>
<td>
<p>Sub types of the container, e.g. &quot;Database&quot; etc</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the container</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
</td>
</tr>
<tr>
<td>
entities<br />
<a href="/docs/graphql/objects#searchresults"><code>SearchResults</code></a>
</td>
<td>
<p>Children entities inside of the Container</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#containerentitiesinput"><code>ContainerEntitiesInput</code></a>
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
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the container</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## ContainerEditableProperties

Read-write properties that originate in DataHub

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
<p>DataHub description of the Container</p>
</td>
</tr>
</tbody>
</table>

## ContainerProperties

Read-only properties that originate in the source data platform

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
<p>Display name of the Container</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>System description of the Container</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>Custom properties of the Container</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the Container</p>
</td>
</tr>
<tr>
<td>
qualifiedName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Fully-qualified name of the Container</p>
</td>
</tr>
</tbody>
</table>

## ContentParams

Params about the recommended content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Number of entities corresponding to the recommended content</p>
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
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the Corp Group</p>
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
editableProperties<br />
<a href="/docs/graphql/objects#corpgroupeditableproperties"><code>CorpGroupEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties about the group</p>
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
origin<br />
<a href="/docs/graphql/objects#origin"><code>Origin</code></a>
</td>
<td>
<p>Origin info about this group.</p>
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

## CorpGroupEditableProperties

Additional read write properties about a group

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
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>display name of this group</p>
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
slack<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Slack handle for the group</p>
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
status<br />
<a href="/docs/graphql/enums#corpuserstatus"><code>CorpUserStatus</code></a>
</td>
<td>
<p>The status of the user</p>
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
isNativeUser<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this user is a native DataHub user</p>
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
<tr>
<td>
settings<br />
<a href="/docs/graphql/objects#corpusersettings"><code>CorpUserSettings</code></a>
</td>
<td>
<p>Settings that a user can customize through the datahub ui</p>
</td>
</tr>
</tbody>
</table>

## CorpUserAppearanceSettings

Settings that control look and feel of the DataHub UI for the user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
showSimplifiedHomepage<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Flag whether the user should see a homepage with only datasets, charts &amp; dashboards. Intended for users
who have less operational use cases for the datahub tool.</p>
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
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>Custom properties of the ldap</p>
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
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>Custom properties of the ldap</p>
</td>
</tr>
</tbody>
</table>

## CorpUserSettings

Settings that a user can customize through the datahub ui

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
appearance<br />
<a href="/docs/graphql/objects#corpuserappearancesettings"><code>CorpUserAppearanceSettings</code></a>
</td>
<td>
<p>Settings that control look and feel of the DataHub UI for the user</p>
</td>
</tr>
<tr>
<td>
views<br />
<a href="/docs/graphql/objects#corpuserviewssettings"><code>CorpUserViewsSettings</code></a>
</td>
<td>
<p>Settings related to the DataHub Views feature</p>
</td>
</tr>
</tbody>
</table>

## CorpUserViewsSettings

Settings related to the Views feature of DataHub.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
defaultView<br />
<a href="/docs/graphql/objects#datahubview"><code>DataHubView</code></a>
</td>
<td>
<p>The default view for the User.</p>
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

## CustomPropertiesEntry

An entry in a custom properties map represented as a tuple

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
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the entity this property came from for tracking purposes e.g. when sibling nodes are merged together</p>
</td>
</tr>
</tbody>
</table>

## Dashboard

A Dashboard Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
</td>
</tr>
<tr>
<td>
container<br />
<a href="/docs/graphql/objects#container"><code>Container</code></a>
</td>
<td>
<p>The parent container in which the entity resides</p>
</td>
</tr>
<tr>
<td>
parentContainers<br />
<a href="/docs/graphql/objects#parentcontainersresult"><code>ParentContainersResult</code></a>
</td>
<td>
<p>Recursively get the lineage of containers for this entity</p>
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
embed<br />
<a href="/docs/graphql/objects#embed"><code>Embed</code></a>
</td>
<td>
<p>Embed information about the Dashboard</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the dashboard</p>
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
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dashboard</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the dashboard</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Dashboard</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the dashboard. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
usageStats<br />
<a href="/docs/graphql/objects#dashboardusagequeryresult"><code>DashboardUsageQueryResult</code></a>
</td>
<td>
<p>Experimental (Subject to breaking change) -- Statistics about how this Dashboard is used</p>

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
statsSummary<br />
<a href="/docs/graphql/objects#dashboardstatssummary"><code>DashboardStatsSummary</code></a>
</td>
<td>
<p>Experimental - Summary operational &amp; usage statistics about a Dashboard</p>
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
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the dashboard is defined</p>
</td>
</tr>
<tr>
<td>
inputFields<br />
<a href="/docs/graphql/objects#inputfields"><code>InputFields</code></a>
</td>
<td>
<p>Input fields that power all the charts in the dashboard</p>
</td>
</tr>
<tr>
<td>
subTypes<br />
<a href="/docs/graphql/objects#subtypes"><code>SubTypes</code></a>
</td>
<td>
<p>Sub Types of the dashboard</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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

## DashboardStatsSummary

Experimental - subject to change. A summary of usage metrics about a Dashboard.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
viewCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total view count for the dashboard</p>
</td>
</tr>
<tr>
<td>
viewCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The view count in the last 30 days</p>
</td>
</tr>
<tr>
<td>
uniqueUserCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The unique user count in the past 30 days</p>
</td>
</tr>
<tr>
<td>
topUsersLast30Days<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<p>The top users in the past 30 days</p>
</td>
</tr>
</tbody>
</table>

## DashboardUsageAggregation

An aggregation of Dashboard usage statistics

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
<p>The resource urn associated with the usage information, eg a Dashboard urn</p>
</td>
</tr>
<tr>
<td>
metrics<br />
<a href="/docs/graphql/objects#dashboardusageaggregationmetrics"><code>DashboardUsageAggregationMetrics</code></a>
</td>
<td>
<p>The rolled up usage metrics</p>
</td>
</tr>
</tbody>
</table>

## DashboardUsageAggregationMetrics

Rolled up metrics about Dashboard usage over time

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
<p>The unique number of dashboard users within the time range</p>
</td>
</tr>
<tr>
<td>
viewsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard views within the time range</p>
</td>
</tr>
<tr>
<td>
executionsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard executions within the time range</p>
</td>
</tr>
</tbody>
</table>

## DashboardUsageMetrics

A set of absolute dashboard usage metrics

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
<p>The time at which the metrics were reported</p>
</td>
</tr>
<tr>
<td>
favoritesCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of times dashboard has been favorited
FIXME: Qualifies as Popularity Metric rather than Usage Metric?</p>
</td>
</tr>
<tr>
<td>
viewsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard views</p>
</td>
</tr>
<tr>
<td>
executionsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard execution</p>
</td>
</tr>
<tr>
<td>
lastViewed<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time when this dashboard was last viewed</p>
</td>
</tr>
</tbody>
</table>

## DashboardUsageQueryResult

The result of a dashboard usage query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
buckets<br />
<a href="/docs/graphql/objects#dashboardusageaggregation"><code>[DashboardUsageAggregation]</code></a>
</td>
<td>
<p>A set of relevant time windows for use in displaying usage statistics</p>
</td>
</tr>
<tr>
<td>
aggregations<br />
<a href="/docs/graphql/objects#dashboardusagequeryresultaggregations"><code>DashboardUsageQueryResultAggregations</code></a>
</td>
<td>
<p>A set of rolled up aggregations about the dashboard usage</p>
</td>
</tr>
<tr>
<td>
metrics<br />
<a href="/docs/graphql/objects#dashboardusagemetrics"><code>[DashboardUsageMetrics!]</code></a>
</td>
<td>
<p>A set of absolute dashboard usage metrics</p>
</td>
</tr>
</tbody>
</table>

## DashboardUsageQueryResultAggregations

A set of rolled up aggregations about the Dashboard usage

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
<p>The count of unique Dashboard users within the queried time range</p>
</td>
</tr>
<tr>
<td>
users<br />
<a href="/docs/graphql/objects#dashboarduserusagecounts"><code>[DashboardUserUsageCounts]</code></a>
</td>
<td>
<p>The specific per user usage counts within the queried time range</p>
</td>
</tr>
<tr>
<td>
viewsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard views within the queried time range</p>
</td>
</tr>
<tr>
<td>
executionsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of dashboard executions within the queried time range</p>
</td>
</tr>
</tbody>
</table>

## DashboardUserUsageCounts

Information about individual user usage of a Dashboard

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
<p>The user of the Dashboard</p>
</td>
</tr>
<tr>
<td>
viewsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>number of times dashboard has been viewed by the user</p>
</td>
</tr>
<tr>
<td>
executionsCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>number of dashboard executions by the user</p>
</td>
</tr>
<tr>
<td>
usageCount<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Normalized numeric metric representing user&#39;s dashboard usage
Higher value represents more usage</p>
</td>
</tr>
</tbody>
</table>

## DataFlow

A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
to produce an output Dataset Also known as a Data Pipeline

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the Data Flow</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dashboard</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the dashboard</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the DataFlow</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the data flow. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
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
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform urn where the datflow is defined</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
</tbody>
</table>

## DataHubPolicy

An DataHub Platform Access Policy - Policies determine who can perform what actions against which resources on the platform

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
<p>The primary key of the Policy</p>
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
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from the Role</p>

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
policyType<br />
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

## DataHubRole

A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take.

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
<p>The primary key of the role</p>
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
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from the Role</p>

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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Role.</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The description of the Role</p>
</td>
</tr>
</tbody>
</table>

## DataHubView

An DataHub View - Filters that are applied across the application automatically.

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
<p>The primary key of the View</p>
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
viewType<br />
<a href="/docs/graphql/enums#datahubviewtype"><code>DataHubViewType!</code></a>
</td>
<td>
<p>The type of the View</p>
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
<p>The description of the View</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/objects#datahubviewdefinition"><code>DataHubViewDefinition!</code></a>
</td>
<td>
<p>The definition of the View</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from the View</p>

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

## DataHubViewDefinition

An DataHub View Definition

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entityTypes<br />
<a href="/docs/graphql/enums#entitytype"><code>[EntityType!]!</code></a>
</td>
<td>
<p>A set of filters to apply. If left empty, then ALL entity types are in scope.</p>
</td>
</tr>
<tr>
<td>
filter<br />
<a href="/docs/graphql/objects#datahubviewfilter"><code>DataHubViewFilter!</code></a>
</td>
<td>
<p>A set of filters to apply. If left empty, then no filters will be applied.</p>
</td>
</tr>
</tbody>
</table>

## DataHubViewFilter

A DataHub View Filter. Note that

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

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
<a href="/docs/graphql/objects#facetfilter"><code>[FacetFilter!]!</code></a>
</td>
<td>
<p>A set of filters combined using the operator. If left empty, then no filters will be applied.</p>
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
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the Data Flow</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dashboard</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the dashboard</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Data Job</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the data job. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
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
<p>Information about the inputs and outputs of a Data processing job including column-level lineage.</p>
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
<tr>
<td>
runs<br />
<a href="/docs/graphql/objects#dataprocessinstanceresult"><code>DataProcessInstanceResult</code></a>
</td>
<td>
<p>History of runs of this task</p>

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

</td>
</tr>
<tr>
<td>
count<br />
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
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
<tr>
<td>
fineGrainedLineages<br />
<a href="/docs/graphql/objects#finegrainedlineage"><code>[FineGrainedLineage!]</code></a>
</td>
<td>
<p>Lineage information for the column-level. Includes a list of objects
detailing which columns are upstream and which are downstream of each other.
The upstream and downstream columns are from datasets.</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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

## DataPlatformInstance

A Data Platform instance represents an instance of a 3rd party platform like Looker, Snowflake, etc.

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
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Name of the data platform</p>
</td>
</tr>
<tr>
<td>
instanceId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The platform instance id</p>
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
properties<br />
<a href="/docs/graphql/objects#dataplatforminstanceproperties"><code>DataPlatformInstanceProperties</code></a>
</td>
<td>
<p>Additional read only properties associated with a data platform instance</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the data platform instance</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the data platform instance</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags used for searching the data platform instance</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the data platform instance</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the container</p>
</td>
</tr>
</tbody>
</table>

## DataPlatformInstanceProperties

Additional read only properties about a DataPlatformInstance

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
<p>The name of the data platform instance used in display</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Read only technical description for the data platform instance</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>Custom properties of the data platform instance</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL associated with the data platform instance</p>
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

## DataProcessInstance

A DataProcessInstance Metadata Entity, representing an individual run of
a task or datajob.

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
<p>The primary key of the DataProcessInstance</p>
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
state<br />
<a href="/docs/graphql/objects#dataprocessrunevent"><code>[DataProcessRunEvent]</code></a>
</td>
<td>
<p>The history of state changes for the run</p>

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
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>When the run was kicked off</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name of the data process</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Edges extending from this entity.
In the UI, used for inputs, outputs and parentTemplate</p>

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
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The link to view the task run in the source system</p>
</td>
</tr>
</tbody>
</table>

## DataProcessInstanceResult

Data Process instances that match the provided query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
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
start<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The offset of the result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of run events returned</p>
</td>
</tr>
<tr>
<td>
runs<br />
<a href="/docs/graphql/objects#dataprocessinstance"><code>[DataProcessInstance]</code></a>
</td>
<td>
<p>The data process instances that produced or consumed the entity</p>
</td>
</tr>
</tbody>
</table>

## DataProcessInstanceRunResult

the result of a run, part of the run state

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resultType<br />
<a href="/docs/graphql/enums#dataprocessinstancerunresulttype"><code>DataProcessInstanceRunResultType</code></a>
</td>
<td>
<p>The outcome of the run</p>
</td>
</tr>
<tr>
<td>
nativeResultType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The outcome of the run in the data platforms native language</p>
</td>
</tr>
</tbody>
</table>

## DataProcessRunEvent

A state change event in the data process instance lifecycle

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [TimeSeriesAspect](/docs/graphql/interfaces#timeseriesaspect)

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
status<br />
<a href="/docs/graphql/enums#dataprocessrunstatus"><code>DataProcessRunStatus</code></a>
</td>
<td>
<p>The status of the data process instance</p>
</td>
</tr>
<tr>
<td>
attempt<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The try number that this instance run is in</p>
</td>
</tr>
<tr>
<td>
result<br />
<a href="/docs/graphql/objects#dataprocessinstancerunresult"><code>DataProcessInstanceRunResult</code></a>
</td>
<td>
<p>The result of a run</p>
</td>
</tr>
<tr>
<td>
timestampMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The timestamp associated with the run event in milliseconds</p>
</td>
</tr>
</tbody>
</table>

## DataProduct

A Data Product, or a logical grouping of Metadata Entities

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
<p>The primary key of the Data Product</p>
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
properties<br />
<a href="/docs/graphql/objects#dataproductproperties"><code>DataProductProperties</code></a>
</td>
<td>
<p>Properties about a Data Product</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the Data Product</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the Data Product</p>
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
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the Data Product</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Data Product</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags used for searching Data Product</p>
</td>
</tr>
</tbody>
</table>

## DataProductProperties

Properties about a domain

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
<p>Display name of the Data Product</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Data Product</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>External URL for the DataProduct (most likely GitHub repo where Data Products are managed as code)</p>
</td>
</tr>
<tr>
<td>
numAssets<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Number of children entities inside of the Data Product</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>Custom properties of the Data Product</p>
</td>
</tr>
</tbody>
</table>

## Dataset

A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
container<br />
<a href="/docs/graphql/objects#container"><code>Container</code></a>
</td>
<td>
<p>The parent container in which the entity resides</p>
</td>
</tr>
<tr>
<td>
parentContainers<br />
<a href="/docs/graphql/objects#parentcontainersresult"><code>ParentContainersResult</code></a>
</td>
<td>
<p>Recursively get the lineage of containers for this entity</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique guid for dataset
No longer to be used as the Dataset display name. Use properties.name instead</p>
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
<p>The deprecation status of the dataset</p>
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
embed<br />
<a href="/docs/graphql/objects#embed"><code>Embed</code></a>
</td>
<td>
<p>Embed information about the Dataset</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
access<br />
<a href="/docs/graphql/objects#access"><code>Access</code></a>
</td>
<td>
<p>The Roles and the properties to access the dataset</p>
</td>
</tr>
<tr>
<td>
usageStats<br />
<a href="/docs/graphql/objects#usagequeryresult"><code>UsageQueryResult</code></a>
</td>
<td>
<p>Statistics about how this Dataset is used
The first parameter, <code>resource</code>, is deprecated and no longer needs to be provided</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resource<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
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
statsSummary<br />
<a href="/docs/graphql/objects#datasetstatssummary"><code>DatasetStatsSummary</code></a>
</td>
<td>
<p>Experimental - Summary operational &amp; usage statistics about a Dataset</p>
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
filter<br />
<a href="/docs/graphql/inputObjects#filterinput"><code>FilterInput</code></a>
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
operations<br />
<a href="/docs/graphql/objects#operation"><code>[Operation!]</code></a>
</td>
<td>
<p>Operational events for an entity.</p>

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
filter<br />
<a href="/docs/graphql/inputObjects#filterinput"><code>FilterInput</code></a>
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
assertions<br />
<a href="/docs/graphql/objects#entityassertionsresult"><code>EntityAssertionsResult</code></a>
</td>
<td>
<p>Assertions associated with the Dataset</p>

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

</td>
</tr>
<tr>
<td>
count<br />
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the dataset. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
health<br />
<a href="/docs/graphql/objects#health"><code>[Health!]</code></a>
</td>
<td>
<p>Experimental! The resolved health statuses of the Dataset</p>
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

<p>Deprecated, see the properties field instead
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

<p>Deprecated, use properties instead
Native Dataset Uri
Uri should not include any environment specific properties</p>
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
<tr>
<td>
subTypes<br />
<a href="/docs/graphql/objects#subtypes"><code>SubTypes</code></a>
</td>
<td>
<p>Sub Types that this entity implements</p>
</td>
</tr>
<tr>
<td>
viewProperties<br />
<a href="/docs/graphql/objects#viewproperties"><code>ViewProperties</code></a>
</td>
<td>
<p>View related properties. Only relevant if subtypes field contains view.</p>
</td>
</tr>
<tr>
<td>
aspects<br />
<a href="/docs/graphql/objects#rawaspect"><code>[RawAspect!]</code></a>
</td>
<td>
<p>Experimental API.
For fetching extra entities that do not have custom UI code yet</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#aspectparams"><code>AspectParams</code></a>
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
runs<br />
<a href="/docs/graphql/objects#dataprocessinstanceresult"><code>DataProcessInstanceResult</code></a>
</td>
<td>
<p>History of datajob runs that either produced or consumed this dataset</p>

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

</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
direction<br />
<a href="/docs/graphql/enums#relationshipdirection"><code>RelationshipDirection!</code></a>
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
siblings<br />
<a href="/docs/graphql/objects#siblingproperties"><code>SiblingProperties</code></a>
</td>
<td>
<p>Metadata about the datasets siblings</p>
</td>
</tr>
<tr>
<td>
fineGrainedLineages<br />
<a href="/docs/graphql/objects#finegrainedlineage"><code>[FineGrainedLineage!]</code></a>
</td>
<td>
<p>Lineage information for the column-level. Includes a list of objects
detailing which columns are upstream and which are downstream of each other.
The upstream and downstream columns are from datasets.</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
<tr>
<td>
testResults<br />
<a href="/docs/graphql/objects#testresults"><code>TestResults</code></a>
</td>
<td>
<p>The results of evaluating tests</p>
</td>
</tr>
</tbody>
</table>

## DatasetAssertionInfo

Detailed information about a Dataset Assertion

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
datasetUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The urn of the dataset that the assertion is related to</p>
</td>
</tr>
<tr>
<td>
scope<br />
<a href="/docs/graphql/enums#datasetassertionscope"><code>DatasetAssertionScope!</code></a>
</td>
<td>
<p>The scope of the Dataset assertion.</p>
</td>
</tr>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#schemafieldref"><code>[SchemaFieldRef!]</code></a>
</td>
<td>
<p>The fields serving as input to the assertion. Empty if there are none.</p>
</td>
</tr>
<tr>
<td>
aggregation<br />
<a href="/docs/graphql/enums#assertionstdaggregation"><code>AssertionStdAggregation</code></a>
</td>
<td>
<p>Standardized assertion operator</p>
</td>
</tr>
<tr>
<td>
operator<br />
<a href="/docs/graphql/enums#assertionstdoperator"><code>AssertionStdOperator!</code></a>
</td>
<td>
<p>Standardized assertion operator</p>
</td>
</tr>
<tr>
<td>
parameters<br />
<a href="/docs/graphql/objects#assertionstdparameters"><code>AssertionStdParameters</code></a>
</td>
<td>
<p>Standard parameters required for the assertion. e.g. min_value, max_value, value, columns</p>
</td>
</tr>
<tr>
<td>
nativeType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The native operator for the assertion. For Great Expectations, this will contain the original expectation name.</p>
</td>
</tr>
<tr>
<td>
nativeParameters<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Native parameters required for the assertion.</p>
</td>
</tr>
<tr>
<td>
logic<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Logic comprising a raw, unstructured assertion.</p>
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
sizeInBytes<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The storage size in bytes</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the dataset used in display</p>
</td>
</tr>
<tr>
<td>
qualifiedName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Fully-qualified name of the Dataset</p>
</td>
</tr>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
<tr>
<td>
created<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Created timestamp millis associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
createdActor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Actor associated with the Dataset&#39;s created timestamp</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Last Modified timestamp millis associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
lastModifiedActor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Actor associated with the Dataset&#39;s lastModified timestamp</p>
</td>
</tr>
</tbody>
</table>

## DatasetStatsSummary

Experimental - subject to change. A summary of usage metrics about a Dataset.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
queryCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The query count in the past 30 days</p>
</td>
</tr>
<tr>
<td>
uniqueUserCountLast30Days<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The unique user count in the past 30 days</p>
</td>
</tr>
<tr>
<td>
topUsersLast30Days<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]</code></a>
</td>
<td>
<p>The top users in the past 30 days</p>
</td>
</tr>
</tbody>
</table>

## DateRange

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
start<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
end<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

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
<a href="/docs/graphql/scalars#string"><code>String</code></a>
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

## Domain

A domain, or a logical grouping of Metadata Entities

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
<p>The primary key of the domain</p>
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
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Id of the domain</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#domainproperties"><code>DomainProperties</code></a>
</td>
<td>
<p>Properties about a domain</p>
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
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the dataset</p>
</td>
</tr>
<tr>
<td>
entities<br />
<a href="/docs/graphql/objects#searchresults"><code>SearchResults</code></a>
</td>
<td>
<p>Children entities inside of the Domain</p>

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#domainentitiesinput"><code>DomainEntitiesInput</code></a>
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
</tbody>
</table>

## DomainAssociation

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domain"><code>Domain!</code></a>
</td>
<td>
<p>The domain related to the assocaited urn</p>
</td>
</tr>
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together</p>
</td>
</tr>
</tbody>
</table>

## DomainProperties

Properties about a domain

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
<p>Display name of the domain</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Domain</p>
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
Deprecated! Replaced by TagProperties.

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
<p>A display name for the Tag</p>
</td>
</tr>
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

## Embed

Information required to render an embedded version of an asset

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
renderUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A URL which can be rendered inside of an iframe.</p>
</td>
</tr>
</tbody>
</table>

## EntityAssertionsResult

A list of Assertions Associated with an Entity

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
<p>The number of assertions in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of assertions in the result set</p>
</td>
</tr>
<tr>
<td>
assertions<br />
<a href="/docs/graphql/objects#assertion"><code>[Assertion!]!</code></a>
</td>
<td>
<p>The assertions themselves</p>
</td>
</tr>
</tbody>
</table>

## EntityCountResult

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
entityType<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## EntityCountResults

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
counts<br />
<a href="/docs/graphql/objects#entitycountresult"><code>[EntityCountResult!]</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## EntityLineageResult

A list of lineage information associated with a source Entity

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
filtered<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The number of results that were filtered out of the page (soft-deleted or non-existent)</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#lineagerelationship"><code>[LineageRelationship!]!</code></a>
</td>
<td>
<p>Relationships in the result set</p>
</td>
</tr>
</tbody>
</table>

## EntityPath

An overview of the field that was matched in the entity search document

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
path<br />
<a href="/docs/graphql/interfaces#entity"><code>[Entity]</code></a>
</td>
<td>
<p>Path of entities between source and destination nodes</p>
</td>
</tr>
</tbody>
</table>

## EntityPrivileges

Shared privileges object across entities. Not all privileges apply to every entity.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
canManageChildren<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not a user can create child entities under a parent entity.
For example, can one create Terms/Node sunder a Glossary Node.</p>
</td>
</tr>
<tr>
<td>
canManageEntity<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not a user can delete or move this entity.</p>
</td>
</tr>
<tr>
<td>
canEditLineage<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not a user can create or delete lineage edges for an entity.</p>
</td>
</tr>
<tr>
<td>
canEditEmbed<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not a user update the embed information</p>
</td>
</tr>
<tr>
<td>
canEditQueries<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not a user can update the Queries for the entity (e.g. dataset)</p>
</td>
</tr>
</tbody>
</table>

## EntityProfileConfig

Configuration for an entity profile

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
defaultTab<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The enum value from EntityProfileTab for which tab should be showed by default on
entity profile pages. If null, rely on default sorting from React code.</p>
</td>
</tr>
</tbody>
</table>

## EntityProfileParams

Context to define the entity profile page

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
<p>Urn of the entity being shown</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#entitytype"><code>EntityType!</code></a>
</td>
<td>
<p>Type of the enity being displayed</p>
</td>
</tr>
</tbody>
</table>

## EntityProfilesConfig

Configuration for different entity profiles

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#entityprofileconfig"><code>EntityProfileConfig</code></a>
</td>
<td>
<p>The configurations for a Domain entity profile</p>
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
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
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

## ExecutionRequest

Retrieve an ingestion execution request

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
<p>Urn of the execution request</p>
</td>
</tr>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id for the execution request</p>
</td>
</tr>
<tr>
<td>
input<br />
<a href="/docs/graphql/objects#executionrequestinput"><code>ExecutionRequestInput!</code></a>
</td>
<td>
<p>Input provided when creating the Execution Request</p>
</td>
</tr>
<tr>
<td>
result<br />
<a href="/docs/graphql/objects#executionrequestresult"><code>ExecutionRequestResult</code></a>
</td>
<td>
<p>Result of the execution request</p>
</td>
</tr>
</tbody>
</table>

## ExecutionRequestInput

Input provided when creating an Execution Request

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
task<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The type of the task to executed</p>
</td>
</tr>
<tr>
<td>
source<br />
<a href="/docs/graphql/objects#executionrequestsource"><code>ExecutionRequestSource!</code></a>
</td>
<td>
<p>The source of the execution request</p>
</td>
</tr>
<tr>
<td>
arguments<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Arguments provided when creating the execution request</p>
</td>
</tr>
<tr>
<td>
requestedAt<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The time at which the request was created</p>
</td>
</tr>
</tbody>
</table>

## ExecutionRequestResult

The result of an ExecutionRequest

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
status<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The result of the request, e.g. either SUCCEEDED or FAILED</p>
</td>
</tr>
<tr>
<td>
startTimeMs<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Time at which the task began</p>
</td>
</tr>
<tr>
<td>
durationMs<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Duration of the task</p>
</td>
</tr>
<tr>
<td>
report<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A report about the ingestion run</p>
</td>
</tr>
<tr>
<td>
structuredReport<br />
<a href="/docs/graphql/objects#structuredreport"><code>StructuredReport</code></a>
</td>
<td>
<p>A structured report for this Execution Request</p>
</td>
</tr>
</tbody>
</table>

## ExecutionRequestSource

Information about the source of an execution request

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The type of the source, e.g. SCHEDULED_INGESTION_SOURCE</p>
</td>
</tr>
</tbody>
</table>

## FacetFilter

A single filter value

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
<p>Name of field to filter by</p>
</td>
</tr>
<tr>
<td>
condition<br />
<a href="/docs/graphql/enums#filteroperator"><code>FilterOperator</code></a>
</td>
<td>
<p>Condition for the values.</p>
</td>
</tr>
<tr>
<td>
values<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
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
displayName<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display name of the field</p>
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

## FeatureFlagsConfig

Configurations related to DataHub Views feature

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
readOnlyModeEnabled<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether read only mode is enabled on an instance.
Right now this only affects ability to edit user profile image URL but can be extended.</p>
</td>
</tr>
<tr>
<td>
showSearchFiltersV2<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether search filters V2 should be shown or the default filter side-panel</p>
</td>
</tr>
<tr>
<td>
showBrowseV2<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether browse V2 sidebar should be shown</p>
</td>
</tr>
<tr>
<td>
showAcrylInfo<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether we should show CTAs in the UI related to moving to Managed DataHub by Acryl.</p>
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

## FineGrainedLineage

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
upstreams<br />
<a href="/docs/graphql/objects#schemafieldref"><code>[SchemaFieldRef!]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
downstreams<br />
<a href="/docs/graphql/objects#schemafieldref"><code>[SchemaFieldRef!]</code></a>
</td>
<td>

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

## ForeignKeyConstraint

Metadata around a foreign key constraint between two datasets

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
<p>The human-readable name of the constraint</p>
</td>
</tr>
<tr>
<td>
foreignFields<br />
<a href="/docs/graphql/objects#schemafieldentity"><code>[SchemaFieldEntity]</code></a>
</td>
<td>
<p>List of fields in the foreign dataset</p>
</td>
</tr>
<tr>
<td>
sourceFields<br />
<a href="/docs/graphql/objects#schemafieldentity"><code>[SchemaFieldEntity]</code></a>
</td>
<td>
<p>List of fields in this dataset</p>
</td>
</tr>
<tr>
<td>
foreignDataset<br />
<a href="/docs/graphql/objects#dataset"><code>Dataset</code></a>
</td>
<td>
<p>The foreign dataset for easy reference</p>
</td>
</tr>
</tbody>
</table>

## FreshnessStats

Freshness stats for a query result.
Captures whether the query was served out of a cache, what the staleness was, etc.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
cached<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether a cache was used to respond to this query</p>
</td>
</tr>
<tr>
<td>
systemFreshness<br />
<a href="/docs/graphql/objects#systemfreshness"><code>[SystemFreshness]</code></a>
</td>
<td>
<p>The latest timestamp in millis of the system that was used to respond to this query
In case a cache was consulted, this reflects the freshness of the cache
In case an index was consulted, this reflects the freshness of the index</p>
</td>
</tr>
</tbody>
</table>

## GetQuickFiltersResult

The result object when fetching quick filters

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
quickFilters<br />
<a href="/docs/graphql/objects#quickfilter"><code>[QuickFilter]!</code></a>
</td>
<td>
<p>The list of quick filters to render in the UI</p>
</td>
</tr>
</tbody>
</table>

## GetRootGlossaryNodesResult

The result when getting Glossary entities

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
nodes<br />
<a href="/docs/graphql/objects#glossarynode"><code>[GlossaryNode!]!</code></a>
</td>
<td>
<p>A list of Glossary Nodes without a parent node</p>
</td>
</tr>
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
<p>The number of nodes in the returned result</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of nodes in the result set</p>
</td>
</tr>
</tbody>
</table>

## GetRootGlossaryTermsResult

The result when getting root GlossaryTerms

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
terms<br />
<a href="/docs/graphql/objects#glossaryterm"><code>[GlossaryTerm!]!</code></a>
</td>
<td>
<p>A list of Glossary Terms without a parent node</p>
</td>
</tr>
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
<p>The number of terms in the returned result</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of terms in the result set</p>
</td>
</tr>
</tbody>
</table>

## GetSchemaBlameResult

Schema changes computed at a specific version.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
version<br />
<a href="/docs/graphql/objects#semanticversionstruct"><code>SemanticVersionStruct</code></a>
</td>
<td>
<p>Selected semantic version</p>
</td>
</tr>
<tr>
<td>
schemaFieldBlameList<br />
<a href="/docs/graphql/objects#schemafieldblame"><code>[SchemaFieldBlame!]</code></a>
</td>
<td>
<p>List of schema blame. Absent when there are no fields to return history for.</p>
</td>
</tr>
</tbody>
</table>

## GetSchemaVersionListResult

Schema changes computed at a specific version.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
latestVersion<br />
<a href="/docs/graphql/objects#semanticversionstruct"><code>SemanticVersionStruct</code></a>
</td>
<td>
<p>Latest and current semantic version</p>
</td>
</tr>
<tr>
<td>
version<br />
<a href="/docs/graphql/objects#semanticversionstruct"><code>SemanticVersionStruct</code></a>
</td>
<td>
<p>Selected semantic version</p>
</td>
</tr>
<tr>
<td>
semanticVersionList<br />
<a href="/docs/graphql/objects#semanticversionstruct"><code>[SemanticVersionStruct!]</code></a>
</td>
<td>
<p>All semantic versions. Absent when there are no versions.</p>
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

## GlobalViewsSettings

Global (platform-level) settings related to the Views feature

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
defaultView<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The global default View. If a user does not have a personal default, then
this will be the default view.</p>
</td>
</tr>
</tbody>
</table>

## GlossaryNode

A Glossary Node, or a directory in a Business Glossary represents a container of
Glossary Terms or other Glossary Nodes

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
<p>Ownership metadata of the glossary term</p>
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
properties<br />
<a href="/docs/graphql/objects#glossarynodeproperties"><code>GlossaryNodeProperties</code></a>
</td>
<td>
<p>Additional properties associated with the Glossary Term</p>
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
parentNodes<br />
<a href="/docs/graphql/objects#parentnodesresult"><code>ParentNodesResult</code></a>
</td>
<td>
<p>Recursively get the lineage of glossary nodes for this entity</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## GlossaryNodeProperties

Additional read only properties about a Glossary Node

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
<p>The name of the Glossary Term</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the glossary term</p>
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
<p>Ownership metadata of the glossary term</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the glossary term</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the Glossary Term</p>
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
<blockquote>Deprecated: No longer supported</blockquote>

<p>A unique identifier for the Glossary Term. Deprecated - Use properties.name field instead.</p>
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
<p>Additional properties associated with the Glossary Term</p>
</td>
</tr>
<tr>
<td>
glossaryTermInfo<br />
<a href="/docs/graphql/objects#glossaryterminfo"><code>GlossaryTermInfo</code></a>
</td>
<td>
<p>Deprecated, use properties field instead
Details of the Glossary Term</p>
</td>
</tr>
<tr>
<td>
deprecation<br />
<a href="/docs/graphql/objects#deprecation"><code>Deprecation</code></a>
</td>
<td>
<p>The deprecation status of the Glossary Term</p>
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
schemaMetadata<br />
<a href="/docs/graphql/objects#schemametadata"><code>SchemaMetadata</code></a>
</td>
<td>
<p>Schema metadata of the dataset</p>

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
parentNodes<br />
<a href="/docs/graphql/objects#parentnodesresult"><code>ParentNodesResult</code></a>
</td>
<td>
<p>Recursively get the lineage of glossary nodes for this entity</p>
</td>
</tr>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/objects#entityprivileges"><code>EntityPrivileges</code></a>
</td>
<td>
<p>Privileges given to a user relevant to this entity</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
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
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the associated urn for tracking purposes e.g. when sibling nodes are merged together</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name of the Glossary Term</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the glossary term</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Definition of the glossary term. Deprecated - Use &#39;description&#39; instead.</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the Glossary Term</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the glossary term</p>
</td>
</tr>
<tr>
<td>
definition<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Definition of the glossary term. Deprecated - Use &#39;description&#39; instead.</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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

## Health

The resolved Health of an Asset

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#healthstatustype"><code>HealthStatusType!</code></a>
</td>
<td>
<p>An enum representing the type of health indicator</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/enums#healthstatus"><code>HealthStatus!</code></a>
</td>
<td>
<p>An enum representing the resolved Health status of an Asset</p>
</td>
</tr>
<tr>
<td>
message<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional message describing the resolved health status</p>
</td>
</tr>
<tr>
<td>
causes<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>The causes responsible for the health status</p>
</td>
</tr>
</tbody>
</table>

## Highlight

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
body<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

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

## IdentityManagementConfig

Configurations related to Identity Management

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
<p>Whether identity management screen is able to be shown in the UI</p>
</td>
</tr>
</tbody>
</table>

## IngestionConfig

A set of configurations for an Ingestion Source

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
recipe<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The JSON-encoded recipe to use for ingestion</p>
</td>
</tr>
<tr>
<td>
executorId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Advanced: The specific executor that should handle the execution request. Defaults to &#39;default&#39;.</p>
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
<tr>
<td>
debugMode<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Advanced: Whether or not to run ingestion in debug mode</p>
</td>
</tr>
</tbody>
</table>

## IngestionRun

The runs associated with an Ingestion Source managed by DataHub

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
executionRequestUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The urn of the execution request associated with the user</p>
</td>
</tr>
</tbody>
</table>

## IngestionSchedule

A schedule associated with an Ingestion Source

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
timezone<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Time Zone abbreviation (e.g. GMT, EDT). Defaults to UTC.</p>
</td>
</tr>
<tr>
<td>
interval<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The cron-formatted interval to execute the ingestion source on</p>
</td>
</tr>
</tbody>
</table>

## IngestionSource

An Ingestion Source Entity

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
<p>The primary key of the Ingestion Source</p>
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
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The display name of the Ingestion Source</p>
</td>
</tr>
<tr>
<td>
schedule<br />
<a href="/docs/graphql/objects#ingestionschedule"><code>IngestionSchedule</code></a>
</td>
<td>
<p>An optional schedule associated with the Ingestion Source</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform</code></a>
</td>
<td>
<p>The data platform associated with this ingestion source</p>
</td>
</tr>
<tr>
<td>
config<br />
<a href="/docs/graphql/objects#ingestionconfig"><code>IngestionConfig!</code></a>
</td>
<td>
<p>An type-specific set of configurations for the ingestion source</p>
</td>
</tr>
<tr>
<td>
executions<br />
<a href="/docs/graphql/objects#ingestionsourceexecutionrequests"><code>IngestionSourceExecutionRequests</code></a>
</td>
<td>
<p>Previous requests to execute the ingestion source</p>

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

</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
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

## IngestionSourceExecutionRequests

Requests for execution associated with an ingestion source

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
total<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
executionRequests<br />
<a href="/docs/graphql/objects#executionrequest"><code>[ExecutionRequest!]!</code></a>
</td>
<td>
<p>The execution request objects comprising the result set</p>
</td>
</tr>
</tbody>
</table>

## InputField

Input field of the chart

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
schemaFieldUrn<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
schemaField<br />
<a href="/docs/graphql/objects#schemafield"><code>SchemaField</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## InputFields

Input fields of the chart

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
fields<br />
<a href="/docs/graphql/objects#inputfield"><code>[InputField]</code></a>
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
label<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Label associated with the URL</p>
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
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use label instead
Description of the resource</p>
</td>
</tr>
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the owned urn for tracking purposes e.g. when sibling nodes are merged together</p>
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

## InviteToken

Token that allows users to sign up as a native user

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
inviteToken<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The invite token</p>
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

## LineageConfig

Configurations related to Lineage

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
supportsImpactAnalysis<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the backend support impact analysis feature</p>
</td>
</tr>
</tbody>
</table>

## LineageRelationship

Metadata about a lineage relationship between two entities

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
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>Entity that is related via lineage</p>
</td>
</tr>
<tr>
<td>
degree<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>Degree of relationship (number of hops to get to entity)</p>
</td>
</tr>
<tr>
<td>
createdOn<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Timestamp for when this lineage relationship was created. Could be null.</p>
</td>
</tr>
<tr>
<td>
createdActor<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>The actor who created this lineage relationship. Could be null.</p>
</td>
</tr>
<tr>
<td>
updatedOn<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Timestamp for when this lineage relationship was last updated. Could be null.</p>
</td>
</tr>
<tr>
<td>
updatedActor<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>The actor who last updated this lineage relationship. Could be null.</p>
</td>
</tr>
<tr>
<td>
isManual<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether this edge is a manual edge. Could be null.</p>
</td>
</tr>
</tbody>
</table>

## LinkParams

Parameters required to specify the page to land once clicked

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
searchParams<br />
<a href="/docs/graphql/objects#searchparams"><code>SearchParams</code></a>
</td>
<td>
<p>Context to define the search page</p>
</td>
</tr>
<tr>
<td>
entityProfileParams<br />
<a href="/docs/graphql/objects#entityprofileparams"><code>EntityProfileParams</code></a>
</td>
<td>
<p>Context to define the entity profile page</p>
</td>
</tr>
</tbody>
</table>

## ListAccessTokenResult

Results returned when listing access tokens

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
<p>The starting offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of results to be returned</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
tokens<br />
<a href="/docs/graphql/objects#accesstokenmetadata"><code>[AccessTokenMetadata!]!</code></a>
</td>
<td>
<p>The token metadata themselves</p>
</td>
</tr>
</tbody>
</table>

## ListDomainsResult

The result obtained when listing DataHub Domains

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
<p>The number of Domains in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Domains in the result set</p>
</td>
</tr>
<tr>
<td>
domains<br />
<a href="/docs/graphql/objects#domain"><code>[Domain!]!</code></a>
</td>
<td>
<p>The Domains themselves</p>
</td>
</tr>
</tbody>
</table>

## ListGroupsResult

The result obtained when listing DataHub Groups

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
groups<br />
<a href="/docs/graphql/objects#corpgroup"><code>[CorpGroup!]!</code></a>
</td>
<td>
<p>The groups themselves</p>
</td>
</tr>
</tbody>
</table>

## ListIngestionSourcesResult

Results returned when listing ingestion sources

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
<p>The starting offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of results to be returned</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
ingestionSources<br />
<a href="/docs/graphql/objects#ingestionsource"><code>[IngestionSource!]!</code></a>
</td>
<td>
<p>The Ingestion Sources themselves</p>
</td>
</tr>
</tbody>
</table>

## ListOwnershipTypesResult

Results when listing custom ownership types.

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
<p>The starting offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of results to be returned</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
ownershipTypes<br />
<a href="/docs/graphql/objects#ownershiptypeentity"><code>[OwnershipTypeEntity!]!</code></a>
</td>
<td>
<p>The Custom Ownership Types themselves</p>
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

## ListPostsResult

The result obtained when listing Posts

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
<p>The number of Roles in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Roles in the result set</p>
</td>
</tr>
<tr>
<td>
posts<br />
<a href="/docs/graphql/objects#post"><code>[Post!]!</code></a>
</td>
<td>
<p>The Posts themselves</p>
</td>
</tr>
</tbody>
</table>

## ListQueriesResult

Results when listing entity queries

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
<p>The starting offset of the result set</p>
</td>
</tr>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of results to be returned</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
queries<br />
<a href="/docs/graphql/objects#queryentity"><code>[QueryEntity!]!</code></a>
</td>
<td>
<p>The Queries themselves</p>
</td>
</tr>
</tbody>
</table>

## ListRecommendationsResult

Results returned by the ListRecommendations query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
modules<br />
<a href="/docs/graphql/objects#recommendationmodule"><code>[RecommendationModule!]!</code></a>
</td>
<td>
<p>List of modules to show</p>
</td>
</tr>
</tbody>
</table>

## ListRolesResult

The result obtained when listing DataHub Roles

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
<p>The number of Roles in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Roles in the result set</p>
</td>
</tr>
<tr>
<td>
roles<br />
<a href="/docs/graphql/objects#datahubrole"><code>[DataHubRole!]!</code></a>
</td>
<td>
<p>The Roles themselves</p>
</td>
</tr>
</tbody>
</table>

## ListSecretsResult

Input for listing DataHub Secrets

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
total<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>The total number of results in the result set</p>
</td>
</tr>
<tr>
<td>
secrets<br />
<a href="/docs/graphql/objects#secret"><code>[Secret!]!</code></a>
</td>
<td>
<p>The secrets themselves</p>
</td>
</tr>
</tbody>
</table>

## ListTestsResult

The result obtained when listing DataHub Tests

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
<p>The number of Tests in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Tests in the result set</p>
</td>
</tr>
<tr>
<td>
tests<br />
<a href="/docs/graphql/objects#test"><code>[Test!]!</code></a>
</td>
<td>
<p>The Tests themselves</p>
</td>
</tr>
</tbody>
</table>

## ListUsersResult

The result obtained when listing DataHub Users

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
users<br />
<a href="/docs/graphql/objects#corpuser"><code>[CorpUser!]!</code></a>
</td>
<td>
<p>The users themselves</p>
</td>
</tr>
</tbody>
</table>

## ListViewsResult

The result obtained when listing DataHub Views

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
<p>The number of Views in the returned result set</p>
</td>
</tr>
<tr>
<td>
total<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The total number of Views in the result set</p>
</td>
</tr>
<tr>
<td>
views<br />
<a href="/docs/graphql/objects#datahubview"><code>[DataHubView!]!</code></a>
</td>
<td>
<p>The Views themselves</p>
</td>
</tr>
</tbody>
</table>

## ManagedIngestionConfig

Configurations related to managed, UI based ingestion

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
<p>Whether ingestion screen is enabled in the UI</p>
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

## Media

Media content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
<blockquote>Deprecated: No longer supported</blockquote>

<p>ModelProperties metadata of the MLFeature</p>
</td>
</tr>
<tr>
<td>
properties<br />
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
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags applied to entity</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the entity</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the entity</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#mlfeatureeditableproperties"><code>MLFeatureEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## MLFeatureEditableProperties

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
<p>The edited description</p>
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

- [EntityWithRelationships](/docs/graphql/interfaces#entitywithrelationships)
- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the ML Feature Table. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags applied to entity</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the entity</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the entity</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#mlfeaturetableeditableproperties"><code>MLFeatureTableEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## MLFeatureTableEditableProperties

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
<p>The edited description</p>
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
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
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
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the ML Model. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the entity</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the entity</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#mlmodeleditableproperties"><code>MLModelEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## MLModelEditableProperties

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
<p>The edited description</p>
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
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
<p>Status metadata of the MLModelGroup</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the ML Model Group. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags applied to entity</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the entity</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the entity</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#mlmodelgroupeditableproperties"><code>MLModelGroupEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## MLModelGroupEditableProperties

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
<p>The edited description</p>
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
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## MLPrimaryKey

An ML Primary Key Entity Note that this entity is incubating

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
lastIngested<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The timestamp for the last time this entity was ingested</p>
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
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>Tags applied to entity</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the entity</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the entity</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#mlprimarykeyeditableproperties"><code>MLPrimaryKeyEditableProperties</code></a>
</td>
<td>
<p>An additional set of of read write properties</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## MLPrimaryKeyEditableProperties

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
<p>The edited description</p>
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

## NamedBar

For consumption by UI only

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

</td>
</tr>
<tr>
<td>
segments<br />
<a href="/docs/graphql/objects#barsegment"><code>[BarSegment!]!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## NamedLine

For consumption by UI only

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

</td>
</tr>
<tr>
<td>
data<br />
<a href="/docs/graphql/objects#numericdatapoint"><code>[NumericDataPoint!]!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## Notebook

A Notebook Metadata Entity

<p style={{ marginBottom: "0.4em" }}><strong>Implements</strong></p>

- [Entity](/docs/graphql/interfaces#entity)
- [BrowsableEntity](/docs/graphql/interfaces#browsableentity)

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
<p>The primary key of the Notebook</p>
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
<p>The Notebook tool name</p>
</td>
</tr>
<tr>
<td>
notebookId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>An id unique within the Notebook tool</p>
</td>
</tr>
<tr>
<td>
info<br />
<a href="/docs/graphql/objects#notebookinfo"><code>NotebookInfo</code></a>
</td>
<td>
<p>Additional read only information about the Notebook</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#notebookeditableproperties"><code>NotebookEditableProperties</code></a>
</td>
<td>
<p>Additional read write properties about the Notebook</p>
</td>
</tr>
<tr>
<td>
ownership<br />
<a href="/docs/graphql/objects#ownership"><code>Ownership</code></a>
</td>
<td>
<p>Ownership metadata of the Notebook</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status metadata of the Notebook</p>
</td>
</tr>
<tr>
<td>
content<br />
<a href="/docs/graphql/objects#notebookcontent"><code>NotebookContent!</code></a>
</td>
<td>
<p>The content of this Notebook</p>
</td>
</tr>
<tr>
<td>
tags<br />
<a href="/docs/graphql/objects#globaltags"><code>GlobalTags</code></a>
</td>
<td>
<p>The tags associated with the Notebook</p>
</td>
</tr>
<tr>
<td>
institutionalMemory<br />
<a href="/docs/graphql/objects#institutionalmemory"><code>InstitutionalMemory</code></a>
</td>
<td>
<p>References to internal resources related to the Notebook</p>
</td>
</tr>
<tr>
<td>
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Notebook</p>
</td>
</tr>
<tr>
<td>
dataPlatformInstance<br />
<a href="/docs/graphql/objects#dataplatforminstance"><code>DataPlatformInstance</code></a>
</td>
<td>
<p>The specific instance of the data platform that this entity belongs to</p>
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
subTypes<br />
<a href="/docs/graphql/objects#subtypes"><code>SubTypes</code></a>
</td>
<td>
<p>Sub Types that this entity implements</p>
</td>
</tr>
<tr>
<td>
glossaryTerms<br />
<a href="/docs/graphql/objects#glossaryterms"><code>GlossaryTerms</code></a>
</td>
<td>
<p>The structured glossary terms associated with the notebook</p>
</td>
</tr>
<tr>
<td>
platform<br />
<a href="/docs/graphql/objects#dataplatform"><code>DataPlatform!</code></a>
</td>
<td>
<p>Standardized platform.</p>
</td>
</tr>
<tr>
<td>
browsePaths<br />
<a href="/docs/graphql/objects#browsepath"><code>[BrowsePath!]</code></a>
</td>
<td>
<p>The browse paths corresponding to the Notebook. If no Browse Paths have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
browsePathV2<br />
<a href="/docs/graphql/objects#browsepathv2"><code>BrowsePathV2</code></a>
</td>
<td>
<p>The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null.</p>
</td>
</tr>
<tr>
<td>
exists<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether or not this entity exists on DataHub</p>
</td>
</tr>
</tbody>
</table>

## NotebookCell

The Union of every NotebookCell

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
chartCell<br />
<a href="/docs/graphql/objects#chartcell"><code>ChartCell</code></a>
</td>
<td>
<p>The chart cell content. The will be non-null only when all other cell field is null.</p>
</td>
</tr>
<tr>
<td>
textCell<br />
<a href="/docs/graphql/objects#textcell"><code>TextCell</code></a>
</td>
<td>
<p>The text cell content. The will be non-null only when all other cell field is null.</p>
</td>
</tr>
<tr>
<td>
queryChell<br />
<a href="/docs/graphql/objects#querycell"><code>QueryCell</code></a>
</td>
<td>
<p>The query cell content. The will be non-null only when all other cell field is null.</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#notebookcelltype"><code>NotebookCellType!</code></a>
</td>
<td>
<p>The type of this Notebook cell</p>
</td>
</tr>
</tbody>
</table>

## NotebookContent

The actual content in a Notebook

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
cells<br />
<a href="/docs/graphql/objects#notebookcell"><code>[NotebookCell!]!</code></a>
</td>
<td>
<p>The content of a Notebook which is composed by a list of NotebookCell</p>
</td>
</tr>
</tbody>
</table>

## NotebookEditableProperties

Notebook properties that are editable via the UI This represents logical metadata,
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
<p>Description of the Notebook</p>
</td>
</tr>
</tbody>
</table>

## NotebookInfo

Additional read only information about a Notebook

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Display of the Notebook</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description of the Notebook</p>
</td>
</tr>
<tr>
<td>
externalUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Native platform URL of the Notebook</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#custompropertiesentry"><code>[CustomPropertiesEntry!]</code></a>
</td>
<td>
<p>A list of platform specific metadata tuples</p>
</td>
</tr>
<tr>
<td>
changeAuditStamps<br />
<a href="/docs/graphql/objects#changeauditstamps"><code>ChangeAuditStamps</code></a>
</td>
<td>
<p>Captures information about who created/last modified/deleted this Notebook and when</p>
</td>
</tr>
</tbody>
</table>

## NumericDataPoint

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
x<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
y<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## Operation

Operational info for an entity.

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
<p>The time at which the operation was reported</p>
</td>
</tr>
<tr>
<td>
actor<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Actor who issued this operation.</p>
</td>
</tr>
<tr>
<td>
operationType<br />
<a href="/docs/graphql/enums#operationtype"><code>OperationType!</code></a>
</td>
<td>
<p>Operation type of change.</p>
</td>
</tr>
<tr>
<td>
customOperationType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A custom operation type</p>
</td>
</tr>
<tr>
<td>
sourceType<br />
<a href="/docs/graphql/enums#operationsourcetype"><code>OperationSourceType</code></a>
</td>
<td>
<p>Source of the operation</p>
</td>
</tr>
<tr>
<td>
numAffectedRows<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>How many rows were affected by this operation.</p>
</td>
</tr>
<tr>
<td>
affectedDatasets<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>Which other datasets were affected by this operation.</p>
</td>
</tr>
<tr>
<td>
lastUpdatedTimestamp<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>When time at which the asset was actually updated</p>
</td>
</tr>
<tr>
<td>
partition<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Optional partition identifier</p>
</td>
</tr>
<tr>
<td>
customProperties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]</code></a>
</td>
<td>
<p>Custom operation properties</p>
</td>
</tr>
</tbody>
</table>

## Origin

Carries information about where an entity originated from.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#origintype"><code>OriginType!</code></a>
</td>
<td>
<p>Where an entity originated from. Either NATIVE or EXTERNAL</p>
</td>
</tr>
<tr>
<td>
externalType<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the identity provider.</p>
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
<a href="/docs/graphql/enums#ownershiptype"><code>OwnershipType</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>The type of the ownership. Deprecated - Use ownershipType field instead.</p>
</td>
</tr>
<tr>
<td>
ownershipType<br />
<a href="/docs/graphql/objects#ownershiptypeentity"><code>OwnershipTypeEntity</code></a>
</td>
<td>
<p>Ownership type information</p>
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
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the owned urn for tracking purposes e.g. when sibling nodes are merged together</p>
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

## OwnershipTypeEntity

A single Custom Ownership Type

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
<p>A primary key associated with the custom ownership type.</p>
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
info<br />
<a href="/docs/graphql/objects#ownershiptypeinfo"><code>OwnershipTypeInfo</code></a>
</td>
<td>
<p>Information about the Custom Ownership Type</p>
</td>
</tr>
<tr>
<td>
status<br />
<a href="/docs/graphql/objects#status"><code>Status</code></a>
</td>
<td>
<p>Status of the Custom Ownership Type</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from the Custom Ownership Type</p>

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

## OwnershipTypeInfo

Properties about an individual Custom Ownership Type.

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
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An Audit Stamp corresponding to the creation of this resource</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>An Audit Stamp corresponding to the update of this resource</p>
</td>
</tr>
</tbody>
</table>

## ParentContainersResult

All of the parent containers for a given entity. Returns parents with direct parent first followed by the parent's parent etc.

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of containers bubbling up for this entity</p>
</td>
</tr>
<tr>
<td>
containers<br />
<a href="/docs/graphql/objects#container"><code>[Container!]!</code></a>
</td>
<td>
<p>A list of parent containers in order from direct parent, to parent&#39;s parent etc. If there are no containers, return an emty list</p>
</td>
</tr>
</tbody>
</table>

## ParentNodesResult

All of the parent nodes for GlossaryTerms and GlossaryNodes

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
count<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>The number of parent nodes bubbling up for this entity</p>
</td>
</tr>
<tr>
<td>
nodes<br />
<a href="/docs/graphql/objects#glossarynode"><code>[GlossaryNode!]!</code></a>
</td>
<td>
<p>A list of parent nodes in order from direct parent, to parent&#39;s parent etc. If there are no nodes, return an empty list</p>
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
type<br />
<a href="/docs/graphql/enums#partitiontype"><code>PartitionType!</code></a>
</td>
<td>
<p>The partition type</p>
</td>
</tr>
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
<tr>
<td>
manageIdentities<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to manage users &amp; groups</p>
</td>
</tr>
<tr>
<td>
generatePersonalAccessTokens<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to generate personal access tokens</p>
</td>
</tr>
<tr>
<td>
createDomains<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to create new Domains</p>
</td>
</tr>
<tr>
<td>
manageDomains<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to manage Domains</p>
</td>
</tr>
<tr>
<td>
manageIngestion<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is able to manage UI-based ingestion</p>
</td>
</tr>
<tr>
<td>
manageSecrets<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is able to manage UI-based secrets</p>
</td>
</tr>
<tr>
<td>
manageTokens<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to manage tokens on behalf of other users.</p>
</td>
</tr>
<tr>
<td>
manageTests<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is able to manage Tests</p>
</td>
</tr>
<tr>
<td>
manageGlossaries<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to manage Glossaries</p>
</td>
</tr>
<tr>
<td>
manageUserCredentials<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user is able to manage user credentials</p>
</td>
</tr>
<tr>
<td>
createTags<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to create new Tags</p>
</td>
</tr>
<tr>
<td>
manageTags<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to create and delete all Tags</p>
</td>
</tr>
<tr>
<td>
manageGlobalViews<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to create, update, and delete global views.</p>
</td>
</tr>
<tr>
<td>
manageOwnershipTypes<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the user should be able to create, update, and delete ownership types.</p>
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

DEPRECATED
TODO: Eventually get rid of this in favor of DataHub Policy
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

## PolicyMatchCriterion

Criterion to define relationship between field and values

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
<p>The name of the field that the criterion refers to
e.g. entity_type, entity_urn, domain</p>
</td>
</tr>
<tr>
<td>
values<br />
<a href="/docs/graphql/objects#policymatchcriterionvalue"><code>[PolicyMatchCriterionValue!]!</code></a>
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

## PolicyMatchCriterionValue

Value in PolicyMatchCriterion with hydrated entity if value is urn

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
<p>The value of the field to match</p>
</td>
</tr>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>Hydrated entities of the above values. Only set if the value is an urn</p>
</td>
</tr>
</tbody>
</table>

## PolicyMatchFilter

Filter object that encodes a complex filter logic with OR + AND

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
criteria<br />
<a href="/docs/graphql/objects#policymatchcriterion"><code>[PolicyMatchCriterion!]</code></a>
</td>
<td>
<p>List of criteria to apply</p>
</td>
</tr>
</tbody>
</table>

## Post

Input provided when creating a Post

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
<p>The primary key of the Post</p>
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
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Granular API for querying edges extending from the Post</p>

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
<a href="/docs/graphql/objects#postcontent"><code>PostContent!</code></a>
</td>
<td>
<p>The content of the post</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>When the post was last modified</p>
</td>
</tr>
</tbody>
</table>

## PostContent

Post content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

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
<a href="/docs/graphql/objects#media"><code>Media</code></a>
</td>
<td>
<p>Optional media contained in the post</p>
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

## Privileges

Object that encodes the privileges the actor has for a given resource

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
privileges<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>
<p>Granted Privileges</p>
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

## QueriesTabConfig

Configuration for the queries tab

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
queriesTabResultSize<br />
<a href="/docs/graphql/scalars#int"><code>Int</code></a>
</td>
<td>
<p>Number of queries to show in the queries tab</p>
</td>
</tr>
</tbody>
</table>

## QueryCell

A Notebook cell which contains Query as content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
cellTitle<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Title of the cell</p>
</td>
</tr>
<tr>
<td>
cellId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id for the cell.</p>
</td>
</tr>
<tr>
<td>
changeAuditStamps<br />
<a href="/docs/graphql/objects#changeauditstamps"><code>ChangeAuditStamps</code></a>
</td>
<td>
<p>Captures information about who created/last modified/deleted this TextCell and when</p>
</td>
</tr>
<tr>
<td>
rawQuery<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Raw query to explain some specific logic in a Notebook</p>
</td>
</tr>
<tr>
<td>
lastExecuted<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp</code></a>
</td>
<td>
<p>Captures information about who last executed this query cell and when</p>
</td>
</tr>
</tbody>
</table>

## QueryEntity

An individual Query

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
<p>A primary key associated with the Query</p>
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
properties<br />
<a href="/docs/graphql/objects#queryproperties"><code>QueryProperties</code></a>
</td>
<td>
<p>Properties about the Query</p>
</td>
</tr>
<tr>
<td>
subjects<br />
<a href="/docs/graphql/objects#querysubject"><code>[QuerySubject!]</code></a>
</td>
<td>
<p>Subjects for the query</p>
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
</tbody>
</table>

## QueryProperties

Properties about an individual Query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
statement<br />
<a href="/docs/graphql/objects#querystatement"><code>QueryStatement!</code></a>
</td>
<td>
<p>The Query statement itself</p>
</td>
</tr>
<tr>
<td>
source<br />
<a href="/docs/graphql/enums#querysource"><code>QuerySource!</code></a>
</td>
<td>
<p>The source of the Query</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The name of the Query</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>The description of the Query</p>
</td>
</tr>
<tr>
<td>
created<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An Audit Stamp corresponding to the creation of this resource</p>
</td>
</tr>
<tr>
<td>
lastModified<br />
<a href="/docs/graphql/objects#auditstamp"><code>AuditStamp!</code></a>
</td>
<td>
<p>An Audit Stamp corresponding to the update of this resource</p>
</td>
</tr>
</tbody>
</table>

## QueryStatement

An individual Query Statement

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
<p>The query statement value</p>
</td>
</tr>
<tr>
<td>
language<br />
<a href="/docs/graphql/enums#querylanguage"><code>QueryLanguage!</code></a>
</td>
<td>
<p>The language for the Query Statement</p>
</td>
</tr>
</tbody>
</table>

## QuerySubject

The subject for a Query

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
<p>The dataset which is the subject of the Query</p>
</td>
</tr>
</tbody>
</table>

## QuickFilter

A quick filter in search and auto-complete

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
<p>Name of field to filter by</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Value to filter on</p>
</td>
</tr>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>Entity that the value maps to if any</p>
</td>
</tr>
</tbody>
</table>

## RawAspect

Payload representing data about a single aspect

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
aspectName<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the aspect</p>
</td>
</tr>
<tr>
<td>
payload<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>JSON string containing the aspect&#39;s payload</p>
</td>
</tr>
<tr>
<td>
renderSpec<br />
<a href="/docs/graphql/objects#aspectrenderspec"><code>AspectRenderSpec</code></a>
</td>
<td>
<p>Details for the frontend on how the raw aspect should be rendered</p>
</td>
</tr>
</tbody>
</table>

## RecommendationContent

Content to display within each recommendation module

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
<p>String representation of content</p>
</td>
</tr>
<tr>
<td>
entity<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity</code></a>
</td>
<td>
<p>Entity being recommended. Empty if the content being recommended is not an entity</p>
</td>
</tr>
<tr>
<td>
params<br />
<a href="/docs/graphql/objects#recommendationparams"><code>RecommendationParams</code></a>
</td>
<td>
<p>Additional context required to generate the the recommendation</p>
</td>
</tr>
</tbody>
</table>

## RecommendationModule

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Title of the module to display</p>
</td>
</tr>
<tr>
<td>
moduleId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id of the module being recommended</p>
</td>
</tr>
<tr>
<td>
renderType<br />
<a href="/docs/graphql/enums#recommendationrendertype"><code>RecommendationRenderType!</code></a>
</td>
<td>
<p>Type of rendering that defines how the module should be rendered</p>
</td>
</tr>
<tr>
<td>
content<br />
<a href="/docs/graphql/objects#recommendationcontent"><code>[RecommendationContent!]!</code></a>
</td>
<td>
<p>List of content to display inside the module</p>
</td>
</tr>
</tbody>
</table>

## RecommendationParams

Parameters required to render a recommendation of a given type

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
searchParams<br />
<a href="/docs/graphql/objects#searchparams"><code>SearchParams</code></a>
</td>
<td>
<p>Context to define the search recommendations</p>
</td>
</tr>
<tr>
<td>
entityProfileParams<br />
<a href="/docs/graphql/objects#entityprofileparams"><code>EntityProfileParams</code></a>
</td>
<td>
<p>Context to define the entity profile page</p>
</td>
</tr>
<tr>
<td>
contentParams<br />
<a href="/docs/graphql/objects#contentparams"><code>ContentParams</code></a>
</td>
<td>
<p>Context about the recommendation</p>
</td>
</tr>
</tbody>
</table>

## ResetToken

Token that allows native users to reset their credentials

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
resetToken<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The reset token</p>
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
<a href="/docs/graphql/scalars#string"><code>String</code></a>
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
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Whether of not to apply the filter to all resources of the type</p>
</td>
</tr>
<tr>
<td>
filter<br />
<a href="/docs/graphql/objects#policymatchfilter"><code>PolicyMatchFilter</code></a>
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

## Role

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
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Id of the Role</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#roleproperties"><code>RoleProperties!</code></a>
</td>
<td>
<p>Role properties to include Request Access Url</p>
</td>
</tr>
<tr>
<td>
actors<br />
<a href="/docs/graphql/objects#actor"><code>Actor!</code></a>
</td>
<td>
<p>A standard Entity Type</p>
</td>
</tr>
</tbody>
</table>

## RoleAssociation

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
role<br />
<a href="/docs/graphql/objects#role"><code>Role!</code></a>
</td>
<td>
<p>The Role entity itself</p>
</td>
</tr>
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together</p>
</td>
</tr>
</tbody>
</table>

## RoleProperties

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
<p>Name of the Role in an organisation </p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Description about the role</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Role type can be READ, WRITE or ADMIN</p>
</td>
</tr>
<tr>
<td>
requestUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Url to request a role for a user in an organisation</p>
</td>
</tr>
</tbody>
</table>

## RoleUser

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
user<br />
<a href="/docs/graphql/objects#corpuser"><code>CorpUser!</code></a>
</td>
<td>
<p>Linked corp user of a role</p>
</td>
</tr>
</tbody>
</table>

## Row

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
values<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
cells<br />
<a href="/docs/graphql/objects#cell"><code>[Cell!]</code></a>
</td>
<td>

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
<tr>
<td>
foreignKeys<br />
<a href="/docs/graphql/objects#foreignkeyconstraint"><code>[ForeignKeyConstraint]</code></a>
</td>
<td>
<p>Client provided list of foreign key constraints</p>
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
<tr>
<td>
lastObserved<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>The time at which the schema metadata information was last ingested</p>
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
label<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Human readable label for the field. Not supplied by all data sources</p>
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

## SchemaFieldBlame

Blame for a single field

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
<p>Flattened name of a schema field</p>
</td>
</tr>
<tr>
<td>
schemaFieldChange<br />
<a href="/docs/graphql/objects#schemafieldchange"><code>SchemaFieldChange!</code></a>
</td>
<td>
<p>Attributes identifying a field change</p>
</td>
</tr>
</tbody>
</table>

## SchemaFieldChange

Attributes identifying a field change

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
<p>The time at which the schema was updated</p>
</td>
</tr>
<tr>
<td>
lastSemanticVersion<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The last semantic version that this schema was changed in</p>
</td>
</tr>
<tr>
<td>
versionStamp<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Version stamp of the change</p>
</td>
</tr>
<tr>
<td>
changeType<br />
<a href="/docs/graphql/enums#changeoperationtype"><code>ChangeOperationType!</code></a>
</td>
<td>
<p>The type of the change</p>
</td>
</tr>
<tr>
<td>
lastSchemaFieldChange<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Last column update, such as Added/Modified/Removed in v1.2.3.</p>
</td>
</tr>
</tbody>
</table>

## SchemaFieldEntity

Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
schema field

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
<p>Primary key of the schema field</p>
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
fieldPath<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Field path identifying the field in its dataset</p>
</td>
</tr>
<tr>
<td>
parent<br />
<a href="/docs/graphql/interfaces#entity"><code>Entity!</code></a>
</td>
<td>
<p>The field&#39;s parent.</p>
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
</tbody>
</table>

## SchemaFieldRef

A Dataset schema field (i.e. column)

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
<p>A schema field urn</p>
</td>
</tr>
<tr>
<td>
path<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>A schema field path</p>
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
foreignKeys<br />
<a href="/docs/graphql/objects#foreignkeyconstraint"><code>[ForeignKeyConstraint]</code></a>
</td>
<td>
<p>Client provided list of foreign key constraints</p>
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

## ScrollAcrossLineageResults

Results returned by issueing a search across relationships query using scroll API

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
nextScrollId<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Opaque ID to pass to the next request to the server</p>
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
<a href="/docs/graphql/objects#searchacrosslineageresult"><code>[SearchAcrossLineageResult!]!</code></a>
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

## ScrollResults

Results returned by issuing a search query

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
nextScrollId<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Opaque ID to pass to the next request to the server</p>
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
<p>The search result entities for a scroll request</p>
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

## SearchAcrossLineageResult

Individual search result from a search across relationships query (has added metadata about the path)

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
insights<br />
<a href="/docs/graphql/objects#searchinsight"><code>[SearchInsight!]</code></a>
</td>
<td>
<p>Insights about why the search result was matched</p>
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
<tr>
<td>
paths<br />
<a href="/docs/graphql/objects#entitypath"><code>[EntityPath]</code></a>
</td>
<td>
<p>Optional list of entities between the source and destination node</p>
</td>
</tr>
<tr>
<td>
degree<br />
<a href="/docs/graphql/scalars#int"><code>Int!</code></a>
</td>
<td>
<p>Degree of relationship (number of hops to get to entity)</p>
</td>
</tr>
</tbody>
</table>

## SearchAcrossLineageResults

Results returned by issueing a search across relationships query

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
<a href="/docs/graphql/objects#searchacrosslineageresult"><code>[SearchAcrossLineageResult!]!</code></a>
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
<tr>
<td>
freshness<br />
<a href="/docs/graphql/objects#freshnessstats"><code>FreshnessStats</code></a>
</td>
<td>
<p>Optional freshness characteristics of this query (cached, staleness etc.)</p>
</td>
</tr>
</tbody>
</table>

## SearchInsight

Insights about why a search result was returned or ranked in the way that it was

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
text<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The insight to display</p>
</td>
</tr>
<tr>
<td>
icon<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional emoji to display in front of the text</p>
</td>
</tr>
</tbody>
</table>

## SearchParams

Context to define the search recommendations

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

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
<p>Search query</p>
</td>
</tr>
<tr>
<td>
filters<br />
<a href="/docs/graphql/objects#facetfilter"><code>[FacetFilter!]</code></a>
</td>
<td>
<p>Filters</p>
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
insights<br />
<a href="/docs/graphql/objects#searchinsight"><code>[SearchInsight!]</code></a>
</td>
<td>
<p>Insights about why the search result was matched</p>
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

## Secret

A referencible secret stored in DataHub's system. Notice that we do not return the actual secret value.

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
<p>The urn of the secret</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The name of the secret</p>
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

## SecretValue

A plaintext secret value

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
<p>The name of the secret</p>
</td>
</tr>
<tr>
<td>
value<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The plaintext value of the secret.</p>
</td>
</tr>
</tbody>
</table>

## SemanticVersionStruct

Properties identify a semantic version

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
semanticVersion<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Semantic version of the change</p>
</td>
</tr>
<tr>
<td>
semanticVersionTimestamp<br />
<a href="/docs/graphql/scalars#long"><code>Long</code></a>
</td>
<td>
<p>Semantic version timestamp</p>
</td>
</tr>
<tr>
<td>
versionStamp<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Version stamp of the change</p>
</td>
</tr>
</tbody>
</table>

## SiblingProperties

Metadata about the entity's siblings

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
isPrimary<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>If this entity is the primary sibling among the sibling set</p>
</td>
</tr>
<tr>
<td>
siblings<br />
<a href="/docs/graphql/interfaces#entity"><code>[Entity]</code></a>
</td>
<td>
<p>The sibling entities</p>
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

## StepStateResult

A single step state

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id of the step</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#stringmapentry"><code>[StringMapEntry!]!</code></a>
</td>
<td>
<p>The properties for the step state</p>
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

## StructuredReport

A flexible carrier for structured results of an execution request.

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
<p>The type of the structured report. (e.g. INGESTION_REPORT, TEST_CONNECTION_REPORT, etc.)</p>
</td>
</tr>
<tr>
<td>
serializedValue<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The serialized value of the structured report</p>
</td>
</tr>
<tr>
<td>
contentType<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The content-type of the serialized value (e.g. application/json, application/json;gzip etc.)</p>
</td>
</tr>
</tbody>
</table>

## SubTypes

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
typeNames<br />
<a href="/docs/graphql/scalars#string"><code>[String!]</code></a>
</td>
<td>
<p>The sub-types that this entity implements. e.g. Datasets that are views will implement the &quot;view&quot; subtype</p>
</td>
</tr>
</tbody>
</table>

## SystemFreshness

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
systemName<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Name of the system</p>
</td>
</tr>
<tr>
<td>
freshnessMillis<br />
<a href="/docs/graphql/scalars#long"><code>Long!</code></a>
</td>
<td>
<p>The latest timestamp in millis of the system that was used to respond to this query
In case a cache was consulted, this reflects the freshness of the cache
In case an index was consulted, this reflects the freshness of the index</p>
</td>
</tr>
</tbody>
</table>

## TableChart

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
columns<br />
<a href="/docs/graphql/scalars#string"><code>[String!]!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
rows<br />
<a href="/docs/graphql/objects#row"><code>[Row!]!</code></a>
</td>
<td>

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
<blockquote>Deprecated: No longer supported</blockquote>

<p>A unique identifier for the Tag. Deprecated - Use properties.name field instead.</p>
</td>
</tr>
<tr>
<td>
properties<br />
<a href="/docs/graphql/objects#tagproperties"><code>TagProperties</code></a>
</td>
<td>
<p>Additional properties about the Tag</p>
</td>
</tr>
<tr>
<td>
editableProperties<br />
<a href="/docs/graphql/objects#editabletagproperties"><code>EditableTagProperties</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Additional read write properties about the Tag
Deprecated! Use &#39;properties&#39; field instead.</p>
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
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, use properties.description field instead</p>
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
<tr>
<td>
associatedUrn<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together</p>
</td>
</tr>
</tbody>
</table>

## TagProperties

Properties for a DataHub Tag

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
<p>A display name for the Tag</p>
</td>
</tr>
<tr>
<td>
description<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>A description of the Tag</p>
</td>
</tr>
<tr>
<td>
colorHex<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>An optional RGB hex code for a Tag color, e.g. #FFFFFF</p>
</td>
</tr>
</tbody>
</table>

## TelemetryConfig

Configurations related to tracking users in the app

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
enableThirdPartyLogging<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean</code></a>
</td>
<td>
<p>Env variable for whether or not third party logging should be enabled for this instance</p>
</td>
</tr>
</tbody>
</table>

## Test

A metadata entity representing a DataHub Test

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
<p>The primary key of the Test itself</p>
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
<a href="/docs/graphql/objects#testdefinition"><code>TestDefinition!</code></a>
</td>
<td>
<p>Definition for the test</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<p>Unused for tests</p>

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

## TestDefinition

Definition of the test

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
json<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>JSON-based def for the test</p>
</td>
</tr>
</tbody>
</table>

## TestResult

The result of running a test

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
test<br />
<a href="/docs/graphql/objects#test"><code>Test</code></a>
</td>
<td>
<p>The test itself, or null if the test has been deleted</p>
</td>
</tr>
<tr>
<td>
type<br />
<a href="/docs/graphql/enums#testresulttype"><code>TestResultType!</code></a>
</td>
<td>
<p>The final result, e.g. either SUCCESS or FAILURE.</p>
</td>
</tr>
</tbody>
</table>

## TestResults

A set of test results

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
passing<br />
<a href="/docs/graphql/objects#testresult"><code>[TestResult!]!</code></a>
</td>
<td>
<p>The tests passing</p>
</td>
</tr>
<tr>
<td>
failing<br />
<a href="/docs/graphql/objects#testresult"><code>[TestResult!]!</code></a>
</td>
<td>
<p>The tests failing</p>
</td>
</tr>
</tbody>
</table>

## TestsConfig

Configurations related to DataHub Tests feature

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
<p>Whether Tests feature is enabled</p>
</td>
</tr>
</tbody>
</table>

## TextCell

A Notebook cell which contains text as content

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
cellTitle<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Title of the cell</p>
</td>
</tr>
<tr>
<td>
cellId<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique id for the cell.</p>
</td>
</tr>
<tr>
<td>
changeAuditStamps<br />
<a href="/docs/graphql/objects#changeauditstamps"><code>ChangeAuditStamps</code></a>
</td>
<td>
<p>Captures information about who created/last modified/deleted this TextCell and when</p>
</td>
</tr>
<tr>
<td>
text<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The actual text in a TextCell in a Notebook</p>
</td>
</tr>
</tbody>
</table>

## TimeSeriesChart

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
title<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
lines<br />
<a href="/docs/graphql/objects#namedline"><code>[NamedLine!]!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
dateRange<br />
<a href="/docs/graphql/objects#daterange"><code>DateRange!</code></a>
</td>
<td>

</td>
</tr>
<tr>
<td>
interval<br />
<a href="/docs/graphql/enums#dateinterval"><code>DateInterval!</code></a>
</td>
<td>

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

## UpdateStepStateResult

Result returned when fetching step state

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
id<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Id of the step</p>
</td>
</tr>
<tr>
<td>
succeeded<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the update succeeded.</p>
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

## VersionedDataset

A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle

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
container<br />
<a href="/docs/graphql/objects#container"><code>Container</code></a>
</td>
<td>
<p>The parent container in which the entity resides</p>
</td>
</tr>
<tr>
<td>
parentContainers<br />
<a href="/docs/graphql/objects#parentcontainersresult"><code>ParentContainersResult</code></a>
</td>
<td>
<p>Recursively get the lineage of containers for this entity</p>
</td>
</tr>
<tr>
<td>
name<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>Unique guid for dataset
No longer to be used as the Dataset display name. Use properties.name instead</p>
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
<p>The deprecation status of the dataset</p>
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
domain<br />
<a href="/docs/graphql/objects#domainassociation"><code>DomainAssociation</code></a>
</td>
<td>
<p>The Domain associated with the Dataset</p>
</td>
</tr>
<tr>
<td>
health<br />
<a href="/docs/graphql/objects#health"><code>[Health!]</code></a>
</td>
<td>
<p>Experimental! The resolved health status of the Dataset</p>
</td>
</tr>
<tr>
<td>
schema<br />
<a href="/docs/graphql/objects#schema"><code>Schema</code></a>
</td>
<td>
<p>Schema metadata of the dataset</p>
</td>
</tr>
<tr>
<td>
subTypes<br />
<a href="/docs/graphql/objects#subtypes"><code>SubTypes</code></a>
</td>
<td>
<p>Sub Types that this entity implements</p>
</td>
</tr>
<tr>
<td>
viewProperties<br />
<a href="/docs/graphql/objects#viewproperties"><code>ViewProperties</code></a>
</td>
<td>
<p>View related properties. Only relevant if subtypes field contains view.</p>
</td>
</tr>
<tr>
<td>
origin<br />
<a href="/docs/graphql/enums#fabrictype"><code>FabricType!</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>Deprecated, see the properties field instead
Environment in which the dataset belongs to or where it was generated
Note that this field will soon be deprecated in favor of a more standardized concept of Environment</p>
</td>
</tr>
<tr>
<td>
relationships<br />
<a href="/docs/graphql/objects#entityrelationshipsresult"><code>EntityRelationshipsResult</code></a>
</td>
<td>
<blockquote>Deprecated: No longer supported</blockquote>

<p>No-op, has to be included due to model</p>

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

## ViewProperties

Properties about a Dataset of type view

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
materialized<br />
<a href="/docs/graphql/scalars#boolean"><code>Boolean!</code></a>
</td>
<td>
<p>Whether the view is materialized or not</p>
</td>
</tr>
<tr>
<td>
logic<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The logic associated with the view, most commonly a SQL statement</p>
</td>
</tr>
<tr>
<td>
language<br />
<a href="/docs/graphql/scalars#string"><code>String!</code></a>
</td>
<td>
<p>The language in which the view logic is written, for example SQL</p>
</td>
</tr>
</tbody>
</table>

## ViewsConfig

Configurations related to DataHub Views feature

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
<p>Whether Views feature is enabled</p>
</td>
</tr>
</tbody>
</table>

## VisualConfig

Configurations related to visual appearance of the app

<p style={{ marginBottom: "0.4em" }}><strong>Fields</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
logoUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Custom logo url for the homepage &amp; top banner</p>
</td>
</tr>
<tr>
<td>
faviconUrl<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>
<p>Custom favicon url for the homepage &amp; top banner</p>
</td>
</tr>
<tr>
<td>
queriesTab<br />
<a href="/docs/graphql/objects#queriestabconfig"><code>QueriesTabConfig</code></a>
</td>
<td>
<p>Configuration for the queries tab</p>
</td>
</tr>
<tr>
<td>
entityProfiles<br />
<a href="/docs/graphql/objects#entityprofilesconfig"><code>EntityProfilesConfig</code></a>
</td>
<td>
<p>Configuration for the queries tab</p>
</td>
</tr>
</tbody>
</table>
