---
id: queries
title: Queries
slug: queries
sidebar_position: 1
---

## aggregateAcrossEntities

**Type:** [AggregateResults](/docs/graphql/objects#aggregateresults)

Aggregate across DataHub entities

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#aggregateacrossentitiesinput"><code>AggregateAcrossEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## appConfig

**Type:** [AppConfig](/docs/graphql/objects#appconfig)

Fetch configurations
Used by DataHub UI

## assertion

**Type:** [Assertion](/docs/graphql/objects#assertion)

Fetch an Assertion by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## autoComplete

**Type:** [AutoCompleteResults](/docs/graphql/objects#autocompleteresults)

Autocomplete a search query against a specific DataHub Entity Type

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#autocompleteinput"><code>AutoCompleteInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## autoCompleteForMultiple

**Type:** [AutoCompleteMultipleResults](/docs/graphql/objects#autocompletemultipleresults)

Autocomplete a search query against a specific set of DataHub Entity Types

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#autocompletemultipleinput"><code>AutoCompleteMultipleInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## batchGetStepStates

**Type:** [BatchGetStepStatesResult!](/docs/graphql/objects#batchgetstepstatesresult)

Batch fetch the state for a set of steps.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#batchgetstepstatesinput"><code>BatchGetStepStatesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## browse

**Type:** [BrowseResults](/docs/graphql/objects#browseresults)

Hierarchically browse a specific type of DataHub Entity by path
Used by explore in the UI

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#browseinput"><code>BrowseInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## browsePaths

**Type:** [[BrowsePath!]](/docs/graphql/objects#browsepath)

Retrieve the browse paths corresponding to an entity

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#browsepathsinput"><code>BrowsePathsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## browseV2

**Type:** [BrowseResultsV2](/docs/graphql/objects#browseresultsv2)

Browse for different entities by getting organizational groups and their
aggregated counts + content. Uses browsePathsV2 aspect and replaces our old
browse endpoint.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#browsev2input"><code>BrowseV2Input!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## chart

**Type:** [Chart](/docs/graphql/objects#chart)

Fetch a Chart by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## container

**Type:** [Container](/docs/graphql/objects#container)

Fetch an Entity Container by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## corpGroup

**Type:** [CorpGroup](/docs/graphql/objects#corpgroup)

Fetch a CorpGroup, representing a DataHub platform group by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## corpUser

**Type:** [CorpUser](/docs/graphql/objects#corpuser)

Fetch a CorpUser, representing a DataHub platform user, by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dashboard

**Type:** [Dashboard](/docs/graphql/objects#dashboard)

Fetch a Dashboard by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dataFlow

**Type:** [DataFlow](/docs/graphql/objects#dataflow)

Fetch a Data Flow (or Data Pipeline) by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dataJob

**Type:** [DataJob](/docs/graphql/objects#datajob)

Fetch a Data Job (or Data Task) by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dataPlatform

**Type:** [DataPlatform](/docs/graphql/objects#dataplatform)

Fetch a Data Platform by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dataProduct

**Type:** [DataProduct](/docs/graphql/objects#dataproduct)

Fetch a DataProduct by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## dataset

**Type:** [Dataset](/docs/graphql/objects#dataset)

Fetch a Dataset by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## domain

**Type:** [Domain](/docs/graphql/objects#domain)

Fetch a Domain by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## entities

**Type:** [[Entity]](/docs/graphql/interfaces#entity)

Gets entities based on their urns

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

</td>
</tr>
</tbody>
</table>

## entity

**Type:** [Entity](/docs/graphql/interfaces#entity)

Gets an entity based on its urn

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

</td>
</tr>
</tbody>
</table>

## entityExists

**Type:** [Boolean](/docs/graphql/scalars#boolean)

Get whether or not not an entity exists

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

</td>
</tr>
</tbody>
</table>

## executionRequest

**Type:** [ExecutionRequest](/docs/graphql/objects#executionrequest)

Get an execution request
urn: The primary key associated with the execution request.

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

</td>
</tr>
</tbody>
</table>

## getAccessToken

**Type:** [AccessToken](/docs/graphql/objects#accesstoken)

Generates an access token for DataHub APIs for a particular user & of a particular type
Deprecated, use createAccessToken instead

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getaccesstokeninput"><code>GetAccessTokenInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getAnalyticsCharts

**Type:** [[AnalyticsChartGroup!]!](/docs/graphql/objects#analyticschartgroup)

Retrieves a set of server driven Analytics Charts to render in the UI

## getEntityCounts

**Type:** [EntityCountResults](/docs/graphql/objects#entitycountresults)

Fetches the number of entities ingested by type

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#entitycountinput"><code>EntityCountInput</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getGrantedPrivileges

**Type:** [Privileges](/docs/graphql/objects#privileges)

Get all granted privileges for the given actor and resource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getgrantedprivilegesinput"><code>GetGrantedPrivilegesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getHighlights

**Type:** [[Highlight!]!](/docs/graphql/objects#highlight)

Retrieves a set of server driven Analytics Highlight Cards to render in the UI

## getInviteToken

**Type:** [InviteToken](/docs/graphql/objects#invitetoken)

Get invite token

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getinvitetokeninput"><code>GetInviteTokenInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getMetadataAnalyticsCharts

**Type:** [[AnalyticsChartGroup!]!](/docs/graphql/objects#analyticschartgroup)

Retrieves a set of charts regarding the ingested metadata

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#metadataanalyticsinput"><code>MetadataAnalyticsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getQuickFilters

**Type:** [GetQuickFiltersResult](/docs/graphql/objects#getquickfiltersresult)

Get quick filters to display in auto-complete

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getquickfiltersinput"><code>GetQuickFiltersInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getRootGlossaryNodes

**Type:** [GetRootGlossaryNodesResult](/docs/graphql/objects#getrootglossarynodesresult)

Get all GlossaryNodes without a parentNode

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getrootglossaryentitiesinput"><code>GetRootGlossaryEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getRootGlossaryTerms

**Type:** [GetRootGlossaryTermsResult](/docs/graphql/objects#getrootglossarytermsresult)

Get all GlossaryTerms without a parentNode

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getrootglossaryentitiesinput"><code>GetRootGlossaryEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getSchemaBlame

**Type:** [GetSchemaBlameResult](/docs/graphql/objects#getschemablameresult)

Returns the most recent changes made to each column in a dataset at each dataset version.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getschemablameinput"><code>GetSchemaBlameInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getSchemaVersionList

**Type:** [GetSchemaVersionListResult](/docs/graphql/objects#getschemaversionlistresult)

Returns the list of schema versions for a dataset.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getschemaversionlistinput"><code>GetSchemaVersionListInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## getSecretValues

**Type:** [[SecretValue!]](/docs/graphql/objects#secretvalue)

Fetch the values of a set of secrets. The caller must have the MANAGE_SECRETS
privilege to use.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#getsecretvaluesinput"><code>GetSecretValuesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## globalViewsSettings

**Type:** [GlobalViewsSettings](/docs/graphql/objects#globalviewssettings)

Fetch the Global Settings related to the Views feature.
Requires the 'Manage Global Views' Platform Privilege.

## glossaryNode

**Type:** [GlossaryNode](/docs/graphql/objects#glossarynode)

Fetch a Glossary Node by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## glossaryTerm

**Type:** [GlossaryTerm](/docs/graphql/objects#glossaryterm)

Fetch a Glossary Term by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## ingestionSource

**Type:** [IngestionSource](/docs/graphql/objects#ingestionsource)

Fetch a specific ingestion source
urn: The primary key associated with the ingestion source.

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

</td>
</tr>
</tbody>
</table>

## isAnalyticsEnabled

**Type:** [Boolean!](/docs/graphql/scalars#boolean)

Deprecated, use appConfig Query instead
Whether the analytics feature is enabled in the UI

## listAccessTokens

**Type:** [ListAccessTokenResult!](/docs/graphql/objects#listaccesstokenresult)

List access tokens stored in DataHub.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listaccesstokeninput"><code>ListAccessTokenInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listDataProductAssets

**Type:** [SearchResults](/docs/graphql/objects#searchresults)

List Data Product assets for a given urn

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

</td>
</tr>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#searchacrossentitiesinput"><code>SearchAcrossEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listDomains

**Type:** [ListDomainsResult](/docs/graphql/objects#listdomainsresult)

List all DataHub Domains

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listdomainsinput"><code>ListDomainsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listGlobalViews

**Type:** [ListViewsResult](/docs/graphql/objects#listviewsresult)

List Global DataHub Views

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listglobalviewsinput"><code>ListGlobalViewsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listGroups

**Type:** [ListGroupsResult](/docs/graphql/objects#listgroupsresult)

List all DataHub Groups

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listgroupsinput"><code>ListGroupsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listIngestionSources

**Type:** [ListIngestionSourcesResult](/docs/graphql/objects#listingestionsourcesresult)

List all ingestion sources

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listingestionsourcesinput"><code>ListIngestionSourcesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listMyViews

**Type:** [ListViewsResult](/docs/graphql/objects#listviewsresult)

List DataHub Views owned by the current user

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listmyviewsinput"><code>ListMyViewsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listOwnershipTypes

**Type:** [ListOwnershipTypesResult!](/docs/graphql/objects#listownershiptypesresult)

List Custom Ownership Types

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listownershiptypesinput"><code>ListOwnershipTypesInput!</code></a>
</td>
<td>
<p>Input required for listing custom ownership types</p>
</td>
</tr>
</tbody>
</table>

## listPolicies

**Type:** [ListPoliciesResult](/docs/graphql/objects#listpoliciesresult)

List all DataHub Access Policies

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listpoliciesinput"><code>ListPoliciesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listPosts

**Type:** [ListPostsResult](/docs/graphql/objects#listpostsresult)

List all Posts

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listpostsinput"><code>ListPostsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listQueries

**Type:** [ListQueriesResult](/docs/graphql/objects#listqueriesresult)

List Dataset Queries

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listqueriesinput"><code>ListQueriesInput!</code></a>
</td>
<td>
<p>Input required for listing queries</p>
</td>
</tr>
</tbody>
</table>

## listRecommendations

**Type:** [ListRecommendationsResult](/docs/graphql/objects#listrecommendationsresult)

Fetch recommendations for a particular scenario

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listrecommendationsinput"><code>ListRecommendationsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listRoles

**Type:** [ListRolesResult](/docs/graphql/objects#listrolesresult)

List all DataHub Roles

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listrolesinput"><code>ListRolesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listSecrets

**Type:** [ListSecretsResult](/docs/graphql/objects#listsecretsresult)

List all secrets stored in DataHub (no values)

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listsecretsinput"><code>ListSecretsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listTests

**Type:** [ListTestsResult](/docs/graphql/objects#listtestsresult)

List all DataHub Tests

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listtestsinput"><code>ListTestsInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## listUsers

**Type:** [ListUsersResult](/docs/graphql/objects#listusersresult)

List all DataHub Users

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#listusersinput"><code>ListUsersInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## me

**Type:** [AuthenticatedUser](/docs/graphql/objects#authenticateduser)

Fetch details associated with the authenticated user, provided via an auth cookie or header

## mlFeature

**Type:** [MLFeature](/docs/graphql/objects#mlfeature)

Incubating: Fetch a ML Feature by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## mlFeatureTable

**Type:** [MLFeatureTable](/docs/graphql/objects#mlfeaturetable)

Incubating: Fetch a ML Feature Table by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## mlModel

**Type:** [MLModel](/docs/graphql/objects#mlmodel)

Incubating: Fetch an ML Model by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## mlModelGroup

**Type:** [MLModelGroup](/docs/graphql/objects#mlmodelgroup)

Incubating: Fetch an ML Model Group by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## mlPrimaryKey

**Type:** [MLPrimaryKey](/docs/graphql/objects#mlprimarykey)

Incubating: Fetch a ML Primary Key by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## notebook

**Type:** [Notebook](/docs/graphql/objects#notebook)

Fetch a Notebook by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## scrollAcrossEntities

**Type:** [ScrollResults](/docs/graphql/objects#scrollresults)

Search DataHub entities by providing a pointer reference for scrolling through results.

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#scrollacrossentitiesinput"><code>ScrollAcrossEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## scrollAcrossLineage

**Type:** [ScrollAcrossLineageResults](/docs/graphql/objects#scrollacrosslineageresults)

Search across the results of a graph query on a node, uses scroll API

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#scrollacrosslineageinput"><code>ScrollAcrossLineageInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## search

**Type:** [SearchResults](/docs/graphql/objects#searchresults)

Full text search against a specific DataHub Entity Type

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#searchinput"><code>SearchInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## searchAcrossEntities

**Type:** [SearchResults](/docs/graphql/objects#searchresults)

Search DataHub entities

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#searchacrossentitiesinput"><code>SearchAcrossEntitiesInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## searchAcrossLineage

**Type:** [SearchAcrossLineageResults](/docs/graphql/objects#searchacrosslineageresults)

Search across the results of a graph query on a node

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#searchacrosslineageinput"><code>SearchAcrossLineageInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## tag

**Type:** [Tag](/docs/graphql/objects#tag)

Fetch a Tag by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## test

**Type:** [Test](/docs/graphql/objects#test)

Fetch a Test by primary key (urn)

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

</td>
</tr>
</tbody>
</table>

## versionedDataset

**Type:** [VersionedDataset](/docs/graphql/objects#versioneddataset)

Fetch a Dataset by primary key (urn) at a point in time based on aspect versions (versionStamp)

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

</td>
</tr>
<tr>
<td>
versionStamp<br />
<a href="/docs/graphql/scalars#string"><code>String</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>
