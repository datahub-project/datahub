---
id: queries
title: Queries
slug: queries
sidebar_position: 1
---

## appConfig

**Type:** [AppConfig](/docs/graphql/objects#appconfig)

Fetch configurations
Used by DataHub UI

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

