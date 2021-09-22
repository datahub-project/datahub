---
id: mutations
title: Mutations
slug: mutations
sidebar_position: 2
---

## addTag

**Type:** [Boolean](/docs/graphql/scalars#boolean)

Add a tag to a particular Entity or subresource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#tagassociationinput"><code>TagAssociationInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## addTerm

**Type:** [Boolean](/docs/graphql/scalars#boolean)

Add a glossary term to a particular Entity or subresource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#termassociationinput"><code>TermAssociationInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## createPolicy

**Type:** [String](/docs/graphql/scalars#string)

Create a policy and returns the resulting urn

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#policyupdateinput"><code>PolicyUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## deletePolicy

**Type:** [String](/docs/graphql/scalars#string)

Remove an existing policy and returns the policy urn

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

## removeTag

**Type:** [Boolean](/docs/graphql/scalars#boolean)

Remove a tag from a particular Entity or subresource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#tagassociationinput"><code>TagAssociationInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## removeTerm

**Type:** [Boolean](/docs/graphql/scalars#boolean)

Remove a glossary term from a particular Entity or subresource

<p style={{ marginBottom: "0.4em" }}><strong>Arguments</strong></p>

<table>
<thead><tr><th>Name</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>
input<br />
<a href="/docs/graphql/inputObjects#termassociationinput"><code>TermAssociationInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateChart

**Type:** [Chart](/docs/graphql/objects#chart)

Update the metadata about a particular Chart

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
<a href="/docs/graphql/inputObjects#chartupdateinput"><code>ChartUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateDashboard

**Type:** [Dashboard](/docs/graphql/objects#dashboard)

Update the metadata about a particular Dashboard

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
<a href="/docs/graphql/inputObjects#dashboardupdateinput"><code>DashboardUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateDataFlow

**Type:** [DataFlow](/docs/graphql/objects#dataflow)

Update the metadata about a particular Data Flow (Pipeline)

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
<a href="/docs/graphql/inputObjects#dataflowupdateinput"><code>DataFlowUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateDataJob

**Type:** [DataJob](/docs/graphql/objects#datajob)

Update the metadata about a particular Data Job (Task)

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
<a href="/docs/graphql/inputObjects#datajobupdateinput"><code>DataJobUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateDataset

**Type:** [Dataset](/docs/graphql/objects#dataset)

Update the metadata about a particular Dataset

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
<a href="/docs/graphql/inputObjects#datasetupdateinput"><code>DatasetUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updatePolicy

**Type:** [String](/docs/graphql/scalars#string)

Update an existing policy and returns the resulting urn

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
<a href="/docs/graphql/inputObjects#policyupdateinput"><code>PolicyUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

## updateTag

**Type:** [Tag](/docs/graphql/objects#tag)

Update the information about a particular Entity Tag

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
<a href="/docs/graphql/inputObjects#tagupdateinput"><code>TagUpdateInput!</code></a>
</td>
<td>

</td>
</tr>
</tbody>
</table>

