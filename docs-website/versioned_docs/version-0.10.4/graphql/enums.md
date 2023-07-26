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

## AccessTokenDuration

The duration for which an Access Token is valid.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ONE_HOUR</td>
<td>
<p>1 hour</p>
</td>
</tr>
<tr>
<td>ONE_DAY</td>
<td>
<p>1 day</p>
</td>
</tr>
<tr>
<td>ONE_WEEK</td>
<td>
<p>1 week</p>
</td>
</tr>
<tr>
<td>ONE_MONTH</td>
<td>
<p>1 month</p>
</td>
</tr>
<tr>
<td>THREE_MONTHS</td>
<td>
<p>3 months</p>
</td>
</tr>
<tr>
<td>SIX_MONTHS</td>
<td>
<p>6 months</p>
</td>
</tr>
<tr>
<td>ONE_YEAR</td>
<td>
<p>1 year</p>
</td>
</tr>
<tr>
<td>NO_EXPIRY</td>
<td>
<p>No expiry</p>
</td>
</tr>
</tbody>
</table>

## AccessTokenType

A type of DataHub Access Token.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>PERSONAL</td>
<td>
<p>Generates a personal access token</p>
</td>
</tr>
</tbody>
</table>

## AssertionResultType

The result type of an assertion, success or failure.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SUCCESS</td>
<td>
<p>The assertion succeeded.</p>
</td>
</tr>
<tr>
<td>FAILURE</td>
<td>
<p>The assertion failed.</p>
</td>
</tr>
</tbody>
</table>

## AssertionRunStatus

The state of an assertion run, as defined within an Assertion Run Event.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>COMPLETE</td>
<td>
<p>An assertion run has completed.</p>
</td>
</tr>
</tbody>
</table>

## AssertionStdAggregation

An "aggregation" function that can be applied to column values of a Dataset to create the input to an Assertion Operator.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>IDENTITY</td>
<td>
<p>Assertion is applied on individual column value</p>
</td>
</tr>
<tr>
<td>MEAN</td>
<td>
<p>Assertion is applied on column mean</p>
</td>
</tr>
<tr>
<td>MEDIAN</td>
<td>
<p>Assertion is applied on column median</p>
</td>
</tr>
<tr>
<td>UNIQUE_COUNT</td>
<td>
<p>Assertion is applied on number of distinct values in column</p>
</td>
</tr>
<tr>
<td>UNIQUE_PROPOTION</td>
<td>
<p>Assertion is applied on proportion of distinct values in column</p>
</td>
</tr>
<tr>
<td>NULL_COUNT</td>
<td>
<p>Assertion is applied on number of null values in column</p>
</td>
</tr>
<tr>
<td>NULL_PROPORTION</td>
<td>
<p>Assertion is applied on proportion of null values in column</p>
</td>
</tr>
<tr>
<td>STDDEV</td>
<td>
<p>Assertion is applied on column std deviation</p>
</td>
</tr>
<tr>
<td>MIN</td>
<td>
<p>Assertion is applied on column min</p>
</td>
</tr>
<tr>
<td>MAX</td>
<td>
<p>Assertion is applied on column std deviation</p>
</td>
</tr>
<tr>
<td>SUM</td>
<td>
<p>Assertion is applied on column sum</p>
</td>
</tr>
<tr>
<td>COLUMNS</td>
<td>
<p>Assertion is applied on all columns</p>
</td>
</tr>
<tr>
<td>COLUMN_COUNT</td>
<td>
<p>Assertion is applied on number of columns</p>
</td>
</tr>
<tr>
<td>ROW_COUNT</td>
<td>
<p>Assertion is applied on number of rows</p>
</td>
</tr>
<tr>
<td>_NATIVE_</td>
<td>
<p>Other</p>
</td>
</tr>
</tbody>
</table>

## AssertionStdOperator

A standard operator or condition that constitutes an assertion definition

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>BETWEEN</td>
<td>
<p>Value being asserted is between min_value and max_value</p>
</td>
</tr>
<tr>
<td>LESS_THAN</td>
<td>
<p>Value being asserted is less than max_value</p>
</td>
</tr>
<tr>
<td>LESS_THAN_OR_EQUAL_TO</td>
<td>
<p>Value being asserted is less than or equal to max_value</p>
</td>
</tr>
<tr>
<td>GREATER_THAN</td>
<td>
<p>Value being asserted is greater than min_value</p>
</td>
</tr>
<tr>
<td>GREATER_THAN_OR_EQUAL_TO</td>
<td>
<p>Value being asserted is greater than or equal to min_value</p>
</td>
</tr>
<tr>
<td>EQUAL_TO</td>
<td>
<p>Value being asserted is equal to value</p>
</td>
</tr>
<tr>
<td>NOT_NULL</td>
<td>
<p>Value being asserted is not null</p>
</td>
</tr>
<tr>
<td>CONTAIN</td>
<td>
<p>Value being asserted contains value</p>
</td>
</tr>
<tr>
<td>END_WITH</td>
<td>
<p>Value being asserted ends with value</p>
</td>
</tr>
<tr>
<td>START_WITH</td>
<td>
<p>Value being asserted starts with value</p>
</td>
</tr>
<tr>
<td>REGEX_MATCH</td>
<td>
<p>Value being asserted matches the regex value.</p>
</td>
</tr>
<tr>
<td>IN</td>
<td>
<p>Value being asserted is one of the array values</p>
</td>
</tr>
<tr>
<td>NOT_IN</td>
<td>
<p>Value being asserted is not in one of the array values.</p>
</td>
</tr>
<tr>
<td>_NATIVE_</td>
<td>
<p>Other</p>
</td>
</tr>
</tbody>
</table>

## AssertionStdParameterType

The type of an AssertionStdParameter

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>STRING</td>
<td>

</td>
</tr>
<tr>
<td>NUMBER</td>
<td>

</td>
</tr>
<tr>
<td>LIST</td>
<td>

</td>
</tr>
<tr>
<td>SET</td>
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

## AssertionType

The top-level assertion type. Currently single Dataset assertions are the only type supported.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DATASET</td>
<td>

</td>
</tr>
</tbody>
</table>

## ChangeCategoryType

Enum of CategoryTypes

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DOCUMENTATION</td>
<td>
<p>When documentation has been edited</p>
</td>
</tr>
<tr>
<td>GLOSSARY_TERM</td>
<td>
<p>When glossary terms have been added or removed</p>
</td>
</tr>
<tr>
<td>OWNERSHIP</td>
<td>
<p>When ownership has been modified</p>
</td>
</tr>
<tr>
<td>TECHNICAL_SCHEMA</td>
<td>
<p>When technical schemas have been added or removed</p>
</td>
</tr>
<tr>
<td>TAG</td>
<td>
<p>When tags have been added or removed</p>
</td>
</tr>
</tbody>
</table>

## ChangeOperationType

Enum of types of changes

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ADD</td>
<td>
<p>When an element is added</p>
</td>
</tr>
<tr>
<td>MODIFY</td>
<td>
<p>When an element is modified</p>
</td>
</tr>
<tr>
<td>REMOVE</td>
<td>
<p>When an element is removed</p>
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
<tr>
<td>WORD_CLOUD</td>
<td>
<p>A word cloud chart</p>
</td>
</tr>
<tr>
<td>COHORT</td>
<td>
<p>A Cohort Analysis chart</p>
</td>
</tr>
</tbody>
</table>

## CorpUserStatus

The state of a CorpUser

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ACTIVE</td>
<td>
<p>A User that has been provisioned and logged in</p>
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

## DataHubViewType

The type of a DataHub View

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>PERSONAL</td>
<td>
<p>A personal view - e.g. saved filters</p>
</td>
</tr>
<tr>
<td>GLOBAL</td>
<td>
<p>A global view, e.g. role view</p>
</td>
</tr>
</tbody>
</table>

## DataProcessInstanceRunResultType

The result of the data process run

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SUCCESS</td>
<td>
<p>The run finished successfully</p>
</td>
</tr>
<tr>
<td>FAILURE</td>
<td>
<p>The run finished in failure</p>
</td>
</tr>
<tr>
<td>SKIPPED</td>
<td>
<p>The run was skipped</p>
</td>
</tr>
<tr>
<td>UP_FOR_RETRY</td>
<td>
<p>The run failed and is up for retry</p>
</td>
</tr>
</tbody>
</table>

## DataProcessRunStatus

The status of the data process instance

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>STARTED</td>
<td>
<p>The data process instance has started but not completed</p>
</td>
</tr>
<tr>
<td>COMPLETE</td>
<td>
<p>The data process instance has completed</p>
</td>
</tr>
</tbody>
</table>

## DatasetAssertionScope

The scope that a Dataset-level assertion applies to.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DATASET_COLUMN</td>
<td>
<p>Assertion applies to columns of a dataset.</p>
</td>
</tr>
<tr>
<td>DATASET_ROWS</td>
<td>
<p>Assertion applies to rows of a dataset.</p>
</td>
</tr>
<tr>
<td>DATASET_SCHEMA</td>
<td>
<p>Assertion applies to schema of a dataset.</p>
</td>
</tr>
<tr>
<td>UNKNOWN</td>
<td>
<p>The scope of an assertion is unknown.</p>
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

## DateInterval

For consumption by UI only

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SECOND</td>
<td>

</td>
</tr>
<tr>
<td>MINUTE</td>
<td>

</td>
</tr>
<tr>
<td>HOUR</td>
<td>

</td>
</tr>
<tr>
<td>DAY</td>
<td>

</td>
</tr>
<tr>
<td>WEEK</td>
<td>

</td>
</tr>
<tr>
<td>MONTH</td>
<td>

</td>
</tr>
<tr>
<td>YEAR</td>
<td>

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
<td>DOMAIN</td>
<td>
<p>A Domain containing Metadata Entities</p>
</td>
</tr>
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
<td>NOTEBOOK</td>
<td>
<p>The Notebook Entity</p>
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
<td>GLOSSARY_NODE</td>
<td>
<p>The Glossary Node Entity</p>
</td>
</tr>
<tr>
<td>CONTAINER</td>
<td>
<p>A container of Metadata Entities</p>
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
<tr>
<td>INGESTION_SOURCE</td>
<td>
<p>A DataHub Managed Ingestion Source</p>
</td>
</tr>
<tr>
<td>EXECUTION_REQUEST</td>
<td>
<p>A DataHub ExecutionRequest</p>
</td>
</tr>
<tr>
<td>ASSERTION</td>
<td>
<p>A DataHub Assertion</p>
</td>
</tr>
<tr>
<td>DATA_PROCESS_INSTANCE</td>
<td>
<p>An instance of an individual run of a data job or data flow</p>
</td>
</tr>
<tr>
<td>DATA_PLATFORM_INSTANCE</td>
<td>
<p>Data Platform Instance Entity</p>
</td>
</tr>
<tr>
<td>ACCESS_TOKEN</td>
<td>
<p>A DataHub Access Token</p>
</td>
</tr>
<tr>
<td>TEST</td>
<td>
<p>A DataHub Test</p>
</td>
</tr>
<tr>
<td>DATAHUB_POLICY</td>
<td>
<p>A DataHub Policy</p>
</td>
</tr>
<tr>
<td>DATAHUB_ROLE</td>
<td>
<p>A DataHub Role</p>
</td>
</tr>
<tr>
<td>POST</td>
<td>
<p>A DataHub Post</p>
</td>
</tr>
<tr>
<td>SCHEMA_FIELD</td>
<td>
<p>A Schema Field</p>
</td>
</tr>
<tr>
<td>DATAHUB_VIEW</td>
<td>
<p>A DataHub View</p>
</td>
</tr>
<tr>
<td>QUERY</td>
<td>
<p>A dataset query</p>
</td>
</tr>
<tr>
<td>DATA_PRODUCT</td>
<td>
<p>A Data Product</p>
</td>
</tr>
<tr>
<td>CUSTOM_OWNERSHIP_TYPE</td>
<td>
<p>A Custom Ownership Type</p>
</td>
</tr>
<tr>
<td>ROLE</td>
<td>
<p>&quot;
A Role from an organisation</p>
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
<td>TEST</td>
<td>
<p>Designates testing fabrics</p>
</td>
</tr>
<tr>
<td>QA</td>
<td>
<p>Designates quality assurance fabrics</p>
</td>
</tr>
<tr>
<td>UAT</td>
<td>
<p>Designates user acceptance testing fabrics</p>
</td>
</tr>
<tr>
<td>EI</td>
<td>
<p>Designates early integration fabrics</p>
</td>
</tr>
<tr>
<td>PRE</td>
<td>
<p>Designates pre-production fabrics</p>
</td>
</tr>
<tr>
<td>STG</td>
<td>
<p>Designates staging fabrics</p>
</td>
</tr>
<tr>
<td>NON_PROD</td>
<td>
<p>Designates non-production fabrics</p>
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

## FilterOperator

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>CONTAIN</td>
<td>
<p>Represent the relation: String field contains value, e.g. name contains Profile</p>
</td>
</tr>
<tr>
<td>EQUAL</td>
<td>
<p>Represent the relation: field = value, e.g. platform = hdfs</p>
</td>
</tr>
<tr>
<td>IN</td>
<td>
<ul>
<li>Represent the relation: String field is one of the array values to, e.g. name in [&quot;Profile&quot;, &quot;Event&quot;]</li>
</ul>
</td>
</tr>
</tbody>
</table>

## HealthStatus

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>PASS</td>
<td>
<p>The Asset is in a healthy state</p>
</td>
</tr>
<tr>
<td>WARN</td>
<td>
<p>The Asset is in a warning state</p>
</td>
</tr>
<tr>
<td>FAIL</td>
<td>
<p>The Asset is in a failing (unhealthy) state</p>
</td>
</tr>
</tbody>
</table>

## HealthStatusType

The type of the health status

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ASSERTIONS</td>
<td>
<p>Assertions status</p>
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

## LineageDirection

Direction between two nodes in the lineage graph

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>UPSTREAM</td>
<td>
<p>Upstream, or left-to-right in the lineage visualization</p>
</td>
</tr>
<tr>
<td>DOWNSTREAM</td>
<td>
<p>Downstream, or right-to-left in the lineage visualization</p>
</td>
</tr>
</tbody>
</table>

## LogicalOperator

A Logical Operator, AND or OR.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>AND</td>
<td>
<p>An AND operator.</p>
</td>
</tr>
<tr>
<td>OR</td>
<td>
<p>An OR operator.</p>
</td>
</tr>
</tbody>
</table>

## MediaType

The type of media

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>IMAGE</td>
<td>
<p>An image</p>
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

## NotebookCellType

The type for a NotebookCell

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>TEXT_CELL</td>
<td>
<p>TEXT Notebook cell type. The cell context is text only.</p>
</td>
</tr>
<tr>
<td>QUERY_CELL</td>
<td>
<p>QUERY Notebook cell type. The cell context is query only.</p>
</td>
</tr>
<tr>
<td>CHART_CELL</td>
<td>
<p>CHART Notebook cell type. The cell content is chart only.</p>
</td>
</tr>
</tbody>
</table>

## OperationSourceType

Enum to define the source/reporter type for an Operation.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>DATA_PROCESS</td>
<td>
<p>A data process reported the operation.</p>
</td>
</tr>
<tr>
<td>DATA_PLATFORM</td>
<td>
<p>A data platform reported the operation.</p>
</td>
</tr>
</tbody>
</table>

## OperationType

Enum to define the operation type when an entity changes.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>INSERT</td>
<td>
<p>When data is inserted.</p>
</td>
</tr>
<tr>
<td>UPDATE</td>
<td>
<p>When data is updated.</p>
</td>
</tr>
<tr>
<td>DELETE</td>
<td>
<p>When data is deleted.</p>
</td>
</tr>
<tr>
<td>CREATE</td>
<td>
<p>When table is created.</p>
</td>
</tr>
<tr>
<td>ALTER</td>
<td>
<p>When table is altered</p>
</td>
</tr>
<tr>
<td>DROP</td>
<td>
<p>When table is dropped</p>
</td>
</tr>
<tr>
<td>UNKNOWN</td>
<td>
<p>Unknown operation</p>
</td>
</tr>
<tr>
<td>CUSTOM</td>
<td>
<p>Custom</p>
</td>
</tr>
</tbody>
</table>

## OriginType

Enum to define where an entity originated from.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>NATIVE</td>
<td>
<p>The entity is native to DataHub.</p>
</td>
</tr>
<tr>
<td>EXTERNAL</td>
<td>
<p>The entity is external to DataHub.</p>
</td>
</tr>
<tr>
<td>UNKNOWN</td>
<td>
<p>The entity is of unknown origin.</p>
</td>
</tr>
</tbody>
</table>

## OwnerEntityType

Entities that are able to own other entities

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>CORP_USER</td>
<td>
<p>A corp user owner</p>
</td>
</tr>
<tr>
<td>CORP_GROUP</td>
<td>
<p>A corp group owner</p>
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
<td>TECHNICAL_OWNER</td>
<td>
<p>A person or group who is responsible for technical aspects of the asset.</p>
</td>
</tr>
<tr>
<td>BUSINESS_OWNER</td>
<td>
<p>A person or group who is responsible for logical, or business related, aspects of the asset.</p>
</td>
</tr>
<tr>
<td>DATA_STEWARD</td>
<td>
<p>A steward, expert, or delegate responsible for the asset.</p>
</td>
</tr>
<tr>
<td>NONE</td>
<td>
<p>No specific type associated with the owner.</p>
</td>
</tr>
<tr>
<td>CUSTOM</td>
<td>
<p>Associated ownership type is a custom ownership type. Please check OwnershipTypeEntity urn for custom value.</p>
</td>
</tr>
<tr>
<td>DATAOWNER</td>
<td>
<p>A person or group that owns the data.
Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.</p>
</td>
</tr>
<tr>
<td>DEVELOPER</td>
<td>
<p>A person or group that is in charge of developing the code
Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.</p>
</td>
</tr>
<tr>
<td>DELEGATE</td>
<td>
<p>A person or a group that overseas the operation, eg a DBA or SRE
Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.</p>
</td>
</tr>
<tr>
<td>PRODUCER</td>
<td>
<p>A person, group, or service that produces or generates the data
Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.</p>
</td>
</tr>
<tr>
<td>STAKEHOLDER</td>
<td>
<p>A person or a group that has direct business interest
Deprecated! Use BUSINESS_OWNER instead.</p>
</td>
</tr>
<tr>
<td>CONSUMER</td>
<td>
<p>A person, group, or service that consumes the data
Deprecated! This ownership type is no longer supported.</p>
</td>
</tr>
</tbody>
</table>

## PartitionType

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>FULL_TABLE</td>
<td>

</td>
</tr>
<tr>
<td>QUERY</td>
<td>

</td>
</tr>
<tr>
<td>PARTITION</td>
<td>

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

## PolicyMatchCondition

Match condition

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>EQUALS</td>
<td>
<p>Whether the field matches the value</p>
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

## PostContentType

The type of post

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>TEXT</td>
<td>
<p>Text content</p>
</td>
</tr>
<tr>
<td>LINK</td>
<td>
<p>Link content</p>
</td>
</tr>
</tbody>
</table>

## PostType

The type of post

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>HOME_PAGE_ANNOUNCEMENT</td>
<td>
<p>Posts on the home page</p>
</td>
</tr>
</tbody>
</table>

## QueryLanguage

A query language / dialect.

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
</tbody>
</table>

## QuerySource

The source of the query

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>MANUAL</td>
<td>
<p>The query was provided manually, e.g. from the UI.</p>
</td>
</tr>
</tbody>
</table>

## RecommendationRenderType

Enum that defines how the modules should be rendered.
There should be two frontend implementation of large and small modules per type.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>ENTITY_NAME_LIST</td>
<td>
<p>Simple list of entities</p>
</td>
</tr>
<tr>
<td>PLATFORM_SEARCH_LIST</td>
<td>
<p>List of platforms</p>
</td>
</tr>
<tr>
<td>TAG_SEARCH_LIST</td>
<td>
<p>Tag search list</p>
</td>
</tr>
<tr>
<td>SEARCH_QUERY_LIST</td>
<td>
<p>A list of recommended search queries</p>
</td>
</tr>
<tr>
<td>GLOSSARY_TERM_SEARCH_LIST</td>
<td>
<p>Glossary Term search list</p>
</td>
</tr>
<tr>
<td>DOMAIN_SEARCH_LIST</td>
<td>
<p>Domain Search List</p>
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

## ScenarioType

Type of the scenario requesting recommendation

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>HOME</td>
<td>
<p>Recommendations to show on the users home page</p>
</td>
</tr>
<tr>
<td>SEARCH_RESULTS</td>
<td>
<p>Recommendations to show on the search results page</p>
</td>
</tr>
<tr>
<td>ENTITY_PROFILE</td>
<td>
<p>Recommendations to show on an Entity Profile page</p>
</td>
</tr>
<tr>
<td>SEARCH_BAR</td>
<td>
<p>Recommendations to show on the search bar when clicked</p>
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

## TermRelationshipType

A type of Metadata Entity sub resource

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>isA</td>
<td>
<p>When a Term inherits from, or has an &#39;Is A&#39; relationship with another Term</p>
</td>
</tr>
<tr>
<td>hasA</td>
<td>
<p>When a Term contains, or has a &#39;Has A&#39; relationship with another Term</p>
</td>
</tr>
</tbody>
</table>

## TestResultType

The result type of a test that has been run

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SUCCESS</td>
<td>
<p>The test succeeded.</p>
</td>
</tr>
<tr>
<td>FAILURE</td>
<td>
<p>The test failed.</p>
</td>
</tr>
</tbody>
</table>

## TimeRange

A time range used in fetching Usage statistics

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

## UserSetting

An individual setting type for a Corp User.

<p style={{ marginBottom: "0.4em" }}><strong>Values</strong></p>

<table>
<thead><tr><th>Value</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td>SHOW_SIMPLIFIED_HOMEPAGE</td>
<td>
<p>Show simplified homepage</p>
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
