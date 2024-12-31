import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# [Feature Name]


<!-- This section should provide plain-language explanations on the feature itself, preferably in one paragraph, and should link to the feature guide for more detailed information.
* What is the short definition of this feature?
* Why is this useful-->

### Goal of This Guide

This guide will show you how to...

<!-- This should include bullet points of the main goals(actions) for this tutorial, such as
- Add lineage between datasets
- Add column-level lineage between datasets
etc.
-->

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data. For detailed steps, please refer to [Datahub Quickstart Guide]<relative_link_to_quickstart_page>

<!-- If there's any specific setup required before starting the tutorial, list them here. 
For example, it can be installing a certain version of CLI, or some admonition like below.
:::NOTE
Before adding lineage, you need to ensure the targeted dataset is already present in your datahub. If you attempt to manipulate entities that do not exist, your operation will fail. In this guide, we will be using data from sample ingestion.
:::
-->

## [Action] [Feature Name]
<!-- The heading should short and straightforward, and start with verb and end with the feature name. For example, "Add Lineage" 
Each action should have the same format of code snippets (Tabs) and expected outcomes. -->

<!-- If there is any other explanations that needs to be made around this action, please include here -->

<Tab>

<TabItem value="GraphQL" label="GraphQL" default>
<!-- Provide graphQL code snippet here with explanations (optional). If this is not supported, delete this TabItem-->
</TabItem>

<TabItem value="Curl" label="Curl">
<!-- Provide Curl code snippet here with explanations (optional). You should be able to generate curl code snippet from graphql query, like this:
```bash
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json'  --data-raw '{ "query": <graphql_query> } })}", "variables":{}}'
```
If this is not supported, delete this TabItem-->
</TabItem>

<TabItem value="Python" label="Python">
<!-- Provide Python code snippet here with explanations (optional). Python snippet should be placed under /metadata-ingestion/examples/library/, and you should refer to it like:
`{{ inline <file_path> show_path_as_comment }}`
If this is not supported, delete this TabItem-->
</TabItem>

<TabItem value="CLI" label="CLI">
<!-- Provide graphQL code snippet here with explanations (optional). If this is not supported, delete this TabItem-->
</TabItem>

</Tab>


### Expected Outcome of [Action] [Feature Name]

<!-- This section should demonstrate the expected outcome of the action. 
In general, this should include the screenshot of the DataHub UI. 
But it can also be an output of the CLI command, etc. -->

