# Creating Terms

## Why Would You Create Terms?

The Business Glossary(Term) feature in DataHub helps you use a shared vocabulary within the orgarnization, by providing a framework for defining a standardized set of data concepts and then associating them with the physical assets that exist within your data ecosystem.

For more information about terms, refer to [About DataHub Business Glossary](/docs/glossary/business-glossary.md).

### Goal Of This Guide

This guide will show you how to create a term named `Rate of Return`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Terms With GraphQL

:::note
Please note that there are two available endpoints (`:8000`, `:9002`) to access `graphql`.
For more information about the differences between these endpoints, please refer to [DataHub Metadata Service](../../../metadata-service/README.md#graphql-api)
:::

### GraphQL Explorer

GraphQL Explorer is the fastest way to experiment with `graphql` without any dependancies.
Navigate to GraphQL Explorer (`http://localhost:9002/api/graphiql`) and run the following query.

```python
mutation createGlossaryTerm {
    createGlossaryTerm(input:
    {
      name: "Rate of Return",
      description: "A rate of return (RoR) is the net gain or loss of an investment over a specified time period."
    })
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "createGlossaryTerm": "<term_urn>"
  },
  "extensions": {}
}
```

### CURL

With CURL, you need to provide tokens. To generate a token, please refer to [Access Token Management](/docs/api/graphql/token-management.md).
With `accessToken`, you can run the following command.

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation createGlossaryTerm { createGlossaryTerm(input: { name: \"Rate of Return\", description: \"A rate of return (RoR) is the net gain or loss of an investment over a specified time period.\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "createGlossaryTerm": "<term_urn>" }, "extensions": {} }
```

## Create Terms With Python SDK

The following code creates a term named `Rate of Return`.
You can refer to the full code in [create_term.py](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/create_term.py).

```python
import logging

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import GlossaryTermInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

term_urn = make_term_urn("rateofreturn")
term_properties_aspect = GlossaryTermInfoClass(
    definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
    name="Rate of Return",
    termSource="",
)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=term_urn,
    aspect=term_properties_aspect,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Created term {term_urn}")
```

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)

## Expected Outcomes

You can now see `Rate of Return` term has been created.
To view the definition, you can either click on 'Govern > Glossary' at the top right of the page or simply search for the term by name.

![term-created](../../imgs/apis/tutorials/term-created.png)

## What's Next?

Now that you created a term, how about adding it to a dataset? Here's a guide on [how to add a term on a dataset](/docs/api/tutorials/adding-terms.md).
