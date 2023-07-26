---
sidebar_position: 4
title: Business Glossary
slug: /generated/ingestion/sources/business-glossary
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/business-glossary.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Business Glossary

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

This plugin pulls business glossary metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/business_glossary.yml).

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[datahub-business-glossary]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: datahub-business-glossary
  config:
    # Coordinates
    file: /path/to/business_glossary_yaml
    enable_auto_id: true # recommended to set to true so datahub will auto-generate guids from your term names

# sink configs if needed
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                               | Description                                                                                                                                                                                         |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">One of string, string(path)</span></div> | File path or URL to business glossary file to ingest.                                                                                                                                               |
| <div className="path-line"><span className="path-main">enable_auto_id</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Generate guid urns instead of a plaintext path urn with the node/term's hierarchy. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "BusinessGlossarySourceConfig",
  "type": "object",
  "properties": {
    "file": {
      "title": "File",
      "description": "File path or URL to business glossary file to ingest.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "string",
          "format": "path"
        }
      ]
    },
    "enable_auto_id": {
      "title": "Enable Auto Id",
      "description": "Generate guid urns instead of a plaintext path urn with the node/term's hierarchy.",
      "default": false,
      "type": "boolean"
    }
  },
  "required": [
    "file"
  ],
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

### Business Glossary File Format

The business glossary source file should be a .yml file with the following top-level keys:

**Glossary**: the top level keys of the business glossary file

Example **Glossary**:

```yaml
version: 1                                     			# the version of business glossary file config the config conforms to. Currently the only version released is `1`.
source: DataHub                                			# the source format of the terms. Currently only supports `DataHub`
owners:                                        			# owners contains two nested fields
  users:                                       		    # (optional) a list of user IDs
    - njones
  groups:                                               # (optional) a list of group IDs
    - logistics
url: "https://github.com/datahub-project/datahub/"      # (optional) external url pointing to where the glossary is defined externally, if applicable
nodes:                                                  # list of child **GlossaryNode** objects. See **GlossaryNode** section below
	...
```

**GlossaryNode**: a container of **GlossaryNode** and **GlossaryTerm** objects

Example **GlossaryNode**:

```yaml
- name: Shipping # name of the node
  description: Provides terms related to the shipping domain # description of the node
  owners: # (optional) owners contains 2 nested fields
    users: # (optional) a list of user IDs
      - njones
    groups: # (optional) a  list of group IDs
      - logistics
  nodes: # list of child **GlossaryNode** objects
    ...
  knowledge_links: # (optional) list of **KnowledgeCard** objects
    - label: Wiki link for shipping
      url: "https://en.wikipedia.org/wiki/Freight_transport"
```

**GlossaryTerm**: a term in your business glossary

Example **GlossaryTerm**:

```yaml
- name: FullAddress # name of the term
  description: A collection of information to give the location of a building or plot of land. # description of the term
  owners: # (optional) owners contains 2 nested fields
    users: # (optional) a list of user IDs
      - njones
    groups: # (optional) a  list of group IDs
      - logistics
  term_source: "EXTERNAL" # one of `EXTERNAL` or `INTERNAL`. Whether the term is coming from an external glossary or one defined in your organization.
  source_ref: FIBO # (optional) if external, what is the name of the source the glossary term is coming from?
  source_url: "https://www.google.com" # (optional) if external, what is the url of the source definition?
  inherits: # (optional) list of **GlossaryTerm** that this term inherits from
    - Privacy.PII
  contains: # (optional) a list of **GlossaryTerm** that this term contains
    - Shipping.ZipCode
    - Shipping.CountryCode
    - Shipping.StreetAddress
  custom_properties: # (optional) a map of key/value pairs of arbitrary custom properties
    - is_used_for_compliance_tracking: true
  knowledge_links: # (optional) a list of **KnowledgeCard** related to this term. These appear as links on the glossary node's page
    - url: "https://en.wikipedia.org/wiki/Address"
      label: Wiki link
  domain: "urn:li:domain:Logistics" # (optional) domain name or domain urn
```

To see how these all work together, check out this comprehensive example business glossary file below:

<details>
<summary>Example business glossary file</summary>

```yaml
version: 1
source: DataHub
owners:
  users:
    - mjames
url: "https://github.com/datahub-project/datahub/"
nodes:
  - name: Classification
    description: A set of terms related to Data Classification
    knowledge_links:
      - label: Wiki link for classification
        url: "https://en.wikipedia.org/wiki/Classification"
    terms:
      - name: Sensitive
        description: Sensitive Data
        custom_properties:
          is_confidential: false
      - name: Confidential
        description: Confidential Data
        custom_properties:
          is_confidential: true
      - name: HighlyConfidential
        description: Highly Confidential Data
        custom_properties:
          is_confidential: true
        domain: Marketing
  - name: PersonalInformation
    description: All terms related to personal information
    owners:
      users:
        - mjames
    terms:
      - name: Email
        ## An example of using an id to pin a term to a specific guid
        ## See "how to generate custom IDs for your terms" section below
        # id: "urn:li:glossaryTerm:41516e310acbfd9076fffc2c98d2d1a3"
        description: An individual's email address
        inherits:
          - Classification.Confidential
        owners:
          groups:
            - Trust and Safety
      - name: Address
        description: A physical address
      - name: Gender
        description: The gender identity of the individual
        inherits:
          - Classification.Sensitive
  - name: Shipping
    description: Provides terms related to the shipping domain
    owners:
      users:
        - njones
      groups:
        - logistics
    terms:
      - name: FullAddress
        description: A collection of information to give the location of a building or plot of land.
        owners:
          users:
            - njones
          groups:
            - logistics
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://www.google.com"
        inherits:
          - Privacy.PII
        contains:
          - Shipping.ZipCode
          - Shipping.CountryCode
          - Shipping.StreetAddress
        related_terms:
          - Housing.Kitchen.Cutlery
        custom_properties:
          - is_used_for_compliance_tracking: true
        knowledge_links:
          - url: "https://en.wikipedia.org/wiki/Address"
            label: Wiki link
        domain: "urn:li:domain:Logistics"
    knowledge_links:
      - label: Wiki link for shipping
        url: "https://en.wikipedia.org/wiki/Freight_transport"
  - name: ClientsAndAccounts
    description: Provides basic concepts such as account, account holder, account provider, relationship manager that are commonly used by financial services providers to describe customers and to determine counterparty identities
    owners:
      groups:
        - finance
    terms:
      - name: Account
        description: Container for records associated with a business arrangement for regular transactions and services
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"
        inherits:
          - Classification.HighlyConfidential
        contains:
          - ClientsAndAccounts.Balance
      - name: Balance
        description: Amount of money available or owed
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Balance"
  - name: Housing
    description: Provides terms related to the housing domain
    owners:
      users:
        - mjames
      groups:
        - interior
    nodes:
      - name: Colors
        description: "Colors that are used in Housing construction"
        terms:
          - name: Red
            description: "red color"
            term_source: "EXTERNAL"
            source_ref: FIBO
            source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"

          - name: Green
            description: "green color"
            term_source: "EXTERNAL"
            source_ref: FIBO
            source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"

          - name: Pink
            description: pink color
            term_source: "EXTERNAL"
            source_ref: FIBO
            source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"
    terms:
      - name: WindowColor
        description: Supported window colors
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"
        values:
          - Housing.Colors.Red
          - Housing.Colors.Pink

      - name: Kitchen
        description: a room or area where food is prepared and cooked.
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"

      - name: Spoon
        description: an implement consisting of a small, shallow oval or round bowl on a long handle, used for eating, stirring, and serving food.
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"
        related_terms:
          - Housing.Kitchen
        knowledge_links:
          - url: "https://en.wikipedia.org/wiki/Spoon"
            label: Wiki link
```

</details>

Source file linked [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/business_glossary.yml).

## Generating custom IDs for your terms

IDs are normally inferred from the glossary term/node's name, see the `enable_auto_id` config. But, if you need a stable
identifier, you can generate a custom ID for your term. It should be unique across the entire Glossary.

Here's an example ID:
`id: "urn:li:glossaryTerm:41516e310acbfd9076fffc2c98d2d1a3"`

A note of caution: once you select a custom ID, it cannot be easily changed.

## Compatibility

Compatible with version 1 of business glossary format.
The source will be evolved as we publish newer versions of this format.

### Code Coordinates

- Class Name: `datahub.ingestion.source.metadata.business_glossary.BusinessGlossaryFileSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/metadata/business_glossary.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Business Glossary, feel free to ping us on [our Slack](https://slack.datahubproject.io).
