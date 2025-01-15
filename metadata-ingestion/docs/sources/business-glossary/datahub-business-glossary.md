### Business Glossary File Format

The business glossary source file should be a .yml file with the following top-level keys:

**Glossary**: the top level keys of the business glossary file

Example **Glossary**:

```yaml
version: "1"                                     			# the version of business glossary file config the config conforms to. Currently the only version released is `1`.
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
- name: Shipping                                                # name of the node
  description: Provides terms related to the shipping domain    # description of the node
  owners:                                                       # (optional) owners contains 2 nested fields
    users:                                                      # (optional) a list of user IDs
      - njones
    groups:                                                     # (optional) a  list of group IDs
      - logistics
  nodes:                                                        # list of child **GlossaryNode** objects
    ...
  knowledge_links:                                              # (optional) list of **KnowledgeCard** objects
    - label: Wiki link for shipping
      url: "https://en.wikipedia.org/wiki/Freight_transport"
```

**GlossaryTerm**: a term in your business glossary

Example **GlossaryTerm**:

```yaml
- name: FullAddress                                                          # name of the term
  description: A collection of information to give the location of a building or plot of land.    # description of the term
  owners:                                                                   # (optional) owners contains 2 nested fields
    users:                                                                  # (optional) a list of user IDs
      - njones
    groups:                                                                 # (optional) a  list of group IDs
      - logistics
  term_source: "EXTERNAL"                                                   # one of `EXTERNAL` or `INTERNAL`. Whether the term is coming from an external glossary or one defined in your organization.
  source_ref: FIBO                                                          # (optional) if external, what is the name of the source the glossary term is coming from?
  source_url: "https://www.google.com"                                      # (optional) if external, what is the url of the source definition?
  inherits:                                                                 # (optional) list of **GlossaryTerm** that this term inherits from
    -  Privacy.PII
  contains:                                                                 # (optional) a list of **GlossaryTerm** that this term contains
    - Shipping.ZipCode
    - Shipping.CountryCode
    - Shipping.StreetAddress
  custom_properties:                                                        # (optional) a map of key/value pairs of arbitrary custom properties
    - is_used_for_compliance_tracking: "true"
  knowledge_links:                                                          # (optional) a list of **KnowledgeCard** related to this term. These appear as links on the glossary node's page
    - url: "https://en.wikipedia.org/wiki/Address"
      label: Wiki link
  domain: "urn:li:domain:Logistics"                                            # (optional) domain name or domain urn
```

To see how these all work together, check out this comprehensive example business glossary file below:

<details>
<summary>Example business glossary file</summary>

```yaml
version: "1"
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
          is_confidential: "false"
      - name: Confidential
        description: Confidential Data
        custom_properties:
          is_confidential: "true"
      - name: HighlyConfidential
        description: Highly Confidential Data
        custom_properties:
          is_confidential: "true"
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
          - is_used_for_compliance_tracking: "true"
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
