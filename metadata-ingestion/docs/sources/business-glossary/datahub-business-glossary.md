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
- name: "Shipping"                                              # name of the node
  id: "Shipping-Logistics"                                      # (optional) custom identifier for the node
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
- name: "Full Address"                                                         # name of the term
  id: "Full-Address-Details"                                                  # (optional) custom identifier for the term
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

## ID Management and URL Generation

The business glossary provides two primary ways to manage term and node identifiers:

1. **Custom IDs**: You can explicitly specify an ID for any term or node using the `id` field. This is recommended for terms that need stable, predictable identifiers:
   ```yaml
   terms:
     - name: "Response Time"
       id: "support-response-time"  # Explicit ID
       description: "Target time to respond to customer inquiries"
   ```

2. **Automatic ID Generation**: When no ID is specified, the system will generate one based on the `enable_auto_id` setting:
   - With `enable_auto_id: false` (default):
     - Node and term names are converted to URL-friendly format
     - Spaces within names are replaced with hyphens
     - Special characters are removed (except hyphens)
     - Case is preserved
     - Multiple hyphens are collapsed to single ones
     - Path components (node/term hierarchy) are joined with periods
     - Example: Node "Customer Support" with term "Response Time" → "Customer-Support.Response-Time"

   - With `enable_auto_id: true`:
     - Generates GUID-based IDs
     - Recommended for guaranteed uniqueness
     - Required for terms with non-ASCII characters

Here's how path-based ID generation works:
```yaml
nodes:
  - name: "Customer Support"          # Node ID: Customer-Support
    terms:
      - name: "Response Time"         # Term ID: Customer-Support.Response-Time
        description: "Response SLA"
      
      - name: "First Reply"          # Term ID: Customer-Support.First-Reply
        description: "Initial response"

  - name: "Product Feedback"         # Node ID: Product-Feedback
    terms:
      - name: "Response Time"        # Term ID: Product-Feedback.Response-Time
        description: "Feedback response"
```

**Important Notes**:
- Periods (.) are used exclusively as path separators between nodes and terms
- Periods in term or node names themselves will be removed
- Each component of the path (node names, term names) is cleaned independently:
  - Spaces to hyphens
  - Special characters removed
  - Case preserved
- The cleaned components are then joined with periods to form the full path
- Non-ASCII characters in any component trigger automatic GUID generation
- Once an ID is created (either manually or automatically), it cannot be easily changed
- All references to a term (in `inherits`, `contains`, etc.) must use its correct ID
- Moving terms in the hierarchy does NOT update their IDs:
  - The ID retains its original path components even after moving
  - This can lead to IDs that don't match the current location
  - Consider using `enable_auto_id: true` if you plan to reorganize your glossary
- For terms that other terms will reference, consider using explicit IDs or enable auto_id

Example of how different names are handled:
```yaml
nodes:
  - name: "Data Services"           # Node ID: Data-Services
    terms:
      # Basic term name
      - name: "Response Time"       # Term ID: Data-Services.Response-Time
        description: "SLA metrics"
      
      # Term name with special characters
      - name: "API @ Response"      # Term ID: Data-Services.API-Response
        description: "API metrics"
      
      # Term with non-ASCII (triggers GUID)
      - name: "パフォーマンス"      # Term ID will be a 32-character GUID
        description: "Performance"
```

To see how these all work together, check out this comprehensive example business glossary file below:

```yaml
version: "1"
source: DataHub
owners:
  users:
    - mjames
url: "https://github.com/datahub-project/datahub/"
nodes:
  - name: "Data Classification"
    id: "Data-Classification"                    # Custom ID for stable references
    description: A set of terms related to Data Classification
    knowledge_links:
      - label: Wiki link for classification
        url: "https://en.wikipedia.org/wiki/Classification"
    terms:
      - name: "Sensitive Data"                   # Will generate: Data-Classification.Sensitive-Data
        description: Sensitive Data
        custom_properties:
          is_confidential: "false"
      - name: "Confidential Information"         # Will generate: Data-Classification.Confidential-Information
        description: Confidential Data
        custom_properties:
          is_confidential: "true"
      - name: "Highly Confidential"              # Will generate: Data-Classification.Highly-Confidential
        description: Highly Confidential Data
        custom_properties:
          is_confidential: "true"
        domain: Marketing

  - name: "Personal Information"
    description: All terms related to personal information
    owners:
      users:
        - mjames
    terms:
      - name: "Email"                           # Will generate: Personal-Information.Email
        description: An individual's email address
        inherits:
          - Data-Classification.Confidential    # References parent node path
        owners:
          groups:
            - Trust and Safety
      - name: "Address"                         # Will generate: Personal-Information.Address
        description: A physical address
      - name: "Gender"                          # Will generate: Personal-Information.Gender
        description: The gender identity of the individual
        inherits:
          - Data-Classification.Sensitive       # References parent node path

  - name: "Clients And Accounts"
    description: Provides basic concepts such as account, account holder, account provider, relationship manager that are commonly used by financial services providers to describe customers and to determine counterparty identities
    owners:
      groups:
        - finance
      type: DATAOWNER
    terms:
      - name: "Account"                         # Will generate: Clients-And-Accounts.Account
        description: Container for records associated with a business arrangement for regular transactions and services
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Account"
        inherits:
          - Data-Classification.Highly-Confidential  # References parent node path
        contains:
          - Clients-And-Accounts.Balance            # References term in same node
      - name: "Balance"                            # Will generate: Clients-And-Accounts.Balance
        description: Amount of money available or owed
        term_source: "EXTERNAL"
        source_ref: FIBO
        source_url: "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/ClientsAndAccounts/Balance"

  - name: "KPIs"
    description: Common Business KPIs
    terms:
      - name: "CSAT %"                             # Will generate: KPIs.CSAT
        description: Customer Satisfaction Score
```

## Custom ID Specification

Custom IDs can be specified in two ways, both of which are fully supported and acceptable:

1. Just the ID portion (simpler approach):
```yaml
terms:
  - name: "Email"
    id: "company-email"  # Will become urn:li:glossaryTerm:company-email
    description: "Company email address"
```

2. Full URN format:
```yaml
terms:
  - name: "Email"
    id: "urn:li:glossaryTerm:company-email"
    description: "Company email address"
```

Both methods are valid and will work correctly. The system will automatically handle the URN prefix if you specify just the ID portion.

The same applies for nodes:
```yaml
nodes:
  - name: "Communications"
    id: "internal-comms"  # Will become urn:li:glossaryNode:internal-comms
    description: "Internal communication methods"
```

Note: Once you select a custom ID, it cannot be easily changed.

## Compatibility

Compatible with version 1 of business glossary format. The source will be evolved as newer versions of this format are published.