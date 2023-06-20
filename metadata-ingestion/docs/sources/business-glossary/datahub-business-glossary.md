### Business Glossary File Format

The business glossary source file should be a `.yml` file. For a comprehensive example, look [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/business_glossary.yml).

A business glossary can have the following top-level keys:

**Glossary**: the top level keys of the business glossary file

Example **Glossary**:

```yaml
version: 1                                     			# the version of business glossary file config the config conforms to. Currently the only version released is `1`.
source: DataHub                                			# the source format of the terms. Currently only supports `DataHub`
owners:                                        			# owners contains two nested fields
  users:                                       		    # (optional) a list of user IDs
    - mjames
  groups:                                               # (optional) a list of group IDs
    - finance
url: "https://github.com/datahub-project/datahub/"      # (optional) external url pointing to where the glossary is defined externally, if applicable
nodes:                                                  # (optional) list of child **GlossaryNode** objects. See **GlossaryNode** section below
	...
terms:                                                  # (optional) list of child **GlossaryTerm** objects. See **GlossaryTerm** section below
	...
```

**GlossaryNode**: a container of **GlossaryNode** and **GlossaryTerm** objects

Example **GlossaryNode**:

```yaml
- name: Housing                                                 # name of the node
  description: Provides terms related to the housing domain     # description of the node
  id: "urn:li:glossaryTerm:41516e310acbfd9076fffc2c98d2d1a3"    # (optional) identifier of the node (normally inferred from the name, see e`nable_auto_id` config. Use this if you need a stable identifier)
  owners:                                                       # (optional) owners contains 2 nested fields
    users:                                                      # (optional) a list of user IDs
      - mjames
    groups:                                                     # (optional) a  list of group IDs
      - interior
  terms:                                                        # list of **GlossaryTerm** objects
    ...
  nodes:                                                        # list of child **GlossaryNode** objects
    ...
  knowledge_links:                                              # (optional) list of **KnowledgeCard** objects
    - label: Wiki link for housing
      url: "https://en.wikipedia.org/wiki/Housing"
```

**GlossaryTerm**: a term in your business glossary

Example **GlossaryTerm**:

```yaml
- name: Silverware                                                          # name of the term
  description: an implement used for eating, stirring, and serving food.    # description of the term
  id: "urn:li:glossaryTerm:41516e310acbfd9076fffc2c98d2d2b4"                # (optional) identifier of the node (normally inferred from the name, see e`nable_auto_id` config. Use this if you need a stable identifier)
  owners:                                                                   # (optional) owners contains 2 nested fields
    users:                                                                  # (optional) a list of user IDs
      - mjames
    groups:                                                                 # (optional) a  list of group IDs
      - interior
  term_source: "EXTERNAL"                                                   # one of `EXTERNAL` or `INTERNAL`. Whether the term is coming from an external glossary or one defined in your organization.
  source_ref: FIBO                                                          # (optional) if external, what is the name of the source the glossary term is coming from?
  source_url: "https://www.google.com"                                      # (optional) if external, what is the url of the source definition?
  inherits:                                                                 # (optional) list of **GlossaryTerm** that this term inherits from
    -  Housing.Kitchen
  contains:                                                                 # (optional) a list of **GlossaryTerm** that this term contains
    - Housing.Kitchen.Spoon
    - Housing.Kitchen.Fork
    - Housing.Kitchen.Knife
  values:                                                                   # (optional) a list of values describing the term
    - Housing.Material.Silver
  related_terms:                                                            # (optional) a list of related terms
    - Housing.Kitchen.Cutlery
  custom_properties:                                                        # (optional) a map of key/value pairs of arbitrary custom properties
    - is_confidential: false
  knowledge_links:                                                          # (optional) a list of **KnowledgeCard** related to this term
    - url: "https://en.wikipedia.org/wiki/Silverware"
      label: Wiki link
  domain: "urn:li:domain:Design"                                            # (optional) domain name or domain urn
```

To see how these all work together, please see the example file at the top of this section.


## Compatibility

Compatible with version 1 of business glossary format. 
The source will be evolved as we publish newer versions of this format.