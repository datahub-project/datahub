### Business Glossary File Format

The business glossary source file should be a `.yml` file with the following top-level keys:

**Glossary**: the top level keys of the business glossary file
- **version**: the version of business glossary file config the config conforms to. Currently the only version released is `1`.
- **source**: the source format of the terms. Currently only supports `DataHub`
- **owners**: owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **url**: (optional) external url pointing to where the glossary is defined externally, if applicable.
- **nodes**: (optional) list of child **GlossaryNode** objects
- **terms**: (optional) list of child **GlossaryTerm** objects


**GlossaryNode**: a container of **GlossaryNode** and **GlossaryTerm** objects
- **name**: name of the node
- **description**: description of the node
- **id**: (optional) identifier of the node (normally inferred from the name, see `enable_auto_id` config. Use this if you need a stable identifier)
- **owners**: (optional) owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **terms**: (optional) list of child **GlossaryTerm** objects
- **nodes**: (optional) list of child **GlossaryNode** objects

**GlossaryTerm**: a term in your business glossary
- **name**: name of the term
- **description**: description of the term
- **id**: (optional) identifier of the term (normally inferred from the name, see `enable_auto_id` config. Use this if you need a stable identifier)
- **owners**: (optional) owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **term_source**: One of `EXTERNAL` or `INTERNAL`. Whether the term is coming from an external glossary or one defined in your organization.
- **source_ref**: (optional) If external, what is the name of the source the glossary term is coming from?
- **source_url**: (optional) If external, what is the url of the source definition?
- **inherits**: (optional) List of **GlossaryTerm** that this term inherits from
- **contains**: (optional) List of **GlossaryTerm** that this term contains
- **custom_properties**: A map of key/value pairs of arbitrary custom properties

You can also view an example business glossary file checked in [here](../../../examples/bootstrap_data/business_glossary.yml)

## Compatibility

Compatible with version 1 of business glossary format. 
The source will be evolved as we publish newer versions of this format.
