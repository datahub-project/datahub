import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Tags

<FeatureAvailability/>

Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.

Tags can help help you in:

* Querying: Tagging a dataset with a phrase that a co-worker can use to query the same dataset
* Mapping assets to a category or group of your choice

## Tags Setup, Prerequisites, and Permissions

What you need to add tags:

* **Edit Tags** metadata privilege to add tags at the entity level
* **Edit Dataset Column Tags** to edit tags at the column level 

You can create these privileges by creating a new [Metadata Policy](./authorization/policies.md).

## Using DataHub Tags

### Adding a Tag

To add a tag at the dataset or container level, simply navigate to the page for that entity and click on the **Add Tag** button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/add-tag.png"/>
</p>

Type in the name of the tag you want to add. You can add a new tag, or add a tag that already exists (the autocomplete will pull up the tag if it already exists).

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/add-tag-search.png"/>
</p>

Click on the "Add" button and you'll see the tag has been added!

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/added-tag.png"/>
</p>

If you would like to add a tag at the schema level, hover over the "Tags" column for a schema until the "Add Tag" button shows up, and then follow the same flow as above.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/add-schema-tag.png"/>
</p>

### Removing a Tag

To remove a tag, simply click on the "X" button in the tag. Then click "Yes" when prompted to confirm tag removal.

### Searching by a Tag

You can search for a tag in the search bar, and even filter entities by the presence of a specific tag.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master/imgs/search-tag.png"/>
</p>

## Additional Resources

### Videos

**Add Ownership, Tags, Terms, and more to DataHub via CSV!**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/BGt59KpH1Ds" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>

### GraphQL

* [addTag](../graphql/mutations.md#addtag)
* [addTags](../graphql/mutations.md#addtags)
* [batchAddTags](../graphql/mutations.md#batchaddtags)
* [removeTag](../graphql/mutations.md#removetag)
* [batchRemoveTags](../graphql/mutations.md#batchremovetags)
* [createTag](../graphql/mutations.md#createtag)
* [updateTag](../graphql/mutations.md#updatetag)
* [deleteTag](../graphql/mutations.md#deletetag)

You can easily fetch the Tags for an entity with a given its URN using the **tags** property. Check out [Working with Metadata Entities](./api/graphql/how-to-set-up-graphql.md#querying-for-tags-of-an-asset) for an example. 

### DataHub Blog

* [Tags and Terms: Two Powerful DataHub Features, Used in Two Different Scenarios
Managing PII in DataHub: A Practitioner’s Guide](https://blog.datahubproject.io/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e)

## FAQ and Troubleshooting

**What is the difference between DataHub Tags and Glossary Terms?**

DataHub Tags are informal, loosely controlled labels while Terms are part of a controlled vocabulary, with optional hierarchy. Tags have no element of formal, central management.

Usage and applications:

* An asset may have multiple tags.
* Tags serve as a tool for search & discovery while Terms are typically used to standardize types of leaf-level attributes (i.e. schema fields) for governance. E.g. (EMAIL_PLAINTEXT)

**How are DataHub Tags different from Domains?**

Domains are a set of top-level categories usually aligned to business units/disciplines to which the assets are most relevant. They rely on central or distributed management. A single domain is assigned per data asset.



### Related Features

* [Glossary Terms](./glossary/business-glossary.md)
* [Domains](./domains.md)
