# Tags

Tags allow labeling entities with markers that have a universally agreed upon definition.
A tag with a given name is universal across datahub. Although a tag can be applied to many entities, its description, 
id and creator is shared.

### How can tags be applied?
Tags can be applied to datasets, schema fields, charts and dashboards. They are presented on the entity's
detail page.

![image](https://user-images.githubusercontent.com/2455694/111394180-6b0a4f00-8677-11eb-8af7-afa794ae1a84.png)


These tags be added and created from 

a) Datahub UI. Click the `Add Tag` badge on an entity or schema field to enter the add/create flow.

and

b) Ingestion pipeline. see [bootstrap_mce](../../metadata-ingestion/examples/mce_files/bootstrap_mce.json) for an example of creatings tags and applying them to entities.

### Search by Tags

Elastic search indexes tags applied to datasets. To find datasets with a given tag, search either
for `tags:<tag-name>` or simply `tag-name`.

### Tag Profiles

Clicking on a tag will bring you to the tag's detail page. There you can see an overview of how many entities
this tag has been applied to, along with a description and the creator of the tag.
