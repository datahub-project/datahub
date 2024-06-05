import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Schema History

<FeatureAvailability/>

Schema History is a valuable tool for understanding how a Dataset changes over time and gives insight into the following cases,
along with informing Data Practitioners when these changes happened.

- A new field is added
- An existing field is removed
- An existing field changes type

Schema History uses DataHub's [Timeline API](https://datahubproject.io/docs/dev-guides/timeline/) to compute schema changes.

##  Schema History Setup, Prerequisites, and Permissions

Schema History is viewable in the DataHub UI for any Dataset that has had at least one schema change. To view a Dataset, a user
must have the **View Entity Page** privilege, or be assigned to **any** DataHub Role.

## Using Schema History

You can view the Schema History for a Dataset by navigating to that Dataset's Schema Tab. As long as that Dataset has more than
one version, you can view what a Dataset looked like at any given version by using the version selector.
Here's an example from DataHub's official Demo environment with the
<a href="https://demo.datahubproject.io/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pets,PROD)/Schema?is_lineage_mode=false">Snowflake pets dataset</a>.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/schema-history-latest-version.png"/>
</p>


If you click on an older version in the selector, you'll be able to see what the schema looked like back then. Notice
the changes here to the glossary terms for the `status` field, and to the descriptions for the `created_at` and `updated_at`
fields.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/schema-history-older-version.png"/>
</p>


In addition to this, you can also toggle the Audit view that shows you when the most recent changes were made to each field.
You can active this by clicking on the Audit icon you see above the top right of the table.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/schema-history-audit-activated.png"/>
</p>


You can see here that some of these fields were added at the oldest dataset version, while some were added only at this latest
version. Some fields were even modified and had a type change at the latest version!

### GraphQL

* [getSchemaBlame](../graphql/queries.md#getSchemaBlame)
* [getSchemaVersionList](../graphql/queries.md#getSchemaVersionList)

## FAQ and Troubleshooting

**What updates are planned for the Schema History feature?**

In the future, we plan on adding the following features
- Supporting a linear timeline view where you can see what changes were made to various schema fields over time
- Adding a diff viewer that highlights the differences between two versions of a Dataset
