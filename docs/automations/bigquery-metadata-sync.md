import FeatureAvailability from '@site/src/components/FeatureAvailability';

# BigQuery Metadata Sync Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in open beta in Acryl Cloud. Reach out to your Acryl representative to get access.

:::

## Introduction

BigQuery Metadata Sync is an automation that synchronizes DataHub Tags, Table and Column descriptions, and Column Glossary Terms with
BigQuery. This automation is exclusively available in DataHub Cloud (Acryl).

## Use-Cases

- Maintain consistent metadata across DataHub and BigQuery
- Improve data discovery by propagating rich descriptions back to BigQuery
- Enhance data governance by applying Policy Tags based on DataHub Glossary Terms
- Streamline data classification by syncing DataHub Tags to BigQuery Labels
- Facilitate compliance efforts by automatically tagging sensitive data columns
- Support data lineage tracking by keeping metadata aligned across platforms

## Sync Capabilities

| DataHub Source | BigQuery Target | Sync Direction | Notes |
|----------------|-----------------|----------------|--------|
| Table Tags | Table Labels | Bi-directional | Changes in either system reflect in both |
| Table Descriptions | Table Descriptions | Bi-directional | Changes in either system reflect in both |
| Column Descriptions | Column Descriptions | Bi-directional | Changes in either system reflect in both. <br/> Thes sync doesn't delete table description from BigQuery |
| Column Glossary Terms | Column Policy Tags | DataHub → BigQuery | Created under DataHub taxonomy |

## Setup Instructions

### 1. Verify Permissions

Ensure your service account has the following permissions:

| Task | Required Permissions | Available Role |
|------|---------------------|----------------|
| Policy Tag Management | • `datacatalog.taxonomies.create`<br/>• `datacatalog.taxonomies.update`<br/>• `datacatalog.taxonomies.list`<br/>• `datacatalog.taxonomies.get`<br/>• `bigquery.tables.createTagBinding` | Policy Tag Admin |
| Policy Tag Assignment | • `bigquery.tables.updateTag` | - |
| Description Management | • `bigquery.tables.update` | - |
| Label Management | • `bigquery.tables.update` | - |

**Note**: `bigquery.tables` permissions must be granted in every project where metadata sync is needed.

### 2. Enable the Automation

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

  <p align="left">
    <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
  </p>

2. **Create An Automation**: Click on 'Create' and select 'BigQuery Tag Propagation'.

  <p align="left">
    <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/automation-type.png"/>
  </p>

3. **Configure Automation**:

    1. **Select a Propagation Action**

    <p align="left">
      <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/automation-form.png"/>
    </p>

    | Propagation Type | DataHub Entity | BigQuery Entity | Note |
    | -------- | ------- | ------- | ------- |
    | Table Tags as Labels | [Table Tag](https://datahubproject.io/docs/tags/) | [BigQuery Label](https://cloud.google.com/bigquery/docs/labels-intro) | - |
    | Column Glossary Terms as Policy Tags | [Glossary Term on Table Column](https://datahubproject.io/docs/glossary/business-glossary/) | [Policy Tag](https://cloud.google.com/bigquery/docs/best-practices-policy-tags) | <ul><li>Assigned Policy tags are created under DataHub taxonomy.</li></ul><ul><li>Only the latest assigned glossary term set as policy tag. BigQuery only supports one assigned policy tag.</li></ul> <ul><li>Policy Tags are not synced to DataHub as glossary term from BigQuery.</li></ul>
    | Table Descriptions | [Table Description](https://datahubproject.io/docs/api/tutorials/descriptions/) | Table Description | - |
    | Column Descriptions | [Column Description](https://datahubproject.io/docs/api/tutorials/descriptions/) | Column Description | - |

    :::note

    You can limit propagation based on specific Tags and Glossary Terms. If none are selected, ALL Tags or Glossary Terms will be automatically propagated to BigQuery tables and columns. (The recommended approach is to not specify a filter to avoid inconsistent states.)

    :::

    :::note

    - BigQuery supports only one Policy Tag per table field. Consequently, the most recently assigned Glossary Term will be set as the Policy Tag for that field.
    - Policy Tags cannot be applied to fields in External tables. Therefore, if a Glossary Term is assigned to a field in an External table, it will not be applied.

    :::

    2. **Fill in the required fields to connect to BigQuery, along with the name, description, and category**

    <p align="left">
      <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/connection_config.png"/>
    </p>

    3. **Finally, click 'Save and Run' to start the automation**

## 3. Propagating for Existing Assets (Optional)

To ensure that all existing table Tags and Column Glossary Terms are propagated to BigQuery, you can back-fill historical data for existing assets. Note that the initial back-filling process may take some time, depending on the number of BigQuery assets you have.

To do so, follow these steps:

1. Navigate to the Automation you created in Step 3 above
2. Click the 3-dot "More" menu

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

3. Click "Initialize"

<p align="left">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing descriptions. If you only want to begin propagating descriptions going forward, you can skip this step.

## Viewing Propagated Tags

You can view propagated Tags inside the BigQuery UI to confirm the automation is working as expected.

<p align="left">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/labels.png"/>
</p>

## Troubleshooting BigQuery Propagation

### Q: What metadata elements support bi-directional syncing between DataHub and BigQuery?

A: The following metadata elements support bi-directional syncing:

- Tags (via BigQuery Labels): Changes made in either DataHub Table Tags or BigQuery Table Labels will be reflected in the other system.
- Descriptions: Both table and column descriptions are synced bi-directionally.

### Q: Are Policy Tags bi-directionally synced?

A: No, BigQuery Policy Tags are only propagated from DataHub to BigQuery, not vice versa. This means that Policy Tags should be mastered in DataHub using the [Business Glossary](https://datahubproject.io/docs/glossary/business-glossary/).

It is recommended to avoid enabling `extract_policy_tags_from_catalog` during
ingestion, as this will ingest policy tags as BigQuery labels. Our sync process
propagates Glossary Term assignments to BigQuery as Policy Tags.

In a future release, we plan to remove this restriction to support full bi-directional syncing.

### Q: What metadata is synced from BigQuery to DataHub during ingestion?

A: During ingestion from BigQuery:

- Tags and descriptions from BigQuery will be ingested into DataHub.
- Existing Policy Tags in BigQuery will not overwrite or create Business Glossary Terms in DataHub. It only syncs assigned column Glossary Terms from DataHub to BigQuery.

### Q: Where should I manage my Business Glossary?

A: The expectation is that you author and manage the glossary in DataHub. Policy tags in BigQuery should be treated as a reflection of the DataHub glossary, not as the primary source of truth.

### Q: Are there any limitations with Policy Tags in BigQuery?

A: Yes, BigQuery only supports one Policy Tag per column. If multiple glossary
terms are assigned to a column in DataHub, only the most recently assigned term
will be set as the policy tag in BigQuery. To reduce the scope of conflicts, you
can set up filters in the BigQuery Metadata Sync to only synchronize terms from
a specific area of the Business Glossary.

### Q: How frequently are changes synced between DataHub and BigQuery?

A: From DataHub to BigQuery, the sync happens instantly (within a few seconds)
when the change occurs in DataHub.

From BigQuery to DataHub, changes are synced when ingestion occurs, and the frequency depends on your custom ingestion schedule. (Visible on the **Integrations** page)

### Q: What happens if there's a conflict between DataHub and BigQuery metadata?

A: In case of conflicts (e.g., a tag is modified in both systems between syncs), the DataHub version will typically take precedence. However, it's best to make changes in one system consistently to avoid potential conflicts.

### Q: What permissions are required for bi-directional syncing?

A: Ensure that the service account used for the automation has the necessary permissions in both DataHub and BigQuery to read and write metadata. See the required BigQuery permissions at the top of the page.

### Q: Can table description removed?

No, the sync can only modify table description but it won't remove or clear a description from a table. 

## Related Documentation

- [DataHub Tags Documentation](https://datahubproject.io/docs/tags/)
- [DataHub Glossary Documentation](https://datahubproject.io/docs/glossary/business-glossary/)
- [BigQuery Labels Documentation](https://cloud.google.com/bigquery/docs/labels-intro)
- [BigQuery Policy Tags Documentation](https://cloud.google.com/bigquery/docs/best-practices-policy-tags)
