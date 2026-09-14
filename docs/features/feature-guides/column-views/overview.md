import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Column Views

<FeatureAvailability />

**Column Views** let you choose which columns appear in a dataset's schema table and save that layout to reuse. When you activate a Column View, the **Columns** table on every dataset's **Schema** tab shows exactly the columns you picked, in the order you picked them.

**Why use Column Views?**

- **See field-level metadata at a glance** — show a column's classification, retention period, or other [Structured Properties](/docs/features/feature-guides/properties/overview.md) right next to its name and type.
- **Read logical model mappings off the schema** — add a **Logical Parent** or **Physical Children** column to see how columns map across [Logical Models](/docs/features/feature-guides/logical-models/overview.md).
- **Standardize how your organization reads schemas** — set an organization-wide default so everyone sees the same layout, and let users tailor their own.

:::note Column Views vs. Views
[Views](/docs/features/feature-guides/views/overview.md) filter which assets you see. Column Views control which columns appear in a dataset's schema table.
:::

## Available Columns

| Column                  | Shows                                                         |
| ----------------------- | ------------------------------------------------------------- |
| **Name**                | Field name                                                    |
| **Type**                | Data type                                                     |
| **Native type**         | The data type exactly as the source system reports it (for example `VARCHAR(255)`, `DECIMAL(18,2)`) |
| **Length**              | Declared length, when the native type carries one             |
| **Precision / Scale**   | Declared precision and scale, when the native type carries them |
| **Nullable**            | A check mark when the field is nullable                       |
| **Primary Key**         | A check mark when the field is part of the primary key        |
| **Partition Key**       | A check mark when the field is a partitioning key             |
| **Description**         | Field documentation                                           |
| **Tags**                | Tags on the field                                             |
| **Glossary Terms**      | Glossary terms on the field                                   |
| **Business Attribute**  | The Business Attribute linked to the field                    |
| **Stats**               | Column profiling and usage statistics                         |
| **Structured Property** | The value of one Structured Property, one column per property |
| **Label**               | A check mark when the field has a specific tag or glossary term |
| **Relationships**       | Related columns or assets: Logical Parent, Physical Children, Upstream Columns, Downstream Columns, Foreign Key To, Referenced By |

## Public vs. Personal Column Views

| Type                   | Visible to       | Who can create | Can be set as       |
| ---------------------- | ---------------- | -------------- | ------------------- |
| **Public (Global)**    | All users        | Admins only    | User or org default |
| **Personal (Private)** | Only the creator | Any user       | User default only   |

Any user can create **personal** Column Views for their own use. **Public** Column Views are shared across the organization and require the **Manage Public Column Views** platform privilege, which is granted to admins by default.

## Creating a Column View

1. Open a dataset's **Schema** tab and click the **Columns** selector above the table
2. Click **+ Create Column View** and choose **Public** or **Personal**
3. Name the Column View, pick your columns, drag them into order, and click **Save**

Use a column's settings to control its width, how related columns are labelled, and how long lists are shortened.

You can also adjust the columns directly on the Schema tab and save the result as a Column View.

## Sorting and Filtering

A Column View can also save a default sort and a set of row filters. Pick a column to sort by, and add filters such as fields with a given tag or glossary term, fields without a description, or fields with a particular Structured Property value.

## Editing and Deleting Column Views

Open the **Columns** selector and click the **...** menu next to any Column View to rename it, change its columns, or delete it. Personal Column Views can only be modified by their creator. Public Column Views can be managed by any admin with the **Manage Public Column Views** privilege.

## Where Column Views Are Applied

An active Column View changes the **Columns** table on the **Schema** tab of every dataset.

## Default Column Views

You can set a default Column View so it's applied automatically whenever you open a dataset's Schema tab. Defaults work at two levels.

### Personal Default

Any user can pick their own default via the **...** menu > **Make my default**. This can be any public or personal Column View.

A personal default always takes priority over the organization default.

### Organization Default

Admins can designate one public Column View as the organization-wide default via the **...** menu > **Make organization default**. This kicks in for any user who hasn't chosen their own default. Requires the **Manage Public Column Views** privilege.

You can always switch or clear the active Column View during a session.

## Managing Column Views

Go to **Settings > My Column Views** to see all your personal and public Column Views in one place. From here you can create, edit, delete, or change defaults.

## Permissions

| Action                               | Who can do it                              |
| ------------------------------------ | ------------------------------------------ |
| Create a personal Column View        | Any user                                   |
| Create a public Column View          | Admins with **Manage Public Column Views** |
| Edit / delete a personal Column View | The user who created it                    |
| Edit / delete a public Column View   | Admins with **Manage Public Column Views** |
| Set personal default                 | Any user (for themselves)                  |
| Set organization default             | Admins with **Manage Public Column Views** |

## Advanced Usage

### GraphQL API

Column Views are managed with the `createColumnView`, `updateColumnView`, and `deleteColumnView` mutations, and listed with `listMyColumnViews` and `listGlobalColumnViews`. Columns are listed in display order:

```graphql
mutation {
  createColumnView(
    input: {
      viewType: PERSONAL
      name: "Governance review"
      definition: {
        columns: [
          { type: TYPE }
          { type: DESCRIPTION }
          {
            type: STRUCTURED_PROPERTY
            structuredPropertyParams: { urn: "urn:li:structuredProperty:io.acryl.privacy.retentionTime" }
          }
          { type: GLOSSARY_TERMS }
          { type: LABEL, labelParams: { urn: "urn:li:glossaryTerm:Classification.PII" } }
          { type: LOGICAL_PARENT }
          { type: DOWNSTREAM_COLUMNS, display: { maxItems: 3 } }
        ]
        sort: { column: { type: TYPE }, order: ASCENDING }
        filter: {
          operator: AND
          filters: [{ field: "tags", values: ["urn:li:tag:PII"] }]
        }
      }
    }
  ) {
    urn
  }
}
```

Set a personal default with `updateCorpUserColumnViewsSettings` and the organization default with `updateGlobalColumnViewsSettings`.
