### Prerequisites

If you want to ingest MSSQL Jobs and stored procedures (with code) the user credentials needs the proper privileges.

Script for granting the privileges:
```
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'

USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```

### Extended Properties

#### Feature Overview

##### What are Extended Properties?

Extended Properties in MSSQL allow users to add custom metadata to database objects such as tables, columns, and schemas. This feature helps in storing additional descriptive information directly within the database.

##### Why Use Extended Properties?
- Enhanced Metadata Management: Store additional context about your data directly within the database.
- Improved Data Governance: Helps in better understanding and managing your data assets.
- Custom Annotations: Allows for annotations that can be specific to your organizational needs.

##### General Purpose
Ingest information from MSSQL's extended properties into DataHub and display it in the appropriate places in DataHub.

#### Goal

Ingest additional metadata properties to enrich the data models and provide more comprehensive context.
Implementation Details

This feature works with tables and views. The retrieved properties are mapped to the appropriate DataHub fields: Description, Tags, Owners and Domain.

Descriptions and Tags can be applied at the schema level of objects, providing context and categorization at a broader scope. All properties (Description, Tags, Owners and Domain) can be applied at the object level, allowing for detailed metadata management of individual database objects.

#### Configuration

Configuring Extended Properties

1. Add to the Ingestion Module Configuration:

    Add the following new setting:

    ```add_extended_properties: True|False (default: False)```

    **_NOTE:_** True only if include_descriptions == True.

2. Add New Setting for Mapping Extended Properties:

    Add the following new setting:

    ```map_extended_properties: Dict(str: Dict)```

    **_NOTE:_** This configuration is used only if add_extended_properties == True.

#### Structure of map_extended_properties

- map_extended_properties: Specifies how and where to write custom MSSQL extended properties to DataHub.

    Key: str - exact name of the extended properties (e.g., "MS_Description", "OWNER").

    Value: dict - contains descriptions of type, actions, strategy, and additional information. Actions and strategies can vary for different types.

##### Types

###### Description

The description type allows you to manage textual descriptions of database objects. Extended properties of this type provide valuable context for data assets.

###### Actions:

- overwrite: Overwrites all descriptions. In DataHub, only the last description from the configuration for this type will be visible.

- append: Adds to the previous description. If this action is used, a delimiter can be added. If the delimiter is absent, the default is used: \n ___ \n.

**_NOTE:_** default action: append

**_Important:_** Additionally, the first description will always be from ms_description.

**_NOTE:_** Actions only have an effect within the configuration. In DataHub, descriptions created via the UI will not be overwritten. However, information from extended properties descriptions can be viewed in the edit description window.

###### Example Configuration:

```map_extended_properties:
  "X_Description_1": {
    "type": "description",
    "strategy": "append",
    "delimiter": "; "
  },
  "X_Description_2": {
    "type": "description",
    "strategy": "overwrite"
  }
  ```

In this example, we are mapping only the value of MSSQL's extended property X_Description_2 to the description of DataHub's dataset.

###### Owner

The owner type is crucial for identifying the responsible individuals or teams for database objects. This helps in assigning accountability and managing data stewardship.

In MSSQL extended properties, responsible persons should be listed as either users' emails or names (email up to "@") and presented as a comma-separated list.

Requires additional setting ownership_type. Values must be existing DataHub ownership types (e.g., "TECHNICAL_OWNER", "PRODUCER").

###### Actions:

- overwrite: Overwrites existing users with the corresponding ownership_type.

- append: Adds another user to the existing ones with the corresponding ownership_type.

**_NOTE:_** default action: append

**_Important:_**  We check for the presence of these users in DataHub. If they are absent, you will see an error in the logs: “Ingestion error: Absent: {list of users}”.

###### Example Configuration:

```
map_extended_properties:
  "X_OWNER_1": {
    "type": "owner",
    "strategy": "append",
    "ownership_type": "TECHNICAL_OWNER"
  },
  "X_OWNER_2": {
    "type": "owner",
    "strategy": "overwrite",
    "ownership_type": "PRODUCER"
  }
```

###### Tag

The tag type is used for categorizing and labeling data assets. Tags help in organizing and retrieving information based on specific criteria.

In MSSQL extended properties, tags can be recorded as a comma-separated list. Each tag is considered a string enclosed in quotes (between “).

###### Actions:

- overwrite: Overwrites the existing tags.
- append: Adds another tag to the existing ones.

**_NOTE:_** default action: append

If a new tag is not present in DataHub, it will be created automatically.

###### Example Configuration:

```
map_extended_properties:
  "X_TAG_1": {
    "type": "tag",
    "strategy": "append"
  },
  "X_TAG_2": {
    "type": "tag",
    "strategy": "overwrite"
  }
```

###### Domains

The domains type is used for associating database objects with specific domains. Only the first domain will be recorded, which simplifies domain management.

In MSSQL extended properties, domains can be listed, but only the first domain will be recorded in DataHub.

There is no strategy for domains.

**_NOTE:_** A domain added via the UI will be overwritten.

**_Important:_** We check for the presence of the domain in DataHub. If it is absent, you will see an error in the logs: “ERROR … Failed to retrieve domain id for domain {domain name}”.

###### Example Configuration:

```
map_extended_properties:
  X_DOMAIN": {
    "type": "domains"
  }
```

###### Combining Different Types:

You can also combine different types in a single configuration.
map_extended_properties:
```
  "X_Description_1": {
    "type": "description",
    "strategy": "append",
    "delimiter": "; "
  },
  "X_Description_2": {
    "type": "description",
    "strategy": "overwrite"
  },
  "X_OWNER_1": {
    "type": "owner",
    "strategy": "append",
    "ownership_type": "TECHNICAL_OWNER"
  },
  "X_OWNER_2": {
    "type": "owner",
    "strategy": "overwrite",
    "ownership_type": "PRODUCER"
  },
  "X_TAG_1": {
    "type": "tag",
    "strategy": "append"
  },
  "X_TAG_2": {
    "type": "tag",
    "strategy": "overwrite"
  },
  "X_DOMAIN": {
    "type": "domains"
  }
```

In this example, various types are mapped in one configuration, demonstrating how to specify different handling strategies for descriptions, owners, tags, and domains.