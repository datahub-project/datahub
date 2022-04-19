- Start Date: 2022-04-19
- RFC PR:
- Discussion Issue: 
- Implementation PR(s): 

# Modeling Data Permissions

## Summary

Many datahub sources provide access control mechanisms to manage user access to assets.
Datahub should provide functionality for viewing these permissions. 

## Motivation

Within many sql systems, the data control language provides commands like GRANT, DENY and REVOKE to manage control to datasets.

Being able to view grants is the first step toward supporting larger integrations with dataset permissions. [See Here](https://feature-requests.datahubproject.io/b/feedback/p/access-control-on-datasets-bi-assets)

Of the datahub integration sources listed, the following have some form of data access control management:
 - Athena (via IAM)
 - BigQuery
 - Clickhouse
 - Druid
 - Elastic Search
 - Glue
 - Hive (via Apache Ranger)
 - Kafka
 - Looker
 - MariaDB
 - Metabase
 - MongoDB
 - MicrosoftSQLServer
 - MySQL
 - Oracle
 - PostgreSQL
 - Redash
 - Redshift
 - S3 (via IAM)
 - Sagemaker
 - Snowflake
 - Superset
 - Tableau
 - Trino

## Requirements

The permissions aspect must be:
- Appliable to datasets, containers, dashboards
- Support ingesting and viewing structured access control (GRANT {permission} ON {object} TO {user or role}
- Support ingesting and viewing unstructured access control (IAM Policy Documents, Custom Policies from other datasets)
- 
### Extensibility

> Please also call out extensibility requirements. Is this proposal meant to be extended in the future? Are you adding
> a new API or set of models that others can build on in later? Please list these concerns here as well.

## Non-Requirements

This RFC specifically calls for functionality to view the permissions. Actions taken on the permissions may be great areas for future enhancement but is not a focus here.

## Detailed design

There are no new Entities associated with this design.

There is one aspect proposed:

```
record DataPermission {
   structuredPermissions: array[StructuredDataPermission]
   unstructuredPermissions: array[UnstructuredDataPermission]
}

record StructuredDataPermission {
   permission: string
   object: Urn // if not specified, is implicitly the urn of the containing Entity
   subject: union[corpUser, corpGroup, string] // grant to a user, group, or non-standard entity (e.g. "*", "all", "NONE")
}

record UnstructuredDataPermission {
  language: string // maybe ENUM? (e.g. "IAM")
  string: string
}
```

This aspect should be attachable to
- Datasets (e.g. SQL Grants on Tables)
- Containers (e.g. Grants on Schemas/Databases)
- Dashboards (e.g. Tableau permissions on dashboard access)
- Charts (e.g. Tableau permissions on chart access)


## How we teach this

We should ingest data from sources that support it automatically.
Many sources already support permissions modeling and providing examples in user-guides will help people learn this.

## Drawbacks

Permissions on datasets can be restricted/sensitive data.
As an aspect on many different entities, managing RBAC for the ability to view/query these permissions may be difficult.

SQLAlchemy does not provide a standard mechanism via inspector for viewing permissions.
Many SQL Sources support `information_schema` and thus we can query those tables to view these.
This may end up with significant source-specific work to support.

## Alternatives

A simple string may suffice or these permissions may be injected into description attributes of datasets.

## Rollout / Adoption Strategy

This functionality is optional and does not break any other functionality.

## Future Work

- Provide UI Visualization for Permissions
- Emit permission related events for downstream applications to consume.
- Visualize permissions from the reverse direction (i.e. given a user/group, which grants are set on them)
- (Provide a way to change permissions on data from within datahub)[https://feature-requests.datahubproject.io/b/feedback/p/access-control-on-datasets-bi-assets]

## Unresolved questions
- Permissions Modeling (RBAC vs ABAC) (Different kinds of structured modeling)
- Naming
  - Permission vs Policies
  - Subject vs User vs Owner vs Group
- Permissions on permissions (i.e who can see this)
- UI/UX (How do we visualize this)

