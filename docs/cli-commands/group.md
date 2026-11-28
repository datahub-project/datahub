# datahub group

The `group` command allows you to interact with the Group entity.
It currently supports the `upsert` operation, which can be used to create a new group or update an existing one with embedded Users.
For more information, please refer to [Creating Users and Groups with DataHub CLI](/docs/api/tutorials/owners.md#upsert-users).

```shell
datahub group upsert -f group.yaml
```

An example of `group.yaml` would look like as in [foo.group.dhub.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/cli_usage/group/foo.group.dhub.yaml) file for the complete code.

```yaml
id: foogroup@acryl.io
display_name: Foo Group
owners:
  - datahub
members:
  - bar@acryl.io # refer to a user either by id or by urn
  - id: joe@acryl.io # inline specification of user
    slack: "@joe_shmoe"
    display_name: "Joe's Hub"
```
