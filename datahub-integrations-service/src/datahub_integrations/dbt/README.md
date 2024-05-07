# dbt bidirectional sync

Provides an action that listens to the MCL / PE Kafka topics and syncs changes back to the dbt repo.

## Usage

```yml
# datahub action config
action:
  type: datahub_integrations.dbt.dbt_sync_back:DbtSyncBackAction
  config:
    github_repo_org: org
    github_repo_name: repo
    token: ${GITHUB_TOKEN} # can be a GitHub token, oauth token, or personal access token
    subdir: .
```

## Details

On the first change, we will create a branch `acryl-dbt-sync` based off `master`/`main`, commit changes to the branch, and open a PR.
As changes occur, we will continuously add commits to that branch and update the PR description accordingly.

Supported change types:

- [x] Add/remove tag to dbt node
- [ ] Add/remove column tag to dbt node
- [x] Add/remove term to dbt node
- [ ] Add/remove column term to dbt node
- [x] Add/remove owner to dbt node
  - Does not support custom ownership types due to https://acryldata.slack.com/archives/C01H76AFDSP/p1713808736276469
- [x] Set documentation for dbt node
- [ ] Set column documentation for dbt node

Supported dbt node types:

- [x] model
- [x] source
- [x] snapshot
- [ ] seed

### Assumptions

- When GitHub PRs are merged, the branch is deleted.
- dbt ingestion must be run with CLI version > TODO (requires https://github.com/datahub-project/datahub/pull/10371)
- If the corresponding `.yml` file for a model does not exist, we will create a file called `schema.yml` in the same directory as the model.

### Limitations

Fixable:

- Changes must be made to the "dbt entity" in the sibling relationship. Changes made to the data warehouse entity will not be synced back.
- This does not support setups with multiple dbt projects that are distinguished with platform instances.
- dbt ingestion must be run with `tag_prefix: ''` set.

Not fixable in the short term:

- This is not aware of any tags/terms/owners that are added by dbt meta mapping configs instead of the meta.datahub section.
- If any git conflicts occur, the end user is responsible for resolving them.
- If we are unable to generate a dbt commit based on a change, the change will be logged and ignored.

## TODO

- Avoid using `dbt ls` to find the original file paths for dbt nodes. We should have that info in custom properties already.
- Build out support for more change types.
- Add more tests for operation appliers.
  - [x] Add/remove tag appliers
  - [ ] Add/remove term appliers
  - [ ] Add/remove owner appliers
  - [ ] Set documentation appliers
- Add tests for event -> operation conversion.
