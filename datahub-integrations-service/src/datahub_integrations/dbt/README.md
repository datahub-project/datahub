# dbt bidirectional sync

Our dbt integration already supports syncing metadata, like tags, terms, owners, and documentation, into DataHub.
This helps you do the reverse: sync changes to that metadata from DataHub back into the dbt repo.

When used together, DataHub and dbt are continuously kept in sync, and users a free to reference and modify metadata in whatever tool they feel most comfortable with.

### How it works

We sync _changes_ that occur in DataHub back to your dbt git repo.
On the first change, we will create a new branch (called `acryl-dbt-sync` by default) based off `master`/`main`, commit and push changes to the branch, and open a PR on GitHub.
As subsequent changes occur, we will continuously add commits to that branch and update the PR description accordingly.
After the PR is merged, subsequent changes will cause a new PR to be opened.
In other words, there will be a single persistent PR that contains all the synced changes.

### Requirements

- When GitHub PRs are merged, the branch should be deleted. GitHub can be configured to [automatically delete branches on merge](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-the-automatic-deletion-of-branches).
- dbt ingestion must be run with CLI version 0.13.13.4 or newer.

### Configuration

- **auth**: We can either use a GitHub token or (advanced) authenticate as a GitHub app. Either way, it requires the following permissions:

  - `contents: write`: Needed to access the dbt repo contents and push commits to our branch.
  - `pull_requests: write`: Needed to create and update pull requests.

- **yml_file_creation_mode**: When a dbt entity does not already have a corresponding `.yml` file in the dbt repo, we will create one. If this config is set to `directory_schema_yml`, we will create a `schema.yml` file in the same directory as the dbt entity. If this config is set to `yml_per_model`, we will create a `<model>.yml` file in the same directory as the `.sql` file.

- **dbt_tag_prefix**: This should match the `tag_prefix` config in the dbt ingestion.

- **open_draft_prs**: Default `false`. When enabled, PRs will be opened in draft mode.

### Capabilities

Supported metadata change types:

- [x] Add/remove tag to dbt node
- [ ] Add/remove column tag to dbt node
- [x] Add/remove term to dbt node
- [ ] Add/remove column term to dbt node
- [x] Add/remove owner to dbt node (but not custom ownership types)
- [x] Set documentation for dbt node
- [ ] Set column documentation for dbt node
- [x] Set the domain of a dbt node

Supported dbt node types:

- [x] model
- [x] source
- [x] snapshot
- [ ] seed

This supports siblings, so changes to either the dbt entity or the data warehouse entity will be synced back.

### Limitations

- This does not support setups with multiple dbt projects that are distinguished with platform instances.
- This is not aware of any tags/terms/owners that are added by dbt meta mapping configs instead of the meta.datahub section.
- If the dbt docs uses "docs blocks" (e.g. the `doc('model_name')` macro), we cannot sync back documentation edits.
- Does not support custom ownership types.

- Only supports GitHub.
- If any git conflicts occur, the end user is responsible for resolving them.
- If we are unable to generate a dbt commit based on a change, the change will be logged and ignored.

- Because the sync is driven by _changes_ in DataHub, any existing metadata changes made in DataHub will not be synced.
