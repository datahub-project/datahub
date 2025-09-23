# Contributing

We always welcome contributions to help make DataHub better. Take a moment to read this document if you would like to contribute.

## Provide Feedback

Have ideas about how to make DataHub better? Head over to [DataHub Feature Requests](https://feature-requests.datahubproject.io/) and tell us all about it!

Show your support for other requests by upvoting; stay up to date on progress by subscribing for updates via email.

## Reporting Issues

We use GitHub issues to track bug reports and submit pull requests.

If you find a bug:

1. Use the GitHub issue search to check whether the bug has already been reported.

1. If the issue has been fixed, try to reproduce the issue using the latest master branch of the repository.

1. If the issue still reproduces or has not yet been reported, try to isolate the problem before opening an issue.

## Submitting a Request For Comment (RFC)

If you have a substantial feature or a design discussion that you'd like to have with the community, follow the RFC process outlined [here](./rfc.md).

## Submitting a Pull Request (PR)

Before you submit your Pull Request (PR), consider the following guidelines:

- Search GitHub for an open or closed PR that relates to your submission. You don't want to duplicate effort.
- Open a pull request (PR) following [GitHub’s standard workflow](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).
- Please make sure to follow our [PR Title Format](#pr-title-format) for clarity and consistency.
- PRs are squashed and merged, resulting in a single commit with the PR title as the commit message.
- If there are any breaking changes, potential downtime, deprecations, or big features, please add an update in [Updating DataHub under Next](how/updating-datahub.md).
- That's it! Thank you for your contribution!

### PR Title Format

```
<type>[optional scope]: <description>
```

Example:

```
feat(parser): add ability to parse arrays
```

#### Type

Must be one of the following:

- _feat_: A new feature
- _fix_: A bug fix
- _refactor_: A code change that neither fixes a bug nor adds a feature
- _docs_: Documentation only changes
- _test_: Adding missing tests or correcting existing tests
- _perf_: A code change that improves performance
- _style_: Changes that do not affect the meaning of the code (whitespace, formatting, missing semicolons, etc.)
- _build_: Changes that affect the build system or external dependencies
- _ci_: Changes to our CI configuration files and scripts
