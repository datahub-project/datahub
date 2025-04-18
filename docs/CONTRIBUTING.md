# Contributing

We always welcome contributions to help make DataHub better. Take a moment to read this document if you would like to contribute.

## Provide Feedback

Have ideas about how to make DataHub better? Head over to [DataHub Feature Requests](https://feature-requests.datahubproject.io/) and tell us all about it!

Show your support for other requests by upvoting; stay up to date on progess by subscribing for updates via email.

## Reporting Issues

We use GitHub issues to track bug reports and submitting pull requests.

If you find a bug:

1. Use the GitHub issue search to check whether the bug has already been reported.

1. If the issue has been fixed, try to reproduce the issue using the latest master branch of the repository.

1. If the issue still reproduces or has not yet been reported, try to isolate the problem before opening an issue.

## Submitting a Request For Comment (RFC)

If you have a substantial feature or a design discussion that you'd like to have with the community follow the RFC process outlined [here](./rfc.md)

## Submitting a Pull Request (PR)

Before you submit your Pull Request (PR), consider the following guidelines:

* Search GitHub for an open or closed PR that relates to your submission. You don't want to duplicate effort.
* Follow the [standard GitHub approach](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to create the PR. Please also follow our [commit message format](#commit-message-format).
* If there are any breaking changes, potential downtime, deprecations, or big feature please add an update in [Updating DataHub under Next](how/updating-datahub.md).
* That's it! Thank you for your contribution!

## Commit Message Format

Please follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for the commit message format. In summary, each commit message consists of a *header*, a *body* and a *footer*, separated by a single blank line.

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Any line of the commit message cannot be longer than 88 characters! This allows the message to be easier to read on GitHub as well as in various Git tools.

### Type

Must be one of the following (based on the [Angular convention](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines)):

* *feat*: A new feature
* *fix*: A bug fix
* *refactor*: A code change that neither fixes a bug nor adds a feature
* *docs*: Documentation only changes
* *test*: Adding missing tests or correcting existing tests
* *perf*: A code change that improves performance
* *style*: Changes that do not affect the meaning of the code (whitespace, formatting, missing semicolons, etc.)
* *build*: Changes that affect the build system or external dependencies
* *ci*: Changes to our CI configuration files and scripts

A scope may be provided to a commit’s type, to provide additional contextual information and is contained within parenthesis, e.g., 
```
feat(parser): add ability to parse arrays
```

### Description

Each commit must contain a succinct description of the change:

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize the first letter
* no dot(.) at the end

### Body

Just as in the description, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.

### Footer

The footer should contain any information about *Breaking Changes*, and is also the place to reference GitHub issues that this commit *Closes*.

*Breaking Changes* should start with the words `BREAKING CHANGE:` with a space or two new lines. The rest of the commit message is then used for this.

### Revert

If the commit reverts a previous commit, it should begin with `revert:`, followed by the description. In the body it should say: `Refs: <hash1> <hash2> ...`, where the hashs are the SHA of the commits being reverted, e.g. 

```
revert: let us never again speak of the noodle incident

Refs: 676104e, a215868
```
