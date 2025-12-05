# DataHub RFC Process

## What is an RFC?

The "RFC" (request for comments) process is intended to provide a consistent and controlled path for new features,
significant modifications, or any other significant proposal to enter DataHub and its related frameworks.

Many changes, including bug fixes and documentation improvements can be implemented and reviewed via the normal GitHub
pull request workflow.

Some changes though are "substantial", and we ask that these be put through a bit of a design process and produce a
consensus among the DataHub core teams.

## The RFC life-cycle

An RFC goes through the following stages:

- _Discussion_ (Optional): Create an issue with the "RFC" label to have a more open ended, initial discussion around
  your proposal (useful if you don't have a concrete proposal yet). Consider posting to #contribute-code in [Slack](./slack.md)
  for more visibility.
- _Pending_: when the RFC is submitted as a PR. Please add the "RFC" label to the PR.
- _Active_: when an RFC PR is merged and undergoing implementation.
- _Landed_: when an RFC's proposed changes are shipped in an actual release.
- _Rejected_: when an RFC PR is closed without being merged.

[Pending RFC List](https://github.com/datahub-project/datahub/pulls?q=is%3Apr+is%3Aopen+label%3ARFC)

## When to follow this process

You need to follow this process if you intend to make "substantial" changes to any components in the DataHub git repo,
their documentation, or any other projects under the purview of the DataHub core teams. What constitutes a "substantial"
change is evolving based on community norms, but may include the following:

- A new feature that creates new API surface area, and would require a feature flag if introduced.
- The removal of features that already shipped as part of the release channel.
- The introduction of new idiomatic usage or conventions, even if they do not include code changes to DataHub itself.

Some changes do not require an RFC:

- Rephrasing, reorganizing or refactoring
- Addition or removal of warnings
- Additions that strictly improve objective, numerical quality criteria (speedup)

If you submit a pull request to implement a new, major feature without going through the RFC process, it may be closed
with a polite request to submit an RFC first.

## Gathering feedback before submitting

It's often helpful to get feedback on your concept before diving into the level of API design detail required for an
RFC. You may open an issue on this repo to start a high-level discussion, with the goal of eventually formulating an RFC
pull request with the specific implementation design. We also highly recommend sharing drafts of RFCs in #contribute-code on the
[DataHub Slack](./slack.md) for early feedback.

## The process

In short, to get a major feature added to DataHub, one must first get the RFC merged into the main DataHub repository as a markdown
file. At that point the RFC is 'active' and may be implemented with the goal of eventual inclusion into DataHub.

- Fork the [datahub-project/datahub repository](https://github.com/datahub-project/datahub).
- Copy the `template.md` file from `docs/rfcs/` to `docs/rfcs/active/000-my-feature.md`, where `my-feature` is more
  descriptive. Don't assign an RFC number yet.
- Fill in the RFC. Put care into the details. _RFCs that do not present convincing motivation, demonstrate understanding
  of the impact of the design, or are disingenuous about the drawback or alternatives tend to be poorly-received._
- Submit a pull request with the "RFC" label. As a pull request the RFC will receive design feedback from the larger community, and the
  author should be prepared to revise it in response.
- Update the pull request to add the number of the PR to the filename and add a link to the PR in the header of the RFC.
- Build consensus and integrate feedback. RFCs that have broad support are much more likely to make progress than those
  that don't receive any comments.
- Eventually, the DataHub team will decide whether the RFC is a candidate for inclusion.
- RFCs that are candidates for inclusion will enter a "final comment period" lasting 7 days. The beginning of this
  period will be signaled with a comment and tag on the pull request. Furthermore, an announcement will be made in the
  \#contribute-code Slack channel for further visibility.
- An RFC can be modified based upon feedback from the DataHub team and community. Significant modifications may trigger
  a new final comment period.
- An RFC may be rejected by the DataHub team after public discussion has settled and comments have been made summarizing
  the rationale for rejection. The RFC will enter a "final comment period to close" lasting 7 days. At the end of the "FCP
  to close" period, the PR will be closed.
- An RFC author may withdraw their own RFC by closing it themselves. Please state the reason for the withdrawal.
- An RFC may be accepted at the close of its final comment period. A DataHub team member will merge the RFC's associated
  pull request, at which point the RFC will become 'active'.

## Details on Active RFCs

Once an RFC becomes active then authors may implement it and submit the feature as a pull request to the DataHub repo.
Becoming 'active' is not a rubber stamp, and in particular still does not mean the feature will ultimately be merged; it
does mean that the core team has agreed to it in principle and are amenable to merging it.

Furthermore, the fact that a given RFC has been accepted and is 'active' implies nothing about what priority is assigned
to its implementation, nor whether anybody is currently working on it.

Modifications to active RFC's can be done in followup PR's. We strive to write each RFC in a manner that it will reflect
the final design of the feature; but the nature of the process means that we cannot expect every merged RFC to actually
reflect what the end result will be at the time of the next major release; therefore we try to keep each RFC document
somewhat in sync with the language feature as planned, tracking such changes via followup pull requests to the document.

## Implementing an RFC

The author of an RFC is not obligated to implement it. Of course, the RFC author (like any other developer) is welcome
to post an implementation for review after the RFC has been accepted.

An active RFC should have the link to the implementation PR(s) listed, if there are any. Feedback to the actual
implementation should be conducted in the implementation PR instead of the original RFC PR.

If you are interested in working on the implementation for an 'active' RFC, but cannot determine if someone else is
already working on it, feel free to ask (e.g. by leaving a comment on the associated issue).

## Implemented RFCs

Once an RFC has finally been implemented, first off, congratulations! And thank you for your contribution! Second, to
help track the status of the RFC, please make one final PR to move the RFC from `docs/rfcs/active` to
`docs/rfcs/accepted`.

## Reviewing RFCs

Most of the DataHub team will attempt to review some set of open RFC pull requests on a regular basis. If a DataHub
team member believes an RFC PR is ready to be accepted into active status, they can approve the PR using GitHub's
review feature to signal their approval of the RFCs.

_DataHub's RFC process is inspired by many others, including [Vue.js](https://github.com/vuejs/rfcs) and
[Ember](https://github.com/emberjs/rfcs)._
