---
description: "Use the acryl-datahub-cloud evals command to manage, run, and report DataHub Context Evals from the terminal or from CI."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# acryl-datahub-cloud evals

<FeatureAvailability saasOnly stage="private-beta" />

The `evals` command manages **Context Evals** — question-and-answer checks that measure whether
DataHub answers questions about your metadata correctly. An eval pairs a question with one or more
pass conditions; running it records a verdict you can track over time or gate a CI job on.

Context Evals are in private beta, so the command surface can change between releases.

## Installation

The command lives in the `acryl-datahub-cloud` package only — it is not part of the open source
`acryl-datahub` CLI. Install that package and run the command from it:

```shell
pip install 'acryl-datahub-cloud[datahub-evals]>=2.1.4rc1'
```

The package requires Python 3.10 or later.

The `datahub-evals` extra pulls in the GraphQL client the command needs, so installing the package
without it leaves `evals` unusable. The version floor matters too: `evals` first shipped in
2.1.4rc1, and because that is a pre-release, pip only considers it when the requirement names it
explicitly as above.

Install it into an isolated environment — a virtualenv, or `pipx install` — rather than alongside an
existing DataHub CLI. `acryl-datahub-cloud` depends on a specific `acryl-datahub` version and will
install or change it, so a shared environment can end up with a different `datahub` CLI than the one
you had.

Every command below is invoked through the package's own entry point:

```shell
acryl-datahub-cloud evals --help
```

Connection details come from the same place as the rest of the DataHub CLI — `~/.datahubenv` written
by `datahub init`, or the `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` environment variables.

## Quick Start

```shell
# Create or update evals from a definition file
acryl-datahub-cloud evals upsert -f evals.yml

# List what exists
acryl-datahub-cloud evals list

# Run one eval and wait for the verdict
acryl-datahub-cloud evals run urn:li:eval:my-eval

# Run everything for one agent and fail the shell on any failed eval
acryl-datahub-cloud evals run --all --agent-urn urn:li:aiAgent:my-agent --fail-on-fail
```

`upsert` is the default subcommand, so `acryl-datahub-cloud evals -f evals.yml` is equivalent to
`acryl-datahub-cloud evals upsert -f evals.yml`.

## Defining Evals

`upsert` reads a YAML or JSON file containing either one eval object or a list of them. An entry
with a `urn` updates that eval; an entry without one creates a new eval.

```yaml
- name: Revenue table owner
  description: Ask DataHub who owns the revenue table.
  question: Who owns the revenue table?
  evalType: METADATA
  origin: CONTEXT_HUB
  executor:
    type: NATIVE
  conditions:
    - type: LLM_JUDGE
      llmJudge:
        guidelines: The answer names the owning team and links the dataset.
    - type: ASSET_REFERENCE
      assetReference:
        mustReference:
          - urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.revenue,PROD)
        mustNotReference:
          - urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.revenue_staging,PROD)
```

### Eval fields

| Field             | Required on create | Description                                               |
| ----------------- | ------------------ | --------------------------------------------------------- |
| `name`            | Yes                | Display name. Must be a non-empty string.                 |
| `question`        | Yes                | The question put to DataHub.                              |
| `conditions`      | Yes                | At least one pass condition. See below.                   |
| `executor`        | Yes                | Who produces the answer. See below.                       |
| `origin`          | Yes                | `CONTEXT_HUB` or `CUSTOM_AGENT`. Set once at create time. |
| `description`     | No                 | Free text describing what the eval covers.                |
| `evalType`        | No                 | `METADATA` or `SQL`.                                      |
| `referenceOutput` | No                 | An example of a good answer, used as judging context.     |
| `agentUrn`        | No                 | The agent the eval belongs to. Set once at create time.   |
| `urn`             | —                  | Present means update. Omit to create.                     |

`origin` and `agentUrn` are fixed at creation: they are accepted in an update entry but ignored, so
an exported definition can be re-applied unchanged.

### Conditions

Each condition is either an LLM judgement or an assertion about which assets the answer cites, both
shown in the example above. A condition carries one shape or the other, never both.

| `type`            | Required fields                                                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `LLM_JUDGE`       | `llmJudge.guidelines` — non-empty text describing what a correct answer must contain.                                                |
| `ASSET_REFERENCE` | `assetReference.mustReference` and/or `assetReference.mustNotReference` — lists of DataHub URNs, with at least one URN between them. |

### Executor

| Field            | Required | Meaning                                                                                                                                   |
| ---------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `type`           | Yes      | `NATIVE` — DataHub answers the question itself. `EXTERNAL` — your own harness answers it and submits the result with [`report`](#report). |
| `externalClient` | No       | Label identifying the harness behind an `EXTERNAL` executor.                                                                              |

## Commands

### list

List eval definitions.

```shell
acryl-datahub-cloud evals list
acryl-datahub-cloud evals list --agent-urn urn:li:aiAgent:my-agent --eval-type METADATA
acryl-datahub-cloud evals list --start 20 --limit 20
```

| Option              | Default | Description                                      |
| ------------------- | ------- | ------------------------------------------------ |
| `--agent-urn`       | —       | Only evals for this agent.                       |
| `--base-agent-only` | —       | Only evals for the Ask DataHub base agent.       |
| `--eval-type`       | —       | `METADATA` or `SQL`.                             |
| `--eval-executor`   | —       | `NATIVE` or `EXTERNAL`.                          |
| `--query`           | —       | Free-text search over evals.                     |
| `--start`           | `0`     | Zero-based offset for the page. Also `--offset`. |
| `--limit`           | `10`    | Number of evals to return.                       |
| `--format`          | auto    | `json` or `table`.                               |

`--agent-urn` and `--base-agent-only` are mutually exclusive.

### get

Fetch one eval and its latest run summary.

```shell
acryl-datahub-cloud evals get urn:li:eval:my-eval
acryl-datahub-cloud evals get urn:li:eval:my-eval --history 5
acryl-datahub-cloud evals get urn:li:eval:my-eval --format upsert > my-eval.yml
```

| Option      | Default | Description                          |
| ----------- | ------- | ------------------------------------ |
| `--history` | —       | Include this many recent run events. |
| `--format`  | auto    | `json`, `table`, or `upsert`.        |

`--format upsert` prints the definition without read-only run fields, so the output can be edited and
fed straight back to `upsert`.

### upsert

Create or update evals from a file.

```shell
acryl-datahub-cloud evals upsert -f evals.yml
acryl-datahub-cloud evals upsert -f evals.yml --dry-run
```

| Option         | Description                                                     |
| -------------- | --------------------------------------------------------------- |
| `-f`, `--file` | Required. Path to the YAML or JSON definition file.             |
| `--dry-run`    | Validate and report create-vs-update per entry without writing. |

If one entry in a multi-eval file fails, the CLI prints the entries that already succeeded with their
URNs, then exits non-zero. Reuse those URNs when retrying — a repeated create makes a duplicate eval.

### delete

```shell
acryl-datahub-cloud evals delete urn:li:eval:my-eval
```

### run

Start a run and wait for every selected eval to reach a terminal event.

```shell
acryl-datahub-cloud evals run urn:li:eval:my-eval urn:li:eval:my-other-eval
acryl-datahub-cloud evals run --all --agent-urn urn:li:aiAgent:my-agent
acryl-datahub-cloud evals run --all --wait 600 --fail-on-fail
```

| Option            | Default | Description                                                                                                      |
| ----------------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `--all`           | —       | Run every matching eval instead of listed URNs.                                                                  |
| `--agent-urn`     | —       | Agent to run against. Selects the evals for `--all`, and is recorded with the run when explicit URNs are passed. |
| `--eval-executor` | —       | Filter `--all` by executor type. Requires `--all`; `EXTERNAL` is rejected, see below.                            |
| `--page-size`     | `10`    | Page size used to collect evals for `--all`. Also `--limit`.                                                     |
| `--wait`          | `300`   | Seconds to wait for terminal events.                                                                             |
| `--fail-on-fail`  | —       | Exit `1` on a failed run. See [Run failure contract](#run-failure-contract).                                     |
| `--format`        | auto    | `json` or `table`.                                                                                               |
| `--dry-run`       | —       | Show what would run without starting anything.                                                                   |

Pass either URNs or `--all`, not both.

#### Run failure contract

With `--fail-on-fail`, the command exits `1` when any selected eval ends in one of four states:

| State          | Meaning                                                  |
| -------------- | -------------------------------------------------------- |
| **FAIL**       | The answer did not satisfy the eval's conditions.        |
| **ERROR**      | The run errored instead of producing a verdict.          |
| **Stale**      | A queued or running event is older than 15 minutes.      |
| **Incomplete** | A target never reached a terminal event within `--wait`. |

Without `--fail-on-fail` the command exits `0` in all four cases, so CI jobs that must catch
regressions should always pass the flag.

A single run selects at most 1000 evals, matching the executor's per-run limit. A larger `--all`
selection is refused before anything is queued rather than failing after the run has started.

`--eval-executor EXTERNAL` is rejected: starting a run always queues native execution, which would
answer the eval itself and race your harness reporting back under the same run ID. Run those evals in
your own harness and submit the answers with `report`.

Note that `--all` without `--agent-urn` covers only the Ask DataHub base agent, whereas `list` without
a filter spans every agent, so the two can return different counts.

### report

Submit an answer produced outside DataHub, using the run ID returned by `run`.

```shell
acryl-datahub-cloud evals report urn:li:eval:my-eval \
  --run-id "$RUN_ID" \
  --answer "The revenue table is owned by the analytics team."

# Read a long answer from stdin
my-harness answer | acryl-datahub-cloud evals report urn:li:eval:my-eval --run-id "$RUN_ID" --answer -
```

| Option                | Description                                                              |
| --------------------- | ------------------------------------------------------------------------ |
| `--answer`            | Required. Answer text, or `-` to read stdin.                             |
| `--run-id`            | Required. Run ID from `run`.                                             |
| `--agent-model`       | Model that produced the answer.                                          |
| `--external-client`   | Identifier for the harness submitting the report.                        |
| `--session-id`        | Session identifier from your harness.                                    |
| `--type`              | `PASS`, `FAIL`, or `ERROR`. Supply only when reporting your own verdict. |
| `--condition-results` | JSON list of per-condition verdicts. Requires `--type`.                  |
| `--judge-model`       | Model that judged the answer. Requires `--type`.                         |
| `--judge-reasoning`   | Why the judge reached that verdict. Requires `--type`.                   |
| `--error`             | Error detail for an `ERROR` result. Requires `--type`.                   |
| `--dry-run`           | Validate the report without writing.                                     |

Omit `--type` and the verdict fields to let DataHub judge the submitted answer. Reports are recorded
with `executorType=EXTERNAL` and confirmed by reading the stored run event back: a server-judged
report is accepted once its matching queued or running event is visible, while a caller-supplied
verdict waits for the stored complete event. Reporting twice against a run ID that is already
terminal is deduplicated rather than duplicated.

### history

List run events for one eval.

```shell
acryl-datahub-cloud evals history urn:li:eval:my-eval
acryl-datahub-cloud evals history urn:li:eval:my-eval --limit 50
```

| Option                    | Default | Description                                 |
| ------------------------- | ------- | ------------------------------------------- |
| `--include-proposal-runs` | —       | Include runs triggered by change proposals. |
| `--action-request-urn`    | —       | Only runs for this change proposal.         |
| `--limit`                 | `10`    | Number of run events to return.             |
| `--format`                | auto    | `json` or `table`.                          |

`--include-proposal-runs` and `--action-request-urn` are mutually exclusive.

## Output and Exit Codes

For the commands that accept `--format` — `list`, `get`, `run`, and `history` — output defaults to a
table when stdout is a terminal and to JSON when it is not, so piping into `jq` works without extra
flags. Pass `--format` explicitly when you need one or the other regardless. `upsert`, `delete`, and
`report` always emit JSON, including on a terminal.

| Exit code | Meaning                                                                                                   |
| --------- | --------------------------------------------------------------------------------------------------------- |
| `0`       | Success.                                                                                                  |
| `1`       | Command failed, or `--fail-on-fail` saw a failed run — see [Run failure contract](#run-failure-contract). |
| `2`       | Invalid command input or an invalid eval definition.                                                      |
| `5`       | Could not reach DataHub, or the instance does not support evals.                                          |

Errors raised by the command itself — a bad eval definition, an unreachable instance, a failed run —
print as human-readable text on a terminal and as `{"error": ..., "message": ...}` on stderr
otherwise. Argument-parsing errors caught before the command runs, such as a non-numeric `--wait`,
stay plain-text usage messages on stderr and still exit `2`.

## Using the Command from an Agent

`acryl-datahub-cloud evals --agent-context` prints a condensed reference covering every subcommand, the exit-code
contract, and the run and report semantics. The same text is appended to `--help` output when stdout
is not a terminal, so a coding agent that shells out to `acryl-datahub-cloud evals --help` gets the full contract
without a separate call.

## CI Example

```shell
pip install 'acryl-datahub-cloud[datahub-evals]>=2.1.4rc1'

export DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
export DATAHUB_GMS_TOKEN=your-token

acryl-datahub-cloud evals upsert -f evals.yml
acryl-datahub-cloud evals run --all --agent-urn "$AGENT_URN" --wait 900 --fail-on-fail
```

The run exits non-zero on any failed eval, which fails the job — see the
[run failure contract](#run-failure-contract) for what counts.
