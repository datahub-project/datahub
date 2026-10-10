

# Workflow Tutorial

> **Availability:** DataHub Cloud only

> **Note**: Action Workflows is currently in **Private Beta**. To enable this feature, please reach out to the DataHub team.

This tutorial builds an Action Workflow one capability at a time. We start with the simplest workflow that does something useful — a promotion request that needs a single approval — and then add one feature at a time until we reach a full governance workflow: a Snowflake-only _Dataset Promotion to Certified_ process with context-aware form pickers, dynamic approvers, mixed quorum, and a conditional legal-review step.

**If you just need a working workflow, [Part 1](#part-1--your-first-workflow) is enough.** [Part 2](#part-2--growing-the-workflow) layers on the advanced features so you can adopt only the ones your process needs.

For the conceptual introduction, see [Action Workflows](action-workflows.md). For the exhaustive JSON syntax of every primitive, see the [Workflow Reference](action-workflows-reference.md). Throughout, the _launching entity_ is the entity from whose profile page the workflow was launched — for example, the dataset on which the requester clicked the launch icon.

## Part 1 · Your First Workflow

Every workflow definition has three essential parts:

- a **trigger** that says where the launch button appears and what the requester fills in,
- one or more **steps** that route the request for approval, and
- a **category**.

Here is the smallest useful workflow: it adds a **Promote** button to every dataset, collects a justification, and routes the request to the dataset's owners.

```json
{
  "name": "Promote Dataset",
  "category": "CUSTOM",
  "customCategory": "GOVERNANCE",
  "trigger": {
    "type": "FORM_SUBMITTED",
    "form": {
      "entityTypes": ["DATASET"],
      "entrypoints": [{ "type": "ENTITY_PROFILE", "label": "Promote" }],
      "fields": [
        {
          "id": "justification",
          "name": "Why should this dataset be promoted?",
          "valueType": "RICH_TEXT",
          "cardinality": "SINGLE",
          "required": true
        }
      ]
    }
  },
  "steps": [
    {
      "id": "data-owner-approval",
      "type": "APPROVAL",
      "actors": {
        "dynamicSource": {
          "resolvers": [{ "resolver": "OWNERS_OF", "source": "launching" }]
        }
      }
    }
  ]
}
```

This is a complete, valid definition. Submit it as the `input` argument to the `upsertActionWorkflow` mutation (see [How to Apply a Workflow Definition](action-workflows-reference.md#how-to-apply-a-workflow-definition)) and the loop is live: opening any dataset shows a **Promote** button; clicking it opens a form with the justification field; on submit, the request routes to the launching dataset's owners, and — because no `quorum` is specified, the default is `anyOf` — any one owner approving completes the workflow.

We omitted the `urn`, so DataHub creates a new workflow and assigns one. Everything in Part 2 refines these same three parts of the definition.

## Part 2 · Growing the Workflow

Part 1 works, but production governance needs more control. In this part we add one capability at a time to the same _Promote_ workflow. Each change is an edit to the JSON you already have; by the end you arrive at the complete definition in the [Appendix](#appendix--full-workflow-definition). The snippets below show only the part that changes.

> As you iterate, pin a `urn` on the definition (e.g. `"urn:li:actionWorkflow:dataset-promotion-certified"`) so that re-applying it updates the same workflow rather than creating a new one.

### Restrict where it launches

Right now the **Promote** button appears on every dataset. Scope it with an entrypoint `filter` — a visibility predicate the catalog evaluates against the launching entity before showing the icon. Here we require the launching dataset's `platform` to be Snowflake:

```json
{
  "type": "ENTITY_PROFILE",
  "label": "Promote to Certified",
  "filter": {
    "operator": "AND",
    "filters": [
      {
        "field": "platform",
        "values": ["urn:li:dataPlatform:snowflake"],
        "condition": "EQUAL"
      }
    ]
  }
}
```

The same filter dialect can match by tag, glossary term, domain, data product, owner, sub-type, or any combination — so an author can scope visibility as narrowly or broadly as the governance process requires. See the [Filter Dialect](action-workflows-reference.md#filter-dialect) section of the reference for the full set of supported fields and operators.

### Validate what the requester enters

The justification field currently accepts anything. Add a `validation` block — a regex plus a custom error rendered inline on submit. This one enforces at least 100 characters:

```json
{
  "id": "justification",
  "name": "Reason for certification",
  "description": "Minimum 100 characters.",
  "valueType": "RICH_TEXT",
  "cardinality": "SINGLE",
  "required": true,
  "validation": {
    "pattern": "[\\s\\S]{100,}",
    "errorMessage": "Please describe at least 100 characters explaining why this dataset is ready for certification."
  }
}
```

### Resolve dropdown options from context

Any URN-valued field can populate its dropdown by traversing the catalog graph instead of listing every entity. A `dynamicSource` declares a resolver and a starting point — the resolver's `source` — which can be the launching entity (`"launching"`) or a URN the requester picked earlier in the form (`"field:<form-field-id>"`). The traversal walks documented relationships (owners, domain, tags, glossary terms, data products, applications, container, members of); an optional `filter` narrows the final option set.

Add a co-signer picker scoped to `CORP_USER` that lists only the owners of the launching dataset's domain, rather than every catalog user:

```json
{
  "id": "co_signer_steward",
  "name": "Domain steward co-signer (nomination)",
  "description": "Picker resolves to domain owners of the launching dataset.",
  "valueType": "URN",
  "allowedEntityTypes": ["CORP_USER"],
  "cardinality": "SINGLE",
  "required": true,
  "dynamicSource": {
    "resolvers": [{ "resolver": "DOMAIN_OWNERS_OF", "source": "launching" }]
  }
}
```

The picker renders each resolved URN as a rich entity card (icon, type pill, display name), and the submit boundary validates that the requester picks a URN inside the resolved set.

### Offer a curated set of choices

For a fixed short list — for example "pick one of these three certification tiers" — pair the `ALL_ENTITIES` resolver (which enumerates entities scoped by the field's `allowedEntityTypes`) with a `filter` that narrows by `urn` to the chosen members. Multi-value `EQUAL` is an OR, so only URNs in the list pass:

```json
{
  "id": "target_tier",
  "name": "Target certification tier",
  "description": "Picker scoped to 3 certification glossary terms via ALL_ENTITIES + urn-EQUAL filter.",
  "valueType": "URN",
  "allowedEntityTypes": ["GLOSSARY_TERM"],
  "cardinality": "SINGLE",
  "required": true,
  "dynamicSource": {
    "resolvers": [{ "resolver": "ALL_ENTITIES", "source": "launching" }],
    "filter": {
      "operator": "AND",
      "filters": [
        {
          "field": "urn",
          "condition": "EQUAL",
          "values": [
            "urn:li:glossaryTerm:certification.bronze",
            "urn:li:glossaryTerm:certification.silver",
            "urn:li:glossaryTerm:certification.gold"
          ]
        }
      ]
    }
  }
}
```

### Show a field only when it's relevant

A field can declare a `filterCondition` that decides whether it renders. The condition is a boolean expression that can mix **form-field values** the requester entered (referenced via `formField:<id>`) and **attributes of the launching entity** the catalog already knows (tags, glossary terms, domain, platform, data product, owners, sub-type, name, description). A field reveals itself only when the combined condition holds.

Add a downgrade-reason field that appears only when the launching dataset is already `certification.gold` **and** the requester's chosen tier is not gold — i.e., a genuine downgrade:

```json
{
  "id": "downgrade_reason",
  "name": "Reason for downgrade",
  "description": "Visible only when the dataset currently has certification.gold AND the target tier is not gold.",
  "valueType": "RICH_TEXT",
  "cardinality": "SINGLE",
  "required": true,
  "filterCondition": {
    "operator": "AND",
    "filters": [
      {
        "field": "glossaryTerms",
        "values": ["urn:li:glossaryTerm:certification.gold"],
        "condition": "CONTAIN"
      },
      {
        "field": "formField:target_tier",
        "values": ["urn:li:glossaryTerm:certification.gold"],
        "condition": "EQUAL",
        "negated": true
      }
    ]
  }
}
```

### Route to dynamic approvers

Part 1's single step already routes to the dataset's owners. Steps use the same resolver dialect as dropdowns, so "the data owner of this dataset" and "the owners of the domain this dataset belongs to" are both expressible from the launching entity. Add a second step that routes to the owners of the dataset's domain:

```json
{
  "id": "domain-steward-approval",
  "type": "APPROVAL",
  "description": "ANY_OF: any owner of the dataset's domain can approve.",
  "actors": {
    "dynamicSource": {
      "resolvers": [{ "resolver": "DOMAIN_OWNERS_OF", "source": "launching" }]
    }
  },
  "quorum": { "anyOf": true }
}
```

### Require the right number of approvers

Each step declares a quorum policy: `anyOf: true` (any single approver suffices — the default), `allOf: true` (every assigned actor must approve), or `nofM: { n: <count> }` (a threshold count out of the assigned pool). Group and role slots are expanded transparently — any member of an assigned group can satisfy the slot — and the step's audit trail records which slot each decision satisfied.

Make owner approval unanimous by adding `"quorum": { "allOf": true }` to the `data-owner-approval` step, and add a final governance committee that requires any two of three named reviewers:

```json
{
  "id": "admin-signoff",
  "type": "APPROVAL",
  "description": "N_OF_M(2/3): any 2 of the 3 named reviewers must approve.",
  "actors": {
    "userUrns": [
      "urn:li:corpuser:reviewer-alpha",
      "urn:li:corpuser:reviewer-beta",
      "urn:li:corpuser:reviewer-gamma"
    ]
  },
  "quorum": { "nofM": { "n": 2 } }
}
```

### Run a step only when it's needed

A step can declare a `condition` that decides whether it opens for a given request. The condition reads form-field values and launching-entity attributes — tags, glossary terms, domain, platform, owners, sub-type — using the same expression dialect as entrypoint and field-visibility conditions. Steps whose condition evaluates to false are skipped automatically, and the audit log records the skip with the condition that decided it.

Add a legal-review step assigned to the `legal-data-stewards` group that fires only when the launching dataset carries the `pii` tag:

```json
{
  "id": "legal-review",
  "type": "APPROVAL",
  "description": "ANY_OF from legal-data-stewards group. Fires only when the dataset carries urn:li:tag:pii.",
  "actors": {
    "groupUrns": ["urn:li:corpGroup:legal-data-stewards"]
  },
  "quorum": { "anyOf": true },
  "condition": {
    "operator": "AND",
    "filters": [
      {
        "field": "tags",
        "values": ["urn:li:tag:pii"],
        "condition": "CONTAIN"
      }
    ]
  }
}
```

### The complete workflow

Assembled, the _Promote_ workflow from Part 1 has grown into a full _Dataset Promotion to Certified_ process:

```mermaid
flowchart LR
    A(["<b>Promote to Certified</b><br/><i>only Snowflake datasets</i>"]) --> B["<b>Submit certification proposal</b><br/><i>tier, justification, optional reason, co-signer</i>"]
    B --> C["<b>S1 · Data Owner approval</b><br/><i>every dataset owner must approve</i>"]
    C --> D["<b>S2 · Domain Steward sign-off</b><br/><i>any steward of the dataset's domain</i>"]
    D --> E{"<b>Dataset<br/>contains PII?</b>"}
    E -->|yes| F["<b>S3 · Privacy & Legal review</b><br/><i>any privacy / legal-data-steward</i>"]
    E -->|no| G["<b>S4 · Governance committee</b><br/><i>any 2 of 3 named admins</i>"]
    F --> G
    G --> H(["<b>Dataset marked Certified</b><br/><i>side-effect via a subscribed DataHub Action</i>"])
```

It now combines:

1. An **entrypoint** restricted to Snowflake datasets.
2. A **user form** with a length-validated justification, a co-signer nominated from the dataset's domain owners, a curated three-option tier picker, and a conditional downgrade-reason field.
3. **Steps** with dynamically-routed data-owner approval (`ALL_OF`), dynamically-routed domain-steward approval (`ANY_OF`), a legal-review step conditional on the launching dataset's PII tag, and an admin sign-off with `N_OF_M(2/3)` quorum.
4. **Lifecycle** support: full audit history with on-behalf-of attribution, requester self-cancellation, and admin override (below).
5. A **side-effect**: on the workflow's `COMPLETED` event, a subscribed DataHub Action flips the dataset's `lifecycleStage` structured property and applies a `certified-by-workflow` tag. The side-effect itself lives in the Actions framework and is out of scope here — see [Reacting to Workflow Events](action-workflows.md#reacting-to-workflow-events) for the integration pattern.

The full assembled JSON is in the [Appendix](#appendix--full-workflow-definition).

## Lifecycle, Attribution, Cancellation

Every state transition on a workflow request — submission, approval, rejection, skip, cancellation — appends an immutable entry to the request's audit history, with the actor URN, timestamp, comment, and (for approvals) the slot the decision counted toward. The UI renders each entry as a separate audit line and includes on-behalf-of attribution when a user acts as part of a group ("Alice, on behalf of the group `data-stewards`").

In-flight requests can be cancelled in two ways. The original requester can cancel their own request through the request profile; an administrator with `Manage Global Settings` can cancel any request as an override. Cancellation appends a `CANCELLED` decision to the history, flips the request's status, and notifies assigned approvers — the audit trail on the cancelled request remains complete.

## Submitting the Definition

The complete JSON definition below is directly accepted as the `input` argument to the `upsertActionWorkflow` GraphQL mutation. Customers typically maintain workflow definitions like this in their version-controlled configuration repository and apply them through one of:

- **DataHub Python SDK**: `graph.execute_graphql(query=UPSERT_MUTATION, variables={"input": <json>})` — see the [Action Workflows concept page](action-workflows.md#creating-an-action-workflow) for a complete example
- **Raw GraphQL** via curl, GraphiQL, or your tool of choice — point your client at `/api/graphql` and submit the mutation directly
- **`datahub` CLI** — wrap the JSON in a small Python or shell script that calls the SDK; there is no dedicated CLI subcommand for workflow upsert today

Every upsert produces a new immutable revision; in-flight workflow requests continue against the revision they were launched under, so a definition change does not retroactively alter approvals already in progress.

## Appendix — Full Workflow Definition

The complete `Dataset Promotion to Certified` workflow definition.

```json
{
  "urn": "urn:li:actionWorkflow:dataset-promotion-certified",
  "name": "Dataset Promotion to Certified",
  "description": "Promote a dataset's lifecycleStage to a higher certification tier. Restricted to Snowflake datasets. Combines context-aware form pickers, conditional fields, dynamic actor resolution, mixed quorum policies, and a conditional legal-review step that auto-fires on PII-tagged datasets.",
  "category": "CUSTOM",
  "customCategory": "GOVERNANCE",
  "trigger": {
    "type": "FORM_SUBMITTED",
    "form": {
      "entityTypes": ["DATASET"],
      "entrypoints": [
        {
          "type": "ENTITY_PROFILE",
          "label": "Promote to Certified",
          "filter": {
            "operator": "AND",
            "filters": [
              {
                "field": "platform",
                "values": ["urn:li:dataPlatform:snowflake"],
                "condition": "EQUAL"
              }
            ]
          }
        }
      ],
      "fields": [
        {
          "id": "target_tier",
          "name": "Target certification tier",
          "description": "Picker scoped to 3 certification glossary terms via ALL_ENTITIES + urn-EQUAL filter.",
          "valueType": "URN",
          "allowedEntityTypes": ["GLOSSARY_TERM"],
          "cardinality": "SINGLE",
          "required": true,
          "dynamicSource": {
            "resolvers": [
              { "resolver": "ALL_ENTITIES", "source": "launching" }
            ],
            "filter": {
              "operator": "AND",
              "filters": [
                {
                  "field": "urn",
                  "condition": "EQUAL",
                  "values": [
                    "urn:li:glossaryTerm:certification.bronze",
                    "urn:li:glossaryTerm:certification.silver",
                    "urn:li:glossaryTerm:certification.gold"
                  ]
                }
              ]
            }
          }
        },
        {
          "id": "justification",
          "name": "Reason for certification",
          "description": "Minimum 100 characters.",
          "valueType": "RICH_TEXT",
          "cardinality": "SINGLE",
          "required": true,
          "validation": {
            "pattern": "[\\s\\S]{100,}",
            "errorMessage": "Please describe at least 100 characters explaining why this dataset is ready for certification."
          }
        },
        {
          "id": "downgrade_reason",
          "name": "Reason for downgrade",
          "description": "Visible only when the dataset currently has certification.gold AND the target tier is not gold.",
          "valueType": "RICH_TEXT",
          "cardinality": "SINGLE",
          "required": true,
          "filterCondition": {
            "operator": "AND",
            "filters": [
              {
                "field": "glossaryTerms",
                "values": ["urn:li:glossaryTerm:certification.gold"],
                "condition": "CONTAIN"
              },
              {
                "field": "formField:target_tier",
                "values": ["urn:li:glossaryTerm:certification.gold"],
                "condition": "EQUAL",
                "negated": true
              }
            ]
          }
        },
        {
          "id": "co_signer_steward",
          "name": "Domain steward co-signer (nomination)",
          "description": "Picker resolves to domain owners of the launching dataset.",
          "valueType": "URN",
          "allowedEntityTypes": ["CORP_USER"],
          "cardinality": "SINGLE",
          "required": true,
          "dynamicSource": {
            "resolvers": [
              { "resolver": "DOMAIN_OWNERS_OF", "source": "launching" }
            ]
          }
        }
      ]
    }
  },
  "steps": [
    {
      "id": "data-owner-approval",
      "type": "APPROVAL",
      "description": "ALL_OF: every dataset owner must approve.",
      "actors": {
        "dynamicSource": {
          "resolvers": [{ "resolver": "OWNERS_OF", "source": "launching" }]
        }
      },
      "quorum": { "allOf": true }
    },
    {
      "id": "domain-steward-approval",
      "type": "APPROVAL",
      "description": "ANY_OF: any owner of the dataset's domain can approve.",
      "actors": {
        "dynamicSource": {
          "resolvers": [
            { "resolver": "DOMAIN_OWNERS_OF", "source": "launching" }
          ]
        }
      },
      "quorum": { "anyOf": true }
    },
    {
      "id": "legal-review",
      "type": "APPROVAL",
      "description": "ANY_OF from legal-data-stewards group. Fires only when the dataset carries urn:li:tag:pii.",
      "actors": {
        "groupUrns": ["urn:li:corpGroup:legal-data-stewards"]
      },
      "quorum": { "anyOf": true },
      "condition": {
        "operator": "AND",
        "filters": [
          {
            "field": "tags",
            "values": ["urn:li:tag:pii"],
            "condition": "CONTAIN"
          }
        ]
      }
    },
    {
      "id": "admin-signoff",
      "type": "APPROVAL",
      "description": "N_OF_M(2/3): any 2 of the 3 named reviewers must approve.",
      "actors": {
        "userUrns": [
          "urn:li:corpuser:reviewer-alpha",
          "urn:li:corpuser:reviewer-beta",
          "urn:li:corpuser:reviewer-gamma"
        ]
      },
      "quorum": { "nofM": { "n": 2 } }
    }
  ]
}
```
