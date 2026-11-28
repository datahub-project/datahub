
# Set Attribution

By default, an ingestion run writes tags, owners, and glossary terms as a plain overwrite: whatever the source produces becomes the entire value of the aspect. Anything already there is replaced — including terms a user added in the UI and tags applied by a different pipeline.

The `set_attribution` transformer changes that. It records the metadata your pipeline produces as asserted by a specific **attribution source**, and scopes the write to that source. Re-running the pipeline updates only its own assertions; everything attributed to other sources — UI edits, other pipelines, automations — is left untouched.

Attribution also makes provenance visible: each tag, owner, and term it writes carries who applied it, when, and optionally why.

## Supported aspects

| Aspect          | What gets attributed |
| --------------- | -------------------- |
| `globalTags`    | Each tag             |
| `ownership`     | Each owner           |
| `glossaryTerms` | Each glossary term   |

This transformer applies to every entity type. Aspects other than the three above are forwarded untouched.

:::warning Sources that emit MCEs lose their other aspects

If your source emits `MetadataChangeEvent`s (MCEs), which carry several aspects per entity in one snapshot, only the three aspects above are forwarded — anything else in that MCE, such as a description or custom properties, is dropped. Sources that emit `MetadataChangeProposal`s (MCPs), which carry a single aspect each, are unaffected. See [MetadataChangeProposal & MetadataChangeLog](/docs/advanced/mcp-mcl.md) for the distinction.

If you are unsure which your source emits, run the pipeline into a `file` sink with and without this transformer and compare which aspects appear.
:::

## Config Details

| Field                | Required | Type                | Default                   | Description                                                                                                                     |
| -------------------- | -------- | ------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `attribution_source` | ✅       | string              |                           | The source these assertions are attributed to. Must be a URN starting with `urn:li:`, e.g. `urn:li:platformResource:ingestion`. |
| `actor`              |          | string              | `urn:li:corpuser:datahub` | The actor recorded on each assertion. Must be a URN starting with `urn:li:`.                                                    |
| `source_detail`      |          | map[string, string] |                           | Free-form details recorded alongside each assertion — for example the pipeline ID or version that produced it.                  |
| `patch_mode`         |          | boolean             | `false`                   | Whether a run replaces this source's assertions (`false`) or adds to them (`true`). See [Choosing a mode](#choosing-a-mode).    |

## How attribution scoping works

Think of each supported aspect as divided into one slot per attribution source. A tag applied in the UI sits in one slot, a tag applied by an automation sits in another, and the tags your pipeline emits sit in the slot named by `attribution_source`.

This transformer only ever writes to its own slot. That has two consequences worth understanding:

- Your pipeline can no longer delete metadata it did not apply. Manual curation survives re-ingestion.
- Conversely, other sources cannot delete yours. If two sources assert the same tag, both assertions are recorded, and the tag remains as long as at least one source still asserts it.

### Choosing a mode

`patch_mode` controls what happens to the assertions your own source made on previous runs. Neither mode ever affects another source.

**`patch_mode: false` (default) — the run is the full picture.**

Whatever the run produces becomes the complete set of tags, owners, and terms for this attribution source. Anything this source asserted previously that is absent from the current run is removed.

Use this when the pipeline is the authority for its source and removals should propagate — for example, when a table loses a tag upstream and you want that reflected in DataHub.

**`patch_mode: true` — the run is an addition.**

Each item is added to this source's existing set. Items this source asserted previously that are absent from the current run are retained.

Use this for partial or incremental runs, where the absence of a tag means "this run didn't look at it" rather than "this tag is gone" — for example when you ingest one schema at a time, or backfill in batches.

## Examples

:::warning List this transformer after the ones it attributes

Any transformer that writes tags, owners, or terms **after** `set_attribution` emits a plain overwrite of that aspect, and that overwrite lands on top of the scoped write. It discards the attribution and removes the tags, owners, or terms contributed by every other source — the exact outcome this transformer exists to prevent.

So `set_attribution` must come after every transformer whose output you want attributed.
:::

Attribute everything the pipeline produces to an ingestion source, and let removals propagate:

```yaml
transformers:
  - type: "set_attribution"
    config:
      attribution_source: "urn:li:platformResource:ingestion"
```

Add to this source's assertions without removing anything it applied earlier:

```yaml
transformers:
  - type: "set_attribution"
    config:
      attribution_source: "urn:li:platformResource:ingestion"
      patch_mode: true
```

Record which pipeline produced the metadata, and attribute it to a service account rather than the default actor:

```yaml
transformers:
  - type: "set_attribution"
    config:
      attribution_source: "urn:li:platformResource:ingestion"
      actor: "urn:li:corpuser:etl-service"
      source_detail:
        pipeline_id: "snowflake-prod-daily"
        version: "1.0"
```


  **What this sends to DataHub**

Instead of overwriting the aspect, the transformer emits a scoped patch. The examples below show what a source emitting the tags `tagA` and `tagB` produces under each mode.

With `patch_mode: false`, a single operation replaces the whole slot, so any tag this source previously asserted and no longer emits disappears:

```json
{
  "arrayPrimaryKeys": { "tags": ["attribution␟source", "tag"] },
  "patch": [
    {
      "op": "add",
      "path": "/tags/urn:li:platformResource:ingestion",
      "value": {
        "urn:li:tag:tagA": {
          "tag": "urn:li:tag:tagA",
          "attribution": {
            "time": 1758499200000,
            "actor": "urn:li:corpuser:datahub",
            "source": "urn:li:platformResource:ingestion"
          }
        },
        "urn:li:tag:tagB": {
          "tag": "urn:li:tag:tagB",
          "attribution": {
            "time": 1758499200000,
            "actor": "urn:li:corpuser:datahub",
            "source": "urn:li:platformResource:ingestion"
          }
        }
      }
    }
  ],
  "forceGenericPatch": true
}
```

With `patch_mode: true`, each tag is added individually, leaving the slot's other entries in place:

```json
{
  "arrayPrimaryKeys": { "tags": ["attribution␟source", "tag"] },
  "patch": [
    {
      "op": "add",
      "path": "/tags/urn:li:platformResource:ingestion/urn:li:tag:tagA",
      "value": {
        "tag": "urn:li:tag:tagA",
        "attribution": {
          "time": 1758499200000,
          "actor": "urn:li:corpuser:datahub",
          "source": "urn:li:platformResource:ingestion"
        }
      }
    },
    {
      "op": "add",
      "path": "/tags/urn:li:platformResource:ingestion/urn:li:tag:tagB",
      "value": {
        "tag": "urn:li:tag:tagB",
        "attribution": {
          "time": 1758499200000,
          "actor": "urn:li:corpuser:datahub",
          "source": "urn:li:platformResource:ingestion"
        }
      }
    }
  ],
  "forceGenericPatch": true
}
```

`ownership` and `glossaryTerms` follow the same shape, keyed on `owners`/`owner` and `terms`/`urn` respectively. For the patch mechanism itself, see [Generic Patching](/docs/api/openapi/openapi-usage-guide.md#generic-patching).



## Behavior notes

- **In the default mode, omitting metadata removes it.** With `patch_mode: false`, if a run emits no tags for an entity, every tag this source previously applied to that entity is cleared. This is the intended way to propagate removals, but it means an incomplete run can wipe your own source's metadata — use `patch_mode: true` for partial runs, where the same situation is a no-op.
- **Attribution is replaced on the metadata it attributes.** If an incoming tag, owner, or term already carries attribution and falls within this transformer's scope, that attribution is overwritten with the configured `attribution_source` and `actor`.
- **Metadata already scoped to another source is left alone, silently.** Tags, owners, and terms that arrive already attributed to a different source keep that attribution — they are not re-attributed to your configured source, and nothing is reported when this happens. If metadata you expected to own is missing attribution, check whether something upstream is already attributing it elsewhere.
- **Records it cannot interpret are forwarded unchanged** rather than failing the run, with a warning in the ingestion log. Not every skip is reported, though — the case above is silent — so the log alone is not proof that everything was attributed.
