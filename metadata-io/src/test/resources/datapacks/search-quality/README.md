"""Cranfield plus the longtail-ecommerce catalog, scored by separate judgments.

This directory is a normal multi-file datapack. `index.json` lists only MCP shards.
`judgments.json` is test metadata and is not ingested by `datahub datapack load`.
`03-longtail-ecommerce.json` is the companions catalog followed by the ecommerce
catalog. `04-longtail-ecommerce-timeseries.json` is the profile shard. Search tests
load this datapack and do not read Elasticsearch index snapshots.

Regenerate judgments, Cranfield, and the probes with:

```bash
python3 metadata-ingestion/tests/unit/search_quality/generate_search_quality_pack.py
```

The generator leaves both longtail-ecommerce shards in place, and it leaves
`08-stage2-default.json` in place. That shard is pack data and is not rebuilt.
`stage2-default` grades live in `judgments.cloud.json` and are not part of the OSS
backport patch. The ecommerce entities
were normalized from `datahub-project/static-assets` `datapacks/showcase-ecommerce`
at commit `a4228e9f9e9c6ca4ccb453528448fca07170ce88` and are not rebuilt from that
snapshot. Provenance is in `source/showcase-ecommerce/provenance.json`. The profile
shard is not rebuilt.

Cranfield (Cleverdon, 1400 abstracts, 225 queries, graded qrels) is adapted for internal
search testing. Each abstract is one dataset: the title is the name and the abstract is
the description. Author and bibliography are not searchable fields.

Two quirks of the source files decide whether the benchmark measures anything at all:

- `cran.qry` labels its queries with the original Cranfield numbers, which skip values and
  reach 365, but `cranqrel` numbers the same 225 queries 1..225 by their order in that
  file. Position is the only valid join key. Joining on the label scores most queries
  against another query's documents and leaves the rest with no grades.
- `cranqrel` grades run the other way from a gain scale: 1 is a complete answer, 4 is of
  minimum interest, and -1 is of no interest. The generator flips them so that a larger
  `relevance` in `judgments.json` always means more relevant.

The `longtail-ecommerce` queries use the same gain scale. A 4 is the asset whose name, title, or
description is the query. A 3 is that asset on another platform or a close variant. A 2 is a
related asset a user would still open. A 1 is a neighbor. `longtailEcommerceNdcgAt10` is the mean
nDCG@10 of those queries on the template path. `javaBuilderLongtailEcommerceNdcgAt10` is the same
mean with template execution skipped. The `keyword-probe` queries are pass/fail constraints on the
template path: top hit, order, presence, and name/qualified-name relationships. `entity-boost` compares one glossary term, glossary
node, data product, domain, and dataset that share a name and description. `entity-search` checks
that one imported user and one imported group are findable. Neither group is part of an nDCG mean.
The `autocomplete` queries are also pass/fail and stay out of both nDCG floors: a two-character
prefix of a name that does not occur in Cranfield, a one-character prefix of the rank users, and a
prefix that matches nothing.

Two kinds of judgment are kept apart. Ground truth is `benchmark` (Cranfield qrels) and
`longtail-ecommerce` (the 4/3/2/1 scale applied to catalog text). Those are the only groups in an
nDCG mean, and a grade has to be defensible from the asset's name, title, description, or field
text. Mechanism probes encode a ranking rule and stay out of both means: `entity-boost`,
`entity-search`, `structured-property`, `sort`, `field-probe`, `autocomplete`, and
`keyword-probe`. `entity-boost` gives a glossary term, glossary node, data
product, domain, and dataset one shared name and description. The glossary term and
data product may lead in either order, and both stay in the top three with the dataset.
The glossary node probe expects the data product ahead of the node. Shipped
`entityTypeBoosts` are glossary term and node 10, domain and data product 5, and dataset
unweighted. These probes are not evidence that those weights are the right product choice.

Each engine writes one JSON file per gate to
`metadata-io/build/reports/search-quality/<engine>-<metric>.json` before the floor check. A named
profile writes `<engine>-<profile>-<metric>.json` so two profiles of one engine keep both files.
OpenSearch 3 uses the `opensearch3` name so it does not overwrite the OpenSearch 2 report. Each
query and each probe records its top hits with rank, urn, name, score, and relevance. When the
metadata-io job fails, CI uploads that directory as `Search Quality (metadata-io <suite>)` and
keeps it for one day.

Until this pack is on `master`, load the checkout instead of the registry name:

```bash
datahub datapack load search-quality \
  --url file://$PWD/metadata-io/src/test/resources/datapacks/search-quality/index.json \
  --trust-custom --no-time-shift
```

## Tests that still use `sample_data`

These stay on the old `sample_data` fixture until the cleanup follow-up. Their whole-index
counts assume that corpus alone:

- `SampleDataFixtureElasticSearchTest`
- `SampleDataFixtureOpenSearchTest`

Both extend `SampleDataFixtureTestBase` (mappings, aggregations, scroll, autocomplete,
analyzers, and exact counts). Search ranking runs from this pack. `search_lineage` stays
the graph-walk fixture and is not part of this pack. Cleanup is tracked as a follow-up to
move `sample_data` onto this pack and delete the old dumps.
