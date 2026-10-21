"""Cranfield plus the long-tail catalog, scored by separate judgments.

This directory is a normal multi-file datapack. `index.json` lists only MCP shards.
`judgments.json` is test metadata and is not ingested by `datahub datapack load`.
The long-tail shards in this pack are the catalog. Search tests load this datapack
and do not read Elasticsearch index snapshots.

Regenerate judgments, Cranfield, and the probes with:

```bash
python3 metadata-ingestion/tests/unit/search_quality/generate_search_quality_pack.py
```

That command leaves `03-long-tail.json` and `04-long-tail-timeseries.json` in place.

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

The `longtail` queries use the same gain scale. A 4 is the asset whose name, title, or
description is the query. A 3 is that asset on another platform or a close variant. A 2 is a
related asset a user would still open. A 1 is a neighbor. `longtailNdcgAt10` is the mean nDCG@10
of those queries on the template path. `javaBuilderLongtailNdcgAt10` is the same mean with
template execution skipped. The `ranking` queries are pass/fail constraints on the template
path: top hit, order, and presence.

Each engine writes one JSON file per gate to
`metadata-io/build/reports/search-quality/<engine>-<metric>.json` before the floor check. OpenSearch
3 uses the `opensearch3` name so it does not overwrite the OpenSearch 2 report. When the metadata-io
job fails, CI uploads that directory as `Search Quality (metadata-io <suite>)` and keeps it for one
day.

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
