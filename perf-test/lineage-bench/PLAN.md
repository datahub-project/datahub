# Lineage walk benchmark (local)

**Questions to answer**

1. Where does a deep Impact walk spend its time? Split it by hop, esQuery call, PIT and slice.
2. How does esQuery latency change with the number of URNs in the query and the hits per page?

Everything runs on the laptop (48 GB RAM, 18 CPUs) with `datahub docker quickstart`. The only GMS change is structured log lines. Results are CSV files.

Files in this folder:

| File                       | Purpose                                                                                                                        |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `docker-compose.bench.yml` | Quickstart compose with bigger heaps and the locally built GMS image                                                           |
| `gen_graph.py`             | Builds the dataset once, in a database-neutral format, with ground-truth answers (§1.3)                                        |
| `dataset/<name>/`          | `nodes.csv`, `edges.csv`, `start_nodes.csv`, `manifest.json`. The same files feed every backend                                |
| `load_mysql.py`            | Loader for the DataHub/MySQL path: turns `dataset/<name>/` into `metadata_aspect_v2` rows                                      |
| `load_indices_parallel.sh` | Runs LoadIndices over disjoint URN ranges in parallel (§1.4)                                                                   |
| `run_walks.py`             | Runs the walks, then turns the GMS logs into CSVs                                                                              |
| `report.py`                | Renders a run directory as one self-contained HTML page: `python3 report.py results/<run_id>` → `results/<run_id>/report.html` |
| `results/<run_id>/`        | `walks.csv`, `hops.csv`, `esquery.csv`, `os_stats_{before,after}.json`                                                         |

---

## 1. Setup

### 1.1 Resources

- Docker Desktop: 32 GB memory, 12 CPUs.
- Copy `docker/quickstart/docker-compose.quickstart-profile.yml` to `docker-compose.bench.yml` and change:

| Service                   | Default                                               | Bench                                                                                                        |
| ------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| opensearch                | `OPENSEARCH_JAVA_OPTS -Xms768m -Xmx1024m`             | `-Xms12g -Xmx12g`                                                                                            |
| datahub-gms               | `JAVA_OPTS -Xms1g -Xmx1g`                             | `-Xms6g -Xmx6g -Dlineage.bench.log=true`                                                                     |
| datahub-gms               | `image: acryldata/datahub-gms:${DATAHUB_VERSION}`     | `image: datahub-gms:lineage-bench`                                                                           |
| mysql                     | default                                               | `command: --innodb-buffer-pool-size=4G` (keep the existing flags)                                            |
| datahub-gms               | —                                                     | `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_MAX_RELATIONS=-1`, `..._PARTIAL_RESULTS=true`, `..._SLICES=6` (see below) |
| system-update             | `image: acryldata/datahub-upgrade:${DATAHUB_VERSION}` | `image: datahub-upgrade:lineage-bench` (also runs LoadIndices)                                               |
| mysql                     | —                                                     | also `--local-infile=1` for the bulk load                                                                    |
| datahub-actions, frontend | on                                                    | leave running; they sit idle                                                                                 |

**Bench GMS settings that differ from defaults:**

- **`maxRelations`**: the default is 40000 with `partialResults=false`, so any walk reaching more than 40k nodes throws instead of finishing. The bench sets unlimited (`-1`), so big walks run to completion, bounded only by the 50 s timeout. With `partialResults=true`, a timeout returns partial results (logged as `truncated`) instead of an error.
- **Slices**: the default is the data-node count, which is 1 locally and gives `max(2, 1) = 2` slices. The bench uses prod's 6.
- **`index.max_terms_count`** on `graph_service_v1`: raised from 65,536 to 262,144 after LoadIndices (`PUT graph_service_v1/_settings`). An Impact hop sends its whole frontier as one `terms` query. With the default, any hop with more than 65,536 URNs is rejected by OpenSearch, and with `partialResults=true` that hop silently returns nothing. Prod doesn't hit this only because `maxRelations=40000` stops the walk first. The bench needs it for the `prod_like` hops of 120k–130k URNs.
- **Shards**: quickstart creates `graph_service_v1` with 1 primary shard; prod has 20. This isn't changed. Keep it in mind when comparing with prod latencies.

### 1.2 Build and start (always from clean volumes)

Every run starts from empty volumes, so nothing left over from earlier graphs or earlier runs gets mixed in.

```bash
./gradlew :metadata-service:war:docker :datahub-upgrade:docker   # logging change (§2) + LoadIndices fix
docker tag acryldata/datahub-gms:debug datahub-gms:lineage-bench
docker tag acryldata/datahub-upgrade:debug datahub-upgrade:lineage-bench
datahub docker nuke                              # removes containers and volumes (MySQL + OpenSearch)
# local images can't be pulled, so pre-pull the published ones and skip pulling
DATAHUB_VERSION=head docker compose -p datahub -f docker-compose.bench.yml --profile quickstart pull --ignore-pull-failures
DATAHUB_VERSION=head datahub docker quickstart --no-pull-images --version head -f docker-compose.bench.yml
```

Quickstart puts bootstrap rows into `metadata_aspect_v2` at startup: data platforms, policies and the `datahub` user. GMS needs them, so never truncate that table. All bench data goes under one URN prefix so it can be deleted on its own:
`urn:li:dataset:(urn:li:dataPlatform:snowflake,bench.<family>.<id>,PROD)`

### 1.3 Synthetic graph (about 1.19M datasets, 2.1M edges)

The target is the largest prod tenant's lineage graph, counting only `DownstreamOf` edges (numbers from the Hex analysis):

| Prod stat                         | Value                                                           | Covered by                                                                                                   |
| --------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Lineage nodes / edges             | ~933k / 2.09M (~2.2 upstreams per node)                         | whole graph (~1.19M / ~2.1M)                                                                                 |
| Largest Impact walk from one node | ~584k nodes                                                     | `prod_like` root                                                                                             |
| Largest direct downstream fan-out | ~56k                                                            | `prod_like` hop 1, `hub`                                                                                     |
| Longest chain                     | 26 hops                                                         | `prod_like` (~22 layers), `deep` (26 layers); the `deep100_*` families go to 100 at widths 1, 10, 100 and 1k |
| Cycles                            | yes, largest SCC ~589 nodes                                     | `prod_like` back-edges; `scc` is one 50k-node SCC                                                            |
| Shape                             | convergent: most nodes are leaves that feed a few shared tables | `prod_like`                                                                                                  |

Impact means downstream: each child's `upstreamLineage` lists its parents.

| Family                                 | Shape                                                                                                                                                                                                                                                                                                | Nodes | Edges | What it exercises                                                                                                                                |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Prod worst case**                    |                                                                                                                                                                                                                                                                                                      |       |       |                                                                                                                                                  |
| `prod_like`                            | Layered DAG from one root, ~22 layers, layer sizes below; each node has 1 parent in the layer above plus ~1.2 more drawn from the layer above or the same layer (avg ~2.2 upstreams); ~1% of nodes get a back-edge to a node 1–3 layers up, and one cluster of ~600 nodes is made strongly connected | ~580k | ~1.3M | The prod worst case: 1 URN → 56k hits, then hops of 100k+ URNs, fan-in dedup, cycles                                                             |
| **Prod-shaped**                        |                                                                                                                                                                                                                                                                                                      |       |       |                                                                                                                                                  |
| `hub`                                  | 1 node → 56k children                                                                                                                                                                                                                                                                                | ~56k  | ~56k  | 1 URN in, 56k hits, many pages per slice, nothing else going on                                                                                  |
| `deep`                                 | 26 layers × 2k nodes, 2 parents each in the layer above                                                                                                                                                                                                                                              | ~52k  | ~104k | Prod-length walk (26 hops) with flat, moderate queries                                                                                           |
| **Very deep: width sweep at 100 hops** |                                                                                                                                                                                                                                                                                                      |       |       |                                                                                                                                                  |
| `deep100_w1`                           | a single 100-node chain                                                                                                                                                                                                                                                                              | 100   | 99    | 1 URN and 1 hit per hop. Almost no ES work, so it isolates the fixed cost per hop (PIT create/delete, slice fan-out, post-processing)            |
| `deep100_w10`                          | 100 layers × 10 nodes, 2 parents each in the layer above                                                                                                                                                                                                                                             | 1k    | ~2k   | Narrow and deep: 10 URNs in, ~20 hits per hop                                                                                                    |
| `deep100_w100`                         | 100 layers × 100 nodes, 2 parents each                                                                                                                                                                                                                                                               | 10k   | ~20k  | 100 URNs in, ~200 hits per hop                                                                                                                   |
| `deep100_w1k`                          | 100 layers × 1k nodes, 2 parents each                                                                                                                                                                                                                                                                | ~100k | ~200k | 1k URNs in, ~2k hits per hop. Does the walk finish inside the 50 s budget?                                                                       |
| **Shallow**                            |                                                                                                                                                                                                                                                                                                      |       |       |                                                                                                                                                  |
| `wide`                                 | fan-out 50, depth 3                                                                                                                                                                                                                                                                                  | ~128k | ~128k | Last hop sends 2.5k URNs and gets 125k hits back                                                                                                 |
| `mid`                                  | fan-out 10, depth 5                                                                                                                                                                                                                                                                                  | ~111k | ~111k | Steady 10× growth per hop                                                                                                                        |
| `small_islands`                        | 8k separate trees of 13 nodes (fan-out 3, depth 2)                                                                                                                                                                                                                                                   | ~104k | ~96k  | The common case: prod has tens of thousands of small components. Baseline latency for tiny walks                                                 |
| **Cycles**                             |                                                                                                                                                                                                                                                                                                      |       |       |                                                                                                                                                  |
| `scc`                                  | one strongly connected component: a 50k-node ring plus 2 random chords per node                                                                                                                                                                                                                      | ~50k  | ~150k | Every node reaches all 50k in ~15 hops. After the first few hops most hits are already visited, so it stresses visited-set dedup and wasted hits |

`prod_like` layer sizes (URNs sent at hop k+1 = size of layer k):

| Layer | 1   | 2    | 3    | 4    | 5   | 6   | 7   | 8   | 9   | 10  | 11  | 12  | 13–22                 |
| ----- | --- | ---- | ---- | ---- | --- | --- | --- | --- | --- | --- | --- | --- | --------------------- |
| Nodes | 56k | 120k | 130k | 100k | 70k | 45k | 25k | 15k | 8k  | 5k  | 3k  | 2k  | ~1.5k total, tapering |

The extra parents come only from the layer above or the same layer, so a node's hop number is still its layer, and each walk's per-hop sizes are known exactly. Back-edges point upward, so they add cycles without changing any hop number.

100-hop walks are allowed. The Impact hop cap is `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_MAX_HOPS`, which defaults to 1000. The limit that can actually bite is the 50 s walk timeout. If a `deep100_*` walk hits it, that's a finding: the walk log records `truncated` and how many hops it got through.

Not modeled: prod lineage is ~93% datasets and ~7% dataJobs. Everything here is a dataset.

#### The dataset is its own artifact

The dataset is the most important part of the benchmark, and we want to reuse it to test other graph backends later. So generating it is kept separate from loading it into any one database.

`gen_graph.py --seed 42 --scale 1.0 --out dataset/v1/` writes:

| File              | Contents                                                                                                                                                                                                                                                                                                                           |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `nodes.csv`       | `urn, family, layer`                                                                                                                                                                                                                                                                                                               |
| `edges.csv`       | `downstream_urn, upstream_urn`, one row per `DownstreamOf` edge                                                                                                                                                                                                                                                                    |
| `start_nodes.csv` | `urn, family, layer, expected_per_hop, expected_total, expected_hops`. It lists each family root, plus `prod_like` nodes at layers 1, 2 and 4 (smaller walks of the same shape), `deep100_*` nodes at layers 50 and 75 (50- and 25-hop walks), one middle node per tree family, 3 `scc` nodes, and 20 random `small_islands` roots |
| `manifest.json`   | seed, flags, per-family node and edge counts, and a sha256 of each file                                                                                                                                                                                                                                                            |

- **The ground truth doesn't come from any database.** `gen_graph.py` runs its own in-memory BFS from every start node to fill in the `expected_*` columns. Every backend is checked against those same answers.
- **It's deterministic.** The same seed and flags always produce identical files, and the manifest hashes prove it.
- `--families` and `--scale` flags allow a 1% version for pipeline checks.
- A dataset version, once used for results, is never changed. Any change means a new version (`dataset/v2/`).

`load_mysql.py dataset/v1/` is the loader for the DataHub path:

- **It deletes only its own rows first**: `DELETE FROM metadata_aspect_v2 WHERE urn LIKE 'urn:li:dataset:(urn:li:dataPlatform:snowflake,bench.%'`.
- It writes three aspects per dataset, all at version 0: `datasetKey`, `status`, and `upstreamLineage`, built by grouping `edges.csv` by `downstream_urn`. Roots get no `upstreamLineage`.
- It loads them as a TSV with `LOAD DATA LOCAL INFILE` into `metadata_aspect_v2`.

Other backends each get their own `load_<backend>.py` that reads the same `dataset/v1/` files.

### 1.4 Build the indices with LoadIndices

LoadIndices reads MySQL and writes OpenSearch directly, with no Kafka. **It never deletes documents.** It turns off refresh and replicas while loading, restores them afterwards, and upserts by doc id. Rerunning the same graph just overwrites the same docs. But edges from a _different_ earlier graph would stay in `graph_service_v1` and give wrong walk counts.

**Cleanup before LoadIndices:**

- Normal path: the graph changed, so start again from §1.2 (`nuke`). Then there is nothing to clean up.
- Quick path (same stack, regenerated graph): delete the bench docs first, then reload.
  ```bash
  P='urn:li:dataset:(urn:li:dataPlatform:snowflake,bench.'
  curl -XPOST "localhost:9200/graph_service_v1/_delete_by_query?refresh=true" -H 'Content-Type: application/json' \
    -d "{\"query\":{\"bool\":{\"should\":[{\"prefix\":{\"source.urn\":\"$P\"}},{\"prefix\":{\"destination.urn\":\"$P\"}}]}}}"
  curl -XPOST "localhost:9200/datasetindex_v2/_delete_by_query?refresh=true" -H 'Content-Type: application/json' \
    -d "{\"query\":{\"prefix\":{\"urn\":\"$P\"}}}"
  ```
  Check the field names against the index mapping the first time you use this.

Then:

```bash
DATAHUB_VERSION=head docker compose -p datahub -f docker-compose.bench.yml --profile quickstart run --rm --no-deps \
  -e ES_BULK_REQUESTS_LIMIT=5000 -e ES_BULK_FLUSH_PERIOD=300 -e ELASTICSEARCH_THREAD_COUNT=4 \
  system-update-quickstart -u LoadIndices -a batchSize=5000
```

**LoadIndices processes on a single thread** (~500 aspects/s, so ~2 h for the 3.6M bench aspects). `load_indices_parallel.sh [N]` runs N LoadIndices containers over disjoint `urnLike` URN-prefix ranges instead (6 in parallel gives ~1,000 aspects/s on the laptop). Stop GMS, frontend and actions while it runs, to free memory.

**LoadIndices is broken on master.** Commit `6bdbf554` (#17446, pgQueue messaging) removed the no-op `kafkaEventProducer` bean from `LoadIndicesConfig`, so LoadIndices fails at startup with "required a bean named 'kafkaEventProducer' that could not be found". The bench branch restores that bean. This should be reported and fixed upstream.

Checks:

- `graph_service_v1` bench doc count equals the edge count from `gen_graph.py` (~2.1M).
- `datasetindex_v2` bench doc count equals the node count (~1.19M).
- A GraphQL walk from each family root returns `expected_total`. For `prod_like` that's ~580k, for `scc` it's 50k from any node, and for `deep100_w1k` it's ~100k over 100 hops.

When the checks pass, save a snapshot so reruns don't have to rebuild. Stop the stack and `tar` the MySQL and OpenSearch volumes. **Restoring means: nuke, untar both volumes (always as a pair), then start.**

---

## 2. Metrics: one log line per event

Add a dedicated logger, `LINEAGE_BENCH`. It writes one JSON line per event and only when `-Dlineage.bench.log=true` is set, so it has no effect otherwise. A walk runs on several threads (the `pit-worker` pool), so every line carries `walk_id`. That is a UUID made at the start of `getImpactLineage` and passed down as a parameter; nothing relies on thread-local state.

| Line      | Where                                                                                 | Fields                                                                                                            |
| --------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `esquery` | `GraphQueryPITDAO.searchWithSlices`, around the `.search(...)` call (around line 293) | `walk_id, hop, slice, page, urns, hits, took_ms, client_ms, timed_out, shards_failed`                             |
| `hop`     | `GraphQueryBaseDAO.getImpactLineage`, once per hop                                    | `walk_id, hop, urns_in, rels_out, pit_create_ms, slices_ms, slice_max_ms, slice_min_ms, postprocess_ms, total_ms` |
| `walk`    | end of `getImpactLineage`                                                             | `walk_id, start_urn, hops, total_rels, truncated, status, total_ms`                                               |

`page` counts from 0 within a slice. The last page is usually empty: that's the empty follow-up we want to count. `client_ms` minus `took_ms` is network, serialization and client time.

Example:

```
LINEAGE_BENCH {"t":"esquery","walk_id":"8f..","hop":3,"slice":2,"page":0,"urns":2500,"hits":5000,"took_ms":41,"client_ms":63,"timed_out":false,"shards_failed":0}
```

OpenSearch-side context comes from the harness, not GMS. It saves `GET _nodes/stats/indices/search,thread_pool` before and after the run, which shows search thread-pool queueing and rejections.

These answer the two questions like this:

- **Q1:** `hops.csv` shows the time split per hop. `esquery.csv` grouped by `(walk_id, hop, slice)` shows slice skew and pages per slice.
- **Q2:** plot `esquery.csv` by `urns` and `hits`, with one row per call. The families are chosen so that URN count and hit count vary independently enough to separate them.

---

## 3. Test: `run_walks.py`

```
python run_walks.py --gms http://localhost:8080 --token $TOKEN \
    --start-nodes start_nodes.csv --repeats 3 --out results/<run_id>
```

1. Save `_nodes/stats` to `os_stats_before.json`, and note the run start time.
2. Warm-up: 3 walks from one `mid` node. They are not recorded.
3. For each start node, repeated `--repeats` times, **one walk at a time**:
   - Call `searchAcrossLineage` with:
     - `urn`
     - `direction: DOWNSTREAM`
     - `types: [DATASET]`
     - `query: "*"`
     - `count: 10`
     - `orFilters` degree `["1","2","3+"]`
     - `searchFlags.skipCache: true`
   - Record the client-side `wall_ms`, the HTTP status, `total`, and any errors.
   - Wait 1 s so PIT cleanup from the previous walk doesn't overlap the next one.
4. Save `_nodes/stats` to `os_stats_after.json`.
5. Get the GMS logs with `docker logs datahub-gms --since <run start>`, keep the `LINEAGE_BENCH` lines, and write `esquery.csv`, `hops.csv` and `walks.csv`.
6. Link each harness walk to its GMS `walk_id` by `start_urn` and order: walks are serial, so it's one to one. Add `family`, `level` and `wall_ms` to `walks.csv`.

Sanity checks the script prints at the end:

- every harness walk has exactly one `walk` line
- `total` matches `expected_per_hop` from `start_nodes.csv`
- there are no `timed_out` or `truncated` walks, or the script lists them if there are

## Order of work

1. **Dataset first.** Write `gen_graph.py` and generate `dataset/v1/`. It prints a report for each family: nodes, edges, upstreams per node, BFS depth, per-hop sizes and SCC sizes. Check that report against the prod targets in §1.3 before anything else. This step doesn't need DataHub running.
2. Logging change in GMS, with a unit test that the lines appear when the flag is on.
3. `load_mysql.py` + LoadIndices at `--scale 0.01`, then run `run_walks.py` end to end. Regenerate once through the quick cleanup path, to check that the counts still match.
4. The full dataset, the checks, and a snapshot.
5. The real run.

---

## Backends: OpenSearch vs the in-memory lineage graph server

The in-memory server is `metadata-jobs:lineage-graph-server` (see `research.md` §6–8). It runs on the host.

```bash
# Images from this branch, under their own tag so acryldata/* images are untouched
./gradlew :metadata-service:war:docker :datahub-upgrade:docker -PdockerRegistry=lineagegraph
./gradlew :metadata-jobs:lineage-graph-server:installDist

# Stack (flag off = OpenSearch walks; on = in-memory walks with OpenSearch fallback)
export BENCH_GMS_IMAGE=lineagegraph/datahub-gms:debug BENCH_UPGRADE_IMAGE=lineagegraph/datahub-upgrade:debug \
  DATAHUB_VERSION=head DATAHUB_TOKEN_SERVICE_SIGNING_KEY=<any> DATAHUB_TOKEN_SERVICE_SALT=<any> \
  LINEAGE_GRAPH_SERVER_ENABLED=false   # or true
docker compose -p datahub -f docker-compose.bench.yml --profile quickstart up -d datahub-gms-quickstart

# Server (first start scans graph_service_v1 and writes a snapshot; later starts mmap it)
LINEAGE_GRAPH_SNAPSHOT_DIR=$PWD/snapshots/lineage-graph/v1 LINEAGE_GRAPH_SERVER_OPTS="-Xmx2g -XX:+UseG1GC" \
  ../../metadata-jobs/lineage-graph-server/build/install/lineage-graph-server/bin/lineage-graph-server

python3 measure_load.py --out results/<run>/load              # cold + warm load time, memory
python3 run_walks.py --backend direct --start-nodes dataset/v1/start_nodes.csv --out results/<run>/direct
python3 run_walks.py --backend gms --query-shape ui --start-nodes dataset/v1/start_nodes.csv --out results/<run>/gms
python3 parity_check.py --out results/<run>/parity             # GMS (current backend) vs server
python3 freshness_test.py --out results/<run>/freshness        # write -> visible, server vs OpenSearch
python3 compare_report.py results/<os_run> results/<inmem_run> --names opensearch inmemory --out compare.html
```

**Experiment 2 (lightning search on the server).** GMS sends lightning searches to
`POST /v1/impact/search` unless `LINEAGE_GRAPH_SERVER_SEARCH_PUSHDOWN=false` (experiment 1).
Export it before `docker compose ... up -d datahub-gms-quickstart`, which recreates GMS when it
changes. Runs and results: `results/exp2/README.md`.

```bash
# Full GraphQL responses, byte for byte, between two GMS configurations
python3 -I response_parity.py capture --label pushdown_on  --out results/<run>/pushdown_on.jsonl
python3 -I response_parity.py capture --label pushdown_off --out results/<run>/pushdown_off.jsonl   # after recreating GMS
python3 -I response_parity.py compare --mode exact results/<run>/pushdown_off.jsonl results/<run>/pushdown_on.jsonl
python3 -I response_parity.py compare --mode sets  results/<run>/opensearch.jsonl  results/<run>/pushdown_on.jsonl
# The server alone: walk + lightning search for a query shape
python3 run_walks.py --backend direct-search --query-shape filtered50 --start-nodes dataset/v1/start_nodes.csv --out results/<run>/direct_search
python3 summarize_exp2.py   # results/exp2/summary.{md,json}
```

If `./gradlew ...:docker` fails with "Cannot run program docker", the Gradle daemon's PATH lacks
`~/.docker/bin`. Add `--no-daemon`.

## ES terms scaling (searching within a lineage result set)

Sets the thresholds in the design's section 9: T (largest result set searched in one ES request),
D (page depth for the batched tier) and batch concurrency. Needs the bench OpenSearch (v1 data) on
:9200 and the lineage graph server on :8090 (it supplies the prod_like root's walk, in walk order).

```bash
caffeinate -i python3 -I es_terms_scaling.py --out results/terms_scaling/<run> \
  --repeats 10 --warmup 2                      # sweep: N = 1k..500k, page 10/1000, sort, exact totals
python3 -I es_terms_scaling.py --out results/terms_scaling/<run>_gms \
  --template captured_gms_request.json        # same, with a request body captured from GMS
```
