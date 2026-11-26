---
description: "Search V3 design: a template-based query builder on shared, unified fields, how a request runs, and the document contract."
---

# Search V3 Design

Search V2 and Search V2.5 stay as they are until they are deprecated.

Search V3 templates change ranking once `SEARCH_V3_TEMPLATES_ENABLED` and Search V3 keyword read are on. Until then, `search_config.yaml` and the Java query builder still apply. Turning the flag on needs a GMS restart. Editing a template body does not. The enable and disable switches are listed in [Environment Variables](../deploy/environment-vars.md#search-v3-feature-switches).

Search V3 is a template-based query builder on shared, unified fields. The builder renders a `searchTemplate`, addressed by URN, into the query. An operator changes relevance by writing that URN. A caller changes one request by selecting one.

- [Overview](#overview) is the model.
- [Customization](#customization) is the template an operator writes and a caller selects.
- [Query execution](#query-execution) is how a selected template is run, ranked, and rejected.
- [Documents](#documents) is what gets indexed, and what a hit can carry.

## Overview

### Shared fields

A query, a filter, a facet, or a template names a shared field. The set is unified: that name means the same thing on every entity.

The traditional PDL model stays. An `@Searchable` annotation maps a PDL field onto a shared field. The shared name, degree, Search V3 weight, and Search V3 query-by-default flag are set on the `v3` object: `v3.searchLabel`, `v3.degree`, `v3.boostScore`, and `v3.queryByDefault`. Unset degree is 0. The top-level `queryByDefault` and `boostScore` stay the Search V2 values. Search V2 ignores `v3`. See [Degrees of separation](#degrees-of-separation). `name` collects title and name. `description` collects every description: info aspects, properties, ingested, edited, and generated. Platform is its own field, separate from the name, because a URN mixes the two. The builder queries the shared field. It does not query the PDL path.

The per-field V2 query surface is not this contract. A new entity maps its PDL fields onto the unified set with the same annotation. It does not add a builder of its own. Each aspect also keeps its own fields under `_aspects.<aspect>`, unanalyzed. The unified fields sit at the document root. A few root names stay reserved for embeddings and other data that is not an aspect. The mapping is in [Documents](#documents).

### Degrees of separation

Some facts live on the entity and again on things related to it. A dataset has tags, and its columns have tags. The dataset's own tags say more about it than the tags on its columns, and a value further away can say less again. Glossary terms, owners, and any other shared field follow the same pattern. Tags are the example.

Each distance is a field a template, a filter, or a `/q` query can name on its own. The value on the entity is degree 0, `tags`. One step away is `tags_1`. Two steps away is `tags_2`. The number is the distance, and the same number means the same distance on every entity. Column name, description, tags, and glossary terms are degree 1: `name_1`, `description_1`, `tags_1`, and `glossaryTerms_1`. Those column fields stay out of an unqualified query. A schema field leaves `v3.queryByDefault` unset. An edited column sets it false, because that field annotation also opts a business attribute in. A filter or `fields.add` names that degree. Search V2 still reads the same values through the top-level `queryByDefault`. An entity with nothing at that distance has no such field.

The model sets the distance and its default weight. A nearer distance weighs at least as much as a farther one. The default query includes a distance when `v3.queryByDefault` is true, at `v3.boostScore`. Column degrees are left out. A template then decides what a request actually runs:

- `fields.override` changes the weight of one distance that is already queried. A farther distance may weigh more than a nearer one.
- `fields.remove` drops one distance and leaves the others in the query. Removing `tags_1` keeps column tags from matching when a template added them. Removing `tags` keeps the dataset's own tags out and still searches the other distances.
- `fields.add` queries a distance the default left out, including a column degree.

The example is in [Field weights](#field-weights).

A filter, a facet, a clause, a list boost, or a highlight names one indexed field. A filter on `tags` matches the dataset's own tags. A filter on `tags_1` matches tags on its columns. A contains or existence filter on the source field matches every degree of that source. Matching more than one distance by name combines those fields. How the fields are stored is in [Documents](#documents).

### The query is a template

A template is the `searchTemplateProperties` aspect, `urn:li:searchTemplate:<id>`. It holds the query body and the sections around it: clauses, scoring, field weights, a ladder, retrieval, sort, and an ordered route list. A surface and an engine resolve to a seed URN. That seed's routes choose the template from the query text. A deployment customizes by writing a child that replaces the sections it sets, or by pointing a slot at a different URN.

A child inherits a parent. A section left unset stays. An explicit empty section drops the inherited one, so a boost, an exclusion, a rescore, a ranking preset, or the document rerank weights is a small write against a bootstrapped default. The next search renders the new body. There is no redeploy. The sections are in [Customization](#customization).

### Query time

A caller overrides the stored template for one request and does not write it. `SearchFlags.searchTemplate` selects a URN. On a search, the walk follows that template's `next`. Scroll does not. `sortCriteria` replaces the sort. On a from/size search it also replaces engine `rescore`, `ranking`, and `knn`. Aggregation flags adjust that request. Highlighting stays off. The flags are in [Runtime and query-time customization](#runtime-and-query-time-customization).

### What the service owns

On a keyword rung, soft delete, hidden lifecycle stages, requested entity types, latest-version filtering, and view authorization wrap the rendered query. A template cannot widen what the caller is allowed to see. On a vector rung, soft delete and hidden lifecycle stages are inside the kNN request, and view authorization is applied after the top-k. See [Vector retrieval](#vector-retrieval). Paging and the result cache stay in the service. System templates are editable only by the system actor.

The service does not classify the query in code. The surface, the engine, and the fulltext flag select a seed template. The seed's ordered routes then match the trimmed query, and the first match wins. A query that matches none stays on the seed unless the seed sets a default target. An empty query or `*`, with no caller URN, selects list and runs on the API the caller used. It does not force scroll. A caller-supplied URN is the entry, and its merged routes still run. Fully qualified name and deep path are shape flags. A quoted query is exact and requires the phrase. A single term with no spaces is not exact. Column search matches the column name. A structured `/q` query uses the shared field names, which the caller reads from the schema. The route tables are in [Selection](#selection).

The template body chooses a placeholder. The service computes the variations. The template does not supply a rewrite. See [Query variations](#query-variations).

A ladder tries one template, then another. The rules for entering a rung, keeping its result, and which URN to try next are data on the template. A template with no `next` does not hand off. Scroll does not walk a ladder. See [Query ladder](#query-ladder) and [Search and scroll](#search-and-scroll).

A rung renders the template sections. The ladder owns selection, stopping, joins, and `next`.

`@Searchable` maps a PDL field onto a shared field. `v3.searchLabel` and `v3.degree` name the indexed field. `v3.queryByDefault` true puts that field in the default queried set, and an unset flag leaves it out. `v3.boostScore` is its default weight. The top-level `queryByDefault` and `boostScore` stay the Search V2 values. `fields.add`, `fields.remove`, `fields.replace`, and `fields.override` choose which fields are queried and the weight of each. The author writes the `term`, `prefix`, `wildcard`, or `match` clause in the Mustache body, and the boost and `dis_max` around it. The composed keyword query adds the generated whole-value terms and the name-and-value group around that body. See [Field weights](#field-weights).

A template declares the section names this release accepts. An unknown name is rejected when the template is written and when it is loaded. `apiVersion` is 1, or 2 for a template that sets a semantic field (`combine`, `querySource`, `anchor`, `profileScore`, `minSimilarity`, `vectorBody`, `profiles`, `scope`, or route `match`), so a release that does not read those fields rejects the template instead of ignoring them. A ranking preset other than `none`, a window sizer other than `fixed`, `vectorThenKeyword`, `vectorFilter`, `vectorBody`, `profiles` on a keyword rung, the native embedding placeholders, a bridge, and a merge join fail when that template is selected. A vector rung fails on scroll. A `scope` that leaves out a requested entity type fails a scroll, filter, aggregation, or count, which render the template once and do not walk a ladder. See [Capabilities](#capabilities).

### How a request runs

Bind resolves the seed or the caller URN, follows its routes, merges parents, and checks the contract. On a search, the ladder tries rungs until one stops. The executor renders, scores, and slices a keyword rung. Scroll runs the composed keyword query and does not walk the ladder. Scroll also drops engine `rescore`, `ranking`, and `knn`, and sorts by score, then URN. Which features each API runs is in [Search and scroll](#search-and-scroll). `includeExplain` returns the explanation of the query that was served. `searchType` opts a request into DFS query-then-fetch when consistent IDF across shards matters. The steps are in [Query execution](#query-execution).

The composed keyword query is the rendered body, then whole-value terms and the name-and-value group, then `clauses`, then `functionScore`. A property in the queried set joins the weighted field list and gets a whole-value term at its field weight. It joins a `dis_max` when the author fills `extraDisMaxQueries`, or when a replaced `query` writes its path into a clause. See [Query holes](#query-holes). Inside a clause that scores by field weight, equal weights tie and a higher weight ranks above `name`. A numeric boost written in the body stays that boost. A quoted query requires the phrase. `functionScore`, engine `rescore`, `ranking`, and `knn` can still reorder inside their windows. Scroll runs this composed keyword query and still drops those three. See [Ranking](#ranking) and [Search and scroll](#search-and-scroll).

A vector participates in one of three ways, and each is a different template shape. `knn` on a keyword template reorders rows already in the keyword window and adds no hits. Corpus kNN is its own rung. A keyword rung and a vector rung can run together and add hits that only the vector leg found. In OSS the three shapes run for the Document entity only. Hybrid adds vector-only hits once a fusion policy is chosen. See [Document rerank](#document-rerank) and [Vector retrieval](#vector-retrieval).

The search box is a search. Which field matched is read from the returned page, for the fields the request opted into. The default set is empty, so the default query stays one body and fetches no highlight source. The engine highlighter stays off. A non-empty set opts into final-page source matching, and `skipHighlighting` clears that set. See [Highlight fields](#highlight-fields).

Hydration from `_source` is not implemented. Search responses still read aspects from the primary store. See [Hydrating from `_source`](#hydrating-from-_source).

## Customization

A template is the `searchTemplateProperties` aspect, addressed as `urn:li:searchTemplate:<id>`. A surface and an engine resolve to a seed URN. A deployment customizes by writing a child, or by pointing a slot at a different URN.

### Runtime and query-time customization

**Runtime** changes what every caller sees. Creating a template, editing a section, or pointing a child at a new parent is a write to `urn:li:searchTemplate:<id>` through the generic entity API, including JSON patch. After the write commits, the next search renders the new body. `application.yaml` names which seed URN a surface and an engine resolve to. The fulltext flag chooses the search seed or the structured seed. The body and the route list behind that URN change with no restart. The seeds are system templates. An operator who wants a different route list writes a child that replaces `routes` and points the slot at that child.

**Query time** changes one request and does not write the template:

- `SearchFlags.searchTemplate` selects a URN for that call.
- `sortCriteria` replaces the template sort. On a from/size search it also replaces engine `rescore`, `ranking`, and `knn`.
- `skipAggregates` and `includeDefaultFacets` adjust aggregations for the request.
- A non-empty highlight set opts into final-page source matching. `skipHighlighting` clears that set, including a template set and a caller override. A request may add, remove, or replace the template set, and `customHighlightingFields` replaces it.

The result cache key combines those service discriminators with the template discriminators, so two requests that differ in template do not share a hit and an edit does not keep serving the previous ranking.

### Template fields

These are the sections on `searchTemplateProperties`. The bodies behind the bootstrapped URNs are not specified here.

| Field                     | What it does                                                                                                                                                                                            | Detail                                                                           |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `name`                    | Display name. Not part of the query.                                                                                                                                                                    |                                                                                  |
| `description`             | What this template ranks or matches. Not part of the query.                                                                                                                                             |                                                                                  |
| `parent`                  | Template whose unset sections this one inherits.                                                                                                                                                        | [Inheritance](#inheritance)                                                      |
| `editable`                | When false, only the system actor may change the template.                                                                                                                                              | [What stays fixed](#what-stays-fixed)                                            |
| `dialect`                 | Body language. The request has to match it.                                                                                                                                                             | [Dialect](#dialect)                                                              |
| `apiVersion`              | Template contract. This release implements 1 and 2; 2 is required for the semantic fields.                                                                                                              | [Capabilities](#capabilities)                                                    |
| `capabilities`            | Section names this body declares. An unknown name is rejected when the template is written.                                                                                                             | [Capabilities](#capabilities)                                                    |
| `query`                   | Mustache body for this rung. Used when retrieval is keyword. Setting it replaces the whole body.                                                                                                        | [Query composition](#query-composition)                                          |
| `queryFragments`          | JSON spliced into a named hole the parent body has opened.                                                                                                                                              | [Query holes](#query-holes)                                                      |
| `clauses`                 | Extra bool clauses merged into that body.                                                                                                                                                               | [Query composition](#query-composition)                                          |
| `functionScore`           | Score functions wrapped around the body. A filter snippet is for a boost that is not list membership.                                                                                                   | [Query composition](#query-composition), [List boosts](#list-boosts)             |
| `boosts`                  | List-membership weights. Each entry is a field, an injectable list, and a weight.                                                                                                                       | [List boosts](#list-boosts)                                                      |
| `fieldValueScores`        | Numeric multipliers for an index field or a structured property. Naming a property turns scoring on. Omitted extents come from the definition's `score`.                                                | [Numeric property scores](#numeric-property-scores)                              |
| `entityTypeBoosts`        | Positive weights for named entity types, multiplied onto the rendered query. A weight of 1 is omitted. A request that names one entity type skips the section.                                          |                                                                                  |
| `rescore`                 | An engine rescore window after that wrapper, on a from/size search. Scroll drops it.                                                                                                                    | [Query composition](#query-composition), [Search and scroll](#search-and-scroll) |
| `ranking`                 | Preset that reorders the engine window after the query returns, on a from/size search. Scroll drops it.                                                                                                 | [Ranking](#ranking), [Search and scroll](#search-and-scroll)                     |
| `routes`                  | Ordered query matchers that choose another template before this body runs. An empty list pins this template. A miss stays here unless a default target is set. A route may also set `match` conditions. | [Selection](#selection)                                                          |
| `ladder`                  | On a search: when to run this rung, when to keep the result, and the next template URN. Scroll ignores it.                                                                                              | [Query ladder](#query-ladder), [Search and scroll](#search-and-scroll)           |
| `retrieval`               | How this rung retrieves. Unset is keyword. `combine: keywordRerank` reorders the keyword window, and `combine: vector` (or `mode: vector`) retrieves by embedding.                                      | [Vector retrieval](#vector-retrieval)                                            |
| `profiles`                | Embedding profiles the vector leg uses. Unset resolves each entity to its registry default. A keyword rung rejects it.                                                                                  | [Vector retrieval](#vector-retrieval)                                            |
| `scope`                   | Static entity types this template serves. Unset means the whole request.                                                                                                                                | [Capabilities](#capabilities)                                                    |
| `knn`                     | The blend that reorders rows already in the keyword window, on a from/size search. Scroll drops it.                                                                                                     | [Document rerank](#document-rerank), [Search and scroll](#search-and-scroll)     |
| `aggregations`            | Facets for this rung. Unset keeps the schema default facets.                                                                                                                                            | [Query sequence](#query-sequence)                                                |
| `sort`                    | Sort for this rung. Unset keeps score, then URN, unless engine `rescore`, `ranking`, or `knn` is applied.                                                                                               | [Query composition](#query-composition)                                          |
| `fields`                  | Queried fields and one weight each. Each degree of separation is its own field.                                                                                                                         | [Field weights](#field-weights)                                                  |
| `structuredPropertyQuery` | Analyzed value, exact value, and name-and-value clauses. A child replaces only the clauses it sets.                                                                                                     | [Name and value](#name-and-value)                                                |
| `highlightFields`         | Fields opted into final-page source matching. The engine highlighter stays off.                                                                                                                         | [Highlight fields](#highlight-fields)                                            |
| `parameters`              | Named values the body can reference, such as `deprecatedBoost`.                                                                                                                                         | [Render context](#render-context)                                                |
| `created`                 | When the template was first written. Callers omit it.                                                                                                                                                   | [Stamps](#stamps)                                                                |
| `lastModified`            | When the template was last written. Callers omit it. Part of the result cache key.                                                                                                                      | [Stamps](#stamps)                                                                |

### Inheritance

A child sets `parent` and replaces only the sections it includes. Leaving a section unset keeps the parent.

An explicit empty string drops that section, including an inherited `knn`, `ranking`, `retrieval`, or `ladder`. A child can keep the parent's `functionScore` and `boosts` and drop the parent's rerank, the parent's ranking, or the parent's hand-off. An explicit empty `boosts` drops an inherited list. An explicit empty `structuredPropertyQuery` drops the inherited clauses. A child replaces only the clauses it sets. See [Name and value](#name-and-value).

`fields` merges by key. A child's `add`, `remove`, and `override` combine with the parent's, and the child's `override` wins when both name the same field. When the parent set `replace`, those child keys apply to that set. A child's `replace` replaces the queried set and discards the parent's `add`, `remove`, and `override`.

`ranking.signals` replaces the inherited signal list. `ranking.add` and `ranking.remove` patch that list by signal name, including a list loaded through `ranking.template`. A new name is appended. The same name replaces that signal and keeps its place. A non-empty `signals` list discards parent patches and leaves `preset` unset. An empty `signals` list or an empty `formula` drops that field and keeps the rest of the ranking. An empty `template` drops the reference and the `add` and `remove` patches that sat beside it. A `ranking.template` URN replaces inherited inline ranking fields. Inline ranking fields replace an inherited `ranking.template`.

`queryFragments` merges by hole name. Every level that sets a non-empty key adds a fragment, joined root-first with commas. An unset key adds nothing. An empty string drops inherited fragments for that key. See [Query holes](#query-holes).

`routes` replaces the whole list. Unset keeps the parent's list. An explicit empty list drops it, which is how a route target stays on the body it was routed to. A non-empty child list replaces the parent's order. There is no per-route merge. A default target is separate from the list. Unset keeps the parent's target. Setting one replaces it. Clearing it leaves a miss on this template.

A rung does not reuse the previous rung's `functionScore`, `boosts`, `fields`, `clauses`, or `structuredPropertyQuery`. Those travel only when the next template names this one as `parent`.

### Query holes

A child inserts query JSON into a named hole the parent has opened. Setting `query` still replaces the whole body. `queryFragments` maps a hole name to that JSON. `parameters` stays a separate map of named values.

A parent opens `extraDisMaxQueries` inside a `dis_max` `queries` array. The parent emits a comma and the fragment only when the fragment is set, so an empty hole leaves the JSON valid. The fragment does not start with a comma. Several query objects separated by commas become further branches of that `dis_max`.

```json
{{#extraDisMaxQueries}}
,{{&extraDisMaxQueries}}
{{/extraDisMaxQueries}}
```

Parent and child templates extend the same hole name; they do not create `Next` or `Next2` holes. The loader walks root-first and joins each level's fragment with a comma. A fragment may itself contain several comma-separated query objects. Setting the key to an empty string at a level clears the pieces inherited above it, while descendants may add new pieces.

Each fragment is rewritten for structured-property definition URNs, then rendered with the same Mustache context as the query, then spliced. It is not rendered again, and it does not see other fragments. `{{urn:li:structuredProperty:...}}` resolves to the current field. `{{unquoted}}` and the other query variables expand.

`parent` is searchable with `queryByDefault` false. `searchTemplate` stays out of the default search, browse, and autocomplete entity-type lists. The validator queries that field by entity type. A child write that fills a hole the inherited query does not contain is rejected. A parent `query` write loads direct children whose `parent` is this URN and walks. A child that does not set `query` inherits this body, so every hole it fills must still appear in the new text, and the walk continues to its children. A child that sets `query` stops the walk. Where a remaining hole sits is not checked. The search index can lag a write. Rendering fails when the merged template fills a hole the merged query text does not contain, so the search does not run with the extension left out.

`fields.add` still only changes the weighted field list and the whole-value term. `clauses` still merge outside that `dis_max`, so those scores add. A field does not declare `exact` or `prefix` for the builder to emit. There is no separate append combiner. Setting `query` remains the way to replace the body when the parent has not opened a hole.

### Dialect

`ELASTICSEARCH` is rendered as query DSL and parsed before the search runs. Elasticsearch and OpenSearch share that dialect. An engine that needs a different clause points its slot at a child template. `POSTGRES` stores Mustache text for a future SQL compiler. Search V3 does not execute it. A `next` URN, a route target, or an explicit `SearchFlags.searchTemplate` of a different dialect fails the search.

### Capabilities

`apiVersion` is the template contract. This release implements 1 and 2. A template that sets a semantic field must resolve to 2. The merged version is the highest in the parent chain, so a child cannot lower it, and it is 1 when no template in the chain sets it. Any other value is rejected when the template is written and when it is loaded.

`capabilities` names the sections this body declares. Unset keeps the parent list. An empty list drops it. A non-empty list replaces it. Declaring a name does not turn a section on. Any other name is rejected when the template is written and when it is loaded.

| Name                      | What it names                                    |
| ------------------------- | ------------------------------------------------ |
| `keyword`                 | The keyword query                                |
| `rescore`                 | Engine rescore                                   |
| `functionScore`           | The function score wrapper                       |
| `routes`                  | Ordered routes                                   |
| `ladder`                  | The ladder                                       |
| `aggregations`            | Facets                                           |
| `structuredPropertyQuery` | Structured-property clauses                      |
| `normalization`           | Wildcard, synonym, fuzzy, and broad-query limits |
| `fieldOverrides`          | `fields`                                         |
| `highlights`              | `highlightFields`                                |
| `sort`                    | Sort                                             |
| `boosts`                  | List-membership weights                          |
| `fieldValueScores`        | Numeric field and property scores                |
| `entityTypeBoosts`        | Entity-type weights                              |
| `queryFragments`          | Query holes                                      |

After parent merge, a ranking preset other than `none`, a window sizer other than `fixed`, a semantic shape whose capability this build does not advertise, a bridge, and a merge join fail the search. See [Embedding profiles and gates](#embedding-profiles-and-gates). A `knn` with only `field` and no `combine` runs through document rerank when the request includes the Document entity; without it the search fails rather than drop that section. A `knn` with both `model` and `field` and no `combine` is read as keywordRerank, which keeps keyword order when it cannot rerank. On scroll, `next` is unread, so a `next` this release cannot run does not fail the scroll. On a search, it does. Scroll still skips the sections that a cursor cannot continue. See [Search and scroll](#search-and-scroll) and [Document rerank](#document-rerank).

### Field weights

`fields` exists so a template can change which shared fields are searched, and how hard, without a code change. The default set is every indexed field whose `v3.queryByDefault` is true, at that field's `v3.boostScore`, plus every structured property whose `searchConfiguration.queryByDefault` is true, at that configuration's `boostScore`. When `v3.boostScore` is unset, Search V3 uses the top-level `boostScore`. When `v3.queryByDefault` is unset, the field stays out of an unqualified Search V3 query; set it true to opt in. The top-level flags stay the Search V2 values. The indexed field is the shared field at degree 0, and `<sharedField>_<degree>` above that. Column degrees stay out of that set. Those annotation values are schema defaults: `v3.boostScore` does not increase with degree on one entity and shared field. Schema validation requires one degree for an aspect location and one `v3.queryByDefault` and `v3.boostScore` for an indexed name, including across entities. See [Documents](#documents). A structured property has one definition, so its `queryByDefault` and `boostScore` are the same on every entity type that definition allows. The annotation or definition weight applies only where the template sets no weight, and the template's field set and weights are what the request runs.

`override` sets the weight of a field that is already in the queried set. One field, one weight. That weight is what the weighted field list uses, and what a structured property's generated whole-value term uses as its boost. In the list, text is queried at that weight and the stemmed subfield at half, except on the name fields, which have no stemmed subfield. A numeric boost written in the body stays the boost the author wrote. Override does not change `nameAndValue.weight`. The same documents match. `rescore` and `ranking` read the resulting score. They do not carry a field-weight list of their own. An `override` of a field that is not in the queried set is rejected at write time.

`add` puts a field into the queried set at the weight written on the entry, so a document that matches only that field can appear. `remove` drops a field, so a document that matched only that field can disappear. `replace` replaces the whole list and each weight. `replace` cannot be combined with `add`, `remove`, or `override`.

A child that only overrides a weight inherits the parent's query, clauses, ladder, rescore, ranking, and the rest of `fields`. A saved ranking is that child. The section it sets is `fields.override`. It may also set `ranking` when the preset itself should change. It does not set `query` or `fields.remove`. There is no second ranking entity.

```yaml
parent: urn:li:searchTemplate:fulltext
fields:
  override:
    - fieldName: name
      boost: 10
```

`name` remains queried. A hit that matched on `description` still matches. Order can change because `name` is `^10` in the weighted field list. A numeric boost written in the body stays the boost the author wrote.

Each degree is its own indexed field. Each is in the default queried set when its own annotation sets `v3.queryByDefault`, and each takes that annotation's `v3.boostScore`. Column tags are `tags_1` and are left out of an unqualified query, so the example adds them. An override applies only to a degree already in the queried set, and an override of a degree outside the set is rejected. The non-increasing rule binds annotation defaults, not templates. A degree that no annotation indexes cannot be named.

The merge key is the indexed name. An override of `tags` leaves `tags_1` alone. Removing `tags_1` keeps a column tag from matching. Removing `tags` keeps the dataset's own tags out of the query and still searches the other degrees.

```yaml
parent: urn:li:searchTemplate:fulltext
fields:
  override:
    - fieldName: tags
      boost: 8
  add:
    - fieldName: tags_1
      boost: 2
    - fieldName: tags_2
      boost: 3
```

The same shape covers glossary terms and owners. A clause, a list boost, a facet, or a highlight names the indexed field. A filter that matches more than one degree is a disjunction of those fields. Sort names score, then URN, or a numeric usage field.

`fields.add`, `fields.remove`, `fields.replace`, and `fields.override` choose which fields are queried and the weight of each. The rendered `query` section is the body. The author writes the `term`, `prefix`, `wildcard`, or `match` clause, and the boost and `dis_max` around it. The builder substitutes resolved field paths into that body and fills `{{&defaultQueryFieldsJson}}` from the queried set. It does not emit a clause because a mode was set.

The composed keyword query is that rendered body, then one generated whole-value term per included structured property, then the name-and-value group when `nameAndValue.weight` is set, then `clauses` and `functionScore`. Search may still attach `rescore`, `ranking`, and `knn`. Scroll runs the composed keyword query and drops those three.

A structured property is named by its definition URN and changed with the same `add`, `remove`, and `override` operations. Those operations, and `replace`, decide which properties are queried and which join name-and-value expansion. `nameAndValue.weight` is not an override. Every included property gets a whole-value term at its field weight, including when the body also names that property. `fields.override` changes that term's boost and the property's `path^weight` in the weighted field list. It does not rewrite a numeric boost written in the body, and it does not change `nameAndValue.weight`. Removing the field drops it from the list, drops its whole-value term, and drops its name-and-value clause. After render, a clause that targets only the removed field is removed. A multi-field clause loses that field and is removed when no field remains. See [Structured properties](#structured-properties).

Inside the weighted field list, equal weights tie and a higher weight ranks above `name`. A numeric boost written in the body stays the boost the author wrote. A property joins a `dis_max` when the author fills `extraDisMaxQueries`, or when a replaced `query` writes its path there. See [Query holes](#query-holes) and [Ranking](#ranking).

```yaml
parent: urn:li:searchTemplate:fulltext
fields:
  add:
    - fieldName: urn:li:structuredProperty:businessName
      boost: 10
```

This `add` puts Business Name in the queried set. It gets a whole-value term at boost 10, and its analyzed path joins the weighted field list at `^10`. A name-only clause the author wrote stays name-only. See [Structured property references](#structured-property-references).

### List boosts

List membership is one structured boost, the same closed shape as Azure's tag function and Algolia's optional filters. The list is a parameter. The boost is not a free snippet.

Each entry names a field, an injectable, and a weight. The injectable is `viewerUrn`, `viewerGroupsJson`, `viewerDomainsJson`, or `domainsJson`. What each value contains is in [Render context](#render-context). `viewerUrn` is that entry with one value.

The service renders a `terms` filter on the field and applies the weight. `score_mode` and `boost_mode` stay on `functionScore`. An empty list adds no function, so a platform-wide policy leaves the `domainsJson` boost unfired. A child that sets `boosts` replaces the parent's list. Leaving `boosts` unset keeps it.

```yaml
boosts:
  - field: domains
    values: domainsJson
    weight: 1.5
```

`functionScore` stays the wrapper for a boost that is not list membership. It holds `functions`, `score_mode`, and `boost_mode`. Shipped keyword templates set `score_mode` to `multiply`, so each matching weight scales the text score and a document that misses a filter is unchanged. Each function is a score function the engine accepts, such as `weight`, `field_value_factor`, or a decay function. A filter on that function is a snippet the author writes for a term on one literal, a numeric field, or a decay. The snippet and the function's other fields reference parameters, the same way a weight references `{{deprecatedBoost}}`. The service supplies those values and leaves that function list as the author wrote it. A snippet that references `viewerUrn`, `viewerGroupsJson`, `viewerDomainsJson`, or `domainsJson` is rejected at write time. A clause may still reference those values to narrow.

### Highlight fields

Which field matched is read from values already on the returned page, for the fields this request opted into and this query searches. The check runs after ranking and page selection, so a discarded window row is not scanned, and it stops at the first matching value in each field. An empty set fetches nothing. The default query stays one body.

A named query identifies a field only when that field is its own clause. A `multi_match` does not say which inner field matched. Lucene already scores a sub-query per field inside that one body, so a `_name` on every field costs template size rather than query time. The template does not grow one per queried field, including every degree of separation and every structured property the query searched. An author may still name a distinct clause. That name is what a structured-property match reports. It does not replace the field check.

A structured-property match reports the definition URN and the matched value. It also reports `exact`, `prefix`, `contains`, or `nameAndValue` when the matching clause has that `_name` and names one structured-property field. The author writes `exact`, `prefix`, or `contains` on the clause. The executor writes `nameAndValue` on the expanded name-and-value clause. A missing name, a reused name, or a clause that names several properties leaves the mode unset. The report does not invent one. Shared-field matches stay indexed names. Write time does not require these names. The report is what a result card uses to show the value that matched. Result-card layout stays outside this contract. It is a companion dependency of this Search V3 result.

The comparison uses values on the returned document. A shared field filled only by `copy_to` is not stored, which keeps that cost off every write, so the check reads the source fields copied into it. A keyword is the value itself, folded the same way as the folded copy when the match was case-insensitive. Analyzed text uses the same approximate mark the UI applies. That mark can miss a hit found only through a stem, a fuzzy edit, a synonym, or a phrase with slop. The drift is accepted. The engine highlighter stays off on a template request.

Shipped templates leave `highlightFields` unset, so the default set is empty. A template opts fields in only when that cost is warranted. A child changes the set with `add`, `remove`, and `replace`. `replace` cannot be combined with `add` or `remove`. An explicit empty section clears the set. A request applies the same section after the template set. `customHighlightingFields` replaces that set outright, including a V2 subfield normalized to its indexed root, and the request section is not applied on top of it. `skipHighlighting` then clears whatever remains. The section has no weight. It names an indexed field, including a degree field such as `tags_1` and a structured property such as `structuredProperties.tier`, which is read where it is stored because `copy_to` does not put its value on the shared field. A field this query does not read, an aspect path, a wildcard, or a subfield the index does not store is dropped. The engine highlighter stays off. A non-empty set fetches those source paths, including the fields copied into a selected shared field, and matches them on the final page.

### Examples

A child of `fulltext` that buries deprecated assets. Column name, description, tags, and glossary terms are already left out of an unqualified query.

```yaml
parent: urn:li:searchTemplate:fulltext
clauses: |
  { "must_not": [ { "term": { "deprecated": true } } ] }
functionScore: |
  {
    "functions": [
      { "filter": { "term": { "deprecated": { "value": true } } }, "weight": 0.5 }
    ],
    "score_mode": "multiply",
    "boost_mode": "multiply"
  }
```

Leaving `knn` or `ranking` unset keeps the parent's section. Setting `knn` to empty drops an inherited rerank and leaves the ranking-step order. Setting `ranking` to empty drops an inherited preset and leaves the query order. Setting only `query` leaves an inherited `ladder` in place, so the child still hands off through `next`.

A child that queries a structured property the definition left out names the definition URN and a weight. See [Structured properties](#structured-properties). This `add` puts Business Name in the queried set. It gets a whole-value term at boost 10, and its analyzed path joins the weighted field list at `^10`. A name-only clause the author wrote stays name-only. A second child suppresses a property the definition included and overrides the weight of one already in the set. That override changes the steward whole-value term and its entry in the weighted field list. It does not change `nameAndValue.weight` or a numeric boost written in the body. `name` stays at its `v3.boostScore` because neither child sets `replace`. The parent's `fields` keys merge with these. See [Inheritance](#inheritance).

```yaml
parent: urn:li:searchTemplate:fulltext
fields:
  add:
    - fieldName: urn:li:structuredProperty:businessName
      boost: 10
```

```yaml
parent: urn:li:searchTemplate:fulltext
fields:
  remove:
    - fieldName: urn:li:structuredProperty:certification.status
  override:
    - fieldName: urn:li:structuredProperty:steward
      boost: 4
```

To rank Business Name in the same `constant_score` as `name`, fill the fulltext hole. The fragment is one more branch of that `dis_max`. The boost is the boost written in the fragment. `fields.add` does not change it. Setting `query` still replaces the whole body when the clause has no hole to fill. See [Query holes](#query-holes).

```yaml
parent: urn:li:searchTemplate:fulltext
queryFragments:
  extraDisMaxQueries: |
    {
      "constant_score": {
        "_name": "exact",
        "boost": 1000,
        "filter": {
          "term": {
            "{{urn:li:structuredProperty:businessName}}.keyword": {
              "value": "{{unquoted}}",
              "case_insensitive": true
            }
          }
        }
      }
    }
```

Shipped fulltext enables analyzed value and exact value matching. Name-and-value runs when its weight is set, which is what makes `certification status gold` match the value `gold`. The expansion rules are in [Name and value](#name-and-value). For a string property whose display name is Certification Status, whose qualified name is `certification.status`, and whose assigned value is `gold`:

```yaml
parent: urn:li:searchTemplate:fulltext
structuredPropertyQuery:
  nameAndValue:
    weight: 8
```

| Query                         | What matches                                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------------------------------- |
| `gold`                        | The value clause. The whole query is the term `gold`.                                                   |
| `certification status gold`   | The name-and-value clause for that definition. The remainder `gold` is the assigned value.              |
| `certification status silver` | Neither clause for that value. The remainder is not `gold`, and the whole query is not the term `gold`. |
| `certification status`        | No name-and-value clause. The remainder is empty, so possessing the property is not a hit.              |

A child that prefers the caller's domains names the field, the injectable list, and the weight. Leaving `functionScore` unset keeps the parent's deprecation weight.

```yaml
parent: urn:li:searchTemplate:fulltext
boosts:
  - field: domains
    values: domainsJson
    weight: 1.5
```

Point a slot at the child and every search of that intent renders it. Pass the URN on one request with `SearchFlags.searchTemplate` and only that request does.

The same child shape covers the other common changes:

- a `functionScore` boost for a tag, glossary term, platform, or `_entityType`
- a list-membership boost
- a `must_not` to exclude
- a fragment in a hole the parent opened
- a replaced `query`
- a `rescore` window
- a `ranking` preset
- a single field weight

### What stays fixed

The rendered query is combined with the caller's filter, soft delete, hidden lifecycle stages, the requested entity types, latest-version filtering, and view authorization. On a vector rung, the caller's filter and those system filters move inside the kNN request, and view authorization is applied after the top-k.

Paging and the result cache stay in the service. System templates are not editable except by the system actor.

`fields.override` leaves the hit set unchanged. `fields.add`, `fields.remove`, and `fields.replace` change which documents match. Owners group cross-match, filter order, result-card layout, Column Views, and favorites are product surfaces outside the query contract. The matched-field report names the structured property and the value that matched so a card can show Business Name. The card layout itself stays outside Search V3.

The template cannot express the following. A section that tries is rejected, or, when the process does not implement the named preset, fails the capability check:

- Painless and `script_score` in any section. Numeric boosts are `field_value_factor` or a filter weight. Formulas that read several signals are a `ranking` preset.
- A regex inside a query body, a ladder predicate, or a Mustache expression. Query routing is the ordered `routes` list in [Selection](#selection). A ladder predicate stays a closed name. A shape flag stays a value the service computed.
- Word-gram fields, ngram fields, and the per-field analyzer chain. One weight per field, as [Field weights](#field-weights) specifies: text at that weight, stemmed at half except on the name fields, exact on the keyword.
- `k` and `num_candidates` on document rerank.
- Offset pagination of a vector rung.
- A template body that wraps, percolates, or does a terms lookup.

## Query execution

With templates enabled on the V3 keyword read path, one template is selected as the first rung and run as follows.

### Search and scroll

Search is a `from`/`size` request. Scroll is a point-in-time query continued with `search_after`. The cursor is the sort values of one query. A ladder, a fused hit list, a rescore window, a ranking window, and a document rerank each produce an order that cursor cannot continue, so scroll does not run them.

The same template URN serves both. On scroll the ladder section is left unread, and the scrolled request is the composed keyword query: the rendered body, generated whole-value terms, the name-and-value group, `clauses`, `functionScore`, and `boosts`. A search that stopped on a later rung is a different result. A scroll cursor does not continue that page.

| Feature                                                 | Search                                                            | Scroll                                                    |
| ------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------- |
| Composed keyword query                                  | Runs                                                              | Runs                                                      |
| `query`, `clauses`, `fields`, `functionScore`, `boosts` | Part of that query                                                | Part of that query                                        |
| Whole-value terms and name-and-value                    | Run                                                               | Run                                                       |
| `aggregations`                                          | Run when the request asks                                         | Run when the request asks                                 |
| Highlight                                               | Final-page source match when the set is non-empty                 | Same, on the scrolled page                                |
| `sort` and `sortCriteria`                               | Run. `sortCriteria` also replaces `rescore`, `ranking`, and `knn` | Run. The default is score, then URN                       |
| `rescore`                                               | Runs on the top `window_size`                                     | Dropped. The engine rejects `rescore` with `search_after` |
| `ranking`                                               | Reorders the fetched window                                       | Dropped                                                   |
| `knn` document rerank                                   | Reorders inside the ranking window                                | Dropped                                                   |
| `routes`                                                | Followed before the body runs                                     | Followed before the body runs                             |
| `ladder`, including a parallel join and fusion          | Walks `select`, `stop`, and `next`                                | Left unread. `next` is not followed                       |
| Vector retrieval, `bridge`, and hybrid retrieval        | A window sliced from offset 0                                     | A vector rung fails the scroll. Ladder members are unread |

`includeExplain` and `searchType` apply to the query that was served on either API. System filters and view authorization wrap the keyword query on either API. Scroll runs the composed keyword query, including whole-value terms and name-and-value clauses, and still drops rescore, ranking, and knn. See [Name and value](#name-and-value).

An empty query or `*`, with no `SearchFlags.searchTemplate`, selects the list query and runs on the API the caller used. It does not force scroll. That query has no `next`, so a search does not walk, and scroll does not follow `next`. Scroll still drops `rescore`, `ranking`, and `knn`. A caller `sortCriteria` is the list order. An explicit template URN skips that selection. Its merged routes still run, and on a search the ladder runs after routing finishes. See [Selection](#selection).

### Query sequence

1. **Bind.** A template URN is resolved from the caller's `SearchFlags.searchTemplate` or from the seed for the surface, engine, and fulltext flag. The parent chain is merged, and the merged `routes` are matched against the trimmed query. The first match loads that target and repeats. A query that matches none stays on the current template unless that template sets a default target. That URN is the first rung.

   Each merge keeps the parent cycle check and a parent depth of five. Route hops keep a separate visited set and hop budget. The set contains each URN whose routes are evaluated, not the parents loaded to merge it. A repeated route URN or a spent hop budget fails the search. A route target of another dialect fails the same way.

   After the merge, `apiVersion` must be 1 or 2, 2 when a semantic field is set, and every declared capability must be a name this release accepts. A ranking preset other than `none`, a window sizer other than `fixed`, `vectorThenKeyword`, `vectorFilter`, `vectorBody`, `profiles` on a keyword rung, the native embedding placeholders, a bridge, and a merge join fail the search. So does a vector rung on scroll, and a `scope` that leaves out a requested entity type on a scroll, filter, aggregation, or count, because those do not walk a ladder that could serve that type. The service does not drop that section and return keyword order. On a search, a `next` this release cannot run fails the same way. On scroll, `next` is unread, so that check waits until a search follows it.

   The cache key combines the service discriminators for the request with the final template and the ladder rungs this request will run: every rung consulted, each modification time, and whether a rerank is armed on the rung that was served. A route change that selects a different template changes that identity. The matched route chain is returned with `includeExplain`. It is not a second cache key. When a route with `match` conditions fired, the key also includes the request inputs those conditions read: the requested entity types, the surface, and their semantic eligibility. The surface is not otherwise part of the key, except `semanticSearch`: a page is cached there only once a vector rung served it, so it is never shared with another surface's page from the same template. When the served query expanded `structuredPropertyQuery`, the key also includes the structured-property catalog stamp used for that expansion. `lastModified` already separates two templates whose composed queries differ. `subIntents` are not a second routing mechanism.

2. **Walk.** On a search, the ladder tries the rung. Each rung is assigned the requested entity types in its `scope`, all of them when `scope` is unset, and searches only those. Selection failure, or an empty assignment, skips it and follows `next` without running the query. A rung that runs and matches its stop is kept. A rung that runs and misses its stop follows `next`. The last rung, which has no `next`, is kept either way, including a total of zero. A template with no ladder section is a single rung: it always runs and its result is kept.

   Scroll does not walk. It runs the resolved template's composed keyword query. See [Search and scroll](#search-and-scroll).

   A kept keyword rung supplies `query`, `clauses`, `functionScore`, `aggregations`, and `sort`. It supplies engine `rescore`, `ranking`, and `knn` on a `from`/`size` search. Scroll drops engine `rescore`, `ranking`, and `knn`, and sorts by score, then URN. A caller `sortCriteria` replaces all three on a search. On scroll it replaces the score, then URN sort.

3. **Rank and slice.** On a `from`/`size` search, for the standard keyword rung the walk kept, the service renders `query`, attaches generated whole-value terms and the name-and-value group, merges `clauses`, wraps `functionScore` (the author's functions plus one weight function per structured boost), and attaches engine `rescore`. It then runs `ranking` on that window. The ranking step reads the score from the engine. Scroll runs that same composed keyword query and does not attach `rescore`, `ranking`, or `knn`.

   When `knn` is armed, semantic read is on, the request includes documents, and the sort is by score, document rerank reorders the document rows of the search's first result batch, read from offset 0 (`SEARCH_SERVICE_BATCH_SIZE`, 100 rows by default; the search is batched whether or not the search cache is on). It reads the score from the ranking step. A page inside that batch is a slice of the reranked order, and a page in a later batch keeps keyword order. The keyword total and facets stand.

   On a search, a later rung's `ranking` or `knn` is not applied when an earlier rung was kept. Scroll does not rank or rerank. The scroll cursor is the engine's sort values, and an in-memory reorder cannot be continued. A vector rung does not follow the keyword order. See [Vector retrieval](#vector-retrieval).

System filters, the caller's filter, and view authorization wrap every keyword rung that runs. Each body is validated at write time. A vector rung applies the caller's filter and the system filters inside the kNN request, and view authorization after the top-k.

`includeExplain` returns the explanation of the query this walk served. `searchType` opts a keyword request into DFS query-then-fetch when consistent IDF across shards matters.

### Selection

A caller-supplied template URN is the entry. Its merged `routes` still run, so naming a child of a seed follows the routes that child inherited. An explicit empty `routes` list is what pins one template. On a search, the ladder then follows `next`. Scroll does not. Otherwise the surface, the engine, and the fulltext flag select a seed, and that seed's routes choose the template. Lineage search uses the same seed as entity search. The graph walk adds a URN filter outside the template. Browse text uses the `search` router. Browse path, depth, and group aggregation stay in Java. `fulltext=false` selects the structured seed, because that flag is not part of the query text. Both seeds carry the identity and list routes.

Routes match the trimmed query as the search receives it: GraphQL keyword search escapes `/` as `\/` first, and semantic search and the Rest.li and OpenAPI search APIs pass the query as typed. Each pattern is a full-string match, so a prefix consumes the rest of the query. The first match wins. A route may also set `match` conditions. It matches only when its pattern matches and every condition holds, and otherwise the next route is tried. The conditions read the full request: `entityTypes` holds when every requested entity type is in the list, and `surfaces` when the request's surface is. `semanticEligible` holds when its value equals whether every requested type is enabled for semantic search, and `anySemanticEligible` when its value equals whether at least one is. Eligibility reads `semanticSearch.enabledEntities` alone, not the other entity gates. A condition this release does not know fails the search when its route's pattern matches. A query that matches none stays on the current template unless that template sets a default target, so a seed does not need a catch-all `.*` route. The structured prefix is the characters `\/q `, including the leading backslash. Identity, list, and the structured routes precede the quoted route.

On the search seed, a miss stays unless that seed sets a default target.

| Order | Pattern                                  | Template                                |
| ----- | ---------------------------------------- | --------------------------------------- |
| 1     | `(?i)^(?:urn:.*\|(?:s3\|gs\|hdfs)://.*)` | `urn:li:searchTemplate:identity`        |
| 2     | `^$`                                     | `urn:li:searchTemplate:list`            |
| 3     | `^\*$`                                   | `urn:li:searchTemplate:list`            |
| 4     | `^\\/q ["'].+["']$`                      | `urn:li:searchTemplate:structuredExact` |
| 5     | `^\\/q .*`                               | `urn:li:searchTemplate:structured`      |
| 6     | `^["'].+["']$`                           | `urn:li:searchTemplate:exact`           |

On the structured seed, a miss stays unless that seed sets a default target. With `fulltext=false`, one unquoted token stays on that seed.

| Order | Pattern                                  | Template                                |
| ----- | ---------------------------------------- | --------------------------------------- |
| 1     | `(?i)^(?:urn:.*\|(?:s3\|gs\|hdfs)://.*)` | `urn:li:searchTemplate:identity`        |
| 2     | `^$`                                     | `urn:li:searchTemplate:list`            |
| 3     | `^\*$`                                   | `urn:li:searchTemplate:list`            |
| 4     | `^\\/q ["'].+["']$`                      | `urn:li:searchTemplate:structuredExact` |
| 5     | `^["'].+["']$`                           | `urn:li:searchTemplate:structuredExact` |

An empty `routes` list pins the template. A hop to a pinned template does not route again.

```yaml
routes:
  - queryRegex: (?i)^(?:urn:.*|(?:s3|gs|hdfs)://.*)
    template: urn:li:searchTemplate:identity
  - queryRegex: ^$
    template: urn:li:searchTemplate:list
  - queryRegex: ^\*$
    template: urn:li:searchTemplate:list
```

A route target may itself have routes, so an operator can insert another router without a client change. Patterns are RE2. Java-only lookarounds and backreferences from a legacy `queryRegex` are rejected. Write time compiles each pattern, checks its length, checks the list length, checks that the target exists and uses the same dialect, and rejects a route whose target is the template being written. Bind time fails the search when a hop repeats a URN or spends the hop budget. Two routers may point at each other when no one query can follow both patterns. Parent cycles stay on the parent chain and use the existing depth of five. They are not route hops.

Fully qualified name is not a route. `fullyQualifiedName` and `deepPath` are shape flags. The old exploratory tier is a later ladder rung. A template with no `next` is the last rung. A deployment that wants strict-then-broad writes a child, sets `next`, and points the slot at the child. An empty query or `*` selects list and runs on the API the caller used, so that path does not force scroll and does not walk. On a search, after routing, the walk follows `next` when the chosen template has one. See [Search and scroll](#search-and-scroll).

A quoted query selects exact, and that query requires the phrase. A single term with no spaces is not exact. It stays on the seed. Whole-value identity selects `identity`.

Search V3 selects the template in this order: an explicit `SearchFlags.searchTemplate`, otherwise the seed for the surface, engine, and fulltext flag, then that template's ordered routes. A View-associated template is a later Search V3 product input. It is not a Search V2 or Search V2.5 ranking method. Authorization stays outside the template. Result-card layout is a companion of the matched-field report, not a template section.

A multi-index query can still name an aspect field that only one of the indices maps; the others match nothing for that clause. A template that should apply only to some entity types, because it names such a field, sets `scope`. A route's `entityTypes` condition can send a request to such a template, and on a search each rung searches only the requested types its scope covers. See [Query ladder](#query-ladder).

Routes are not a ladder. A route looks at the query text and runs before any search, on search and on scroll. A ladder `select` is a closed predicate on the request, checked before a rung runs. `stop` and `next` look at the result. A route does not see the result, and a ladder predicate is not a regex.

### Query ladder

A ladder is a walk across templates. Each template that takes part is a rung. The rung's own sections are the query that runs. The ladder section does not hold a second body. It holds the rules for entering this rung, the rules for keeping its result, and the URN of the template to try next.

A template with no `ladder` section is one query. Setting `ladder` is how a cheap or strict template hands off to a broader one. The two-step cascade in the current code, where one template carries an embedded `lightQuery` body, is this walk with the next body inlined. The design points at the next template instead, so a third rung is another URN and the stop rules are data on the rung.

Scroll does not run this walk. The cursor continues one query, and a ladder chooses among queries. Scroll leaves `select`, `stop`, `next`, and any parallel group unread and serves the composed keyword query. See [Search and scroll](#search-and-scroll).

```yaml
ladder:
  select:
    - scoreSort
    - pageSize
    - "-columnFilter"
  stop:
    - minHits: 1
    - emptyWhen: spacelessIdentifier
  next: urn:li:searchTemplate:fulltextBroad
```

`select` is a conjunction, checked before the search. Every predicate has to match or this rung is skipped and the walk follows `next` without running the query. An empty `select` means the rung is always attempted. `stop` is a disjunction, checked after a successful search. Any one match keeps the result: the hits, the total, the facets, and the ranking. Later rungs are not searched, and their scores are not merged with this one. `next` is unset on the last rung. Its result is returned whether or not it would have stopped, including a total of zero.

A quoted query does not split across rungs. Its phrase clause stays on the exact template. A single term with no spaces is not exact.

The predicates are a closed set the service implements. A template chooses which of them apply. It does not supply an expression or a script. A leading `-` inverts a predicate.

**Selection** looks at the request and the query shape, which are known before any search:

| Predicate             | Matches when                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------------- |
| `scoreSort`           | The sort is omitted or is score descending.                                                                     |
| `pageSize`            | The page size is above zero. A count or aggregation request skips a rung that requires it.                      |
| `columnFilter`        | The filter names a column field. A leading `-` inverts it.                                                      |
| `singleTerm`          | The query is one word.                                                                                          |
| `multiWord`           | The query has two or more words.                                                                                |
| `dotted`              | The query contains a dot.                                                                                       |
| `spacelessIdentifier` | The query has no space, and has either six or more digits or four or more parts split on `.`, `_`, `-`, or `/`. |
| `identity`            | The query is a URN or an `s3://`, `gs://`, or `hdfs://` path.                                                   |
| `longQuery`           | The query has four or more tokens.                                                                              |
| `semanticEligible`    | Every entity type assigned to the rung is in `semanticSearch.enabledEntities`.                                  |
| `anySemanticEligible` | At least one entity type assigned to the rung is in `semanticSearch.enabledEntities`.                           |

`semanticEligible` and `anySemanticEligible` read the rung's assignment, not the whole request, so a rung scoped to Document is eligible on a request that also names Dataset. They are select-only: `emptyWhen` does not accept them.

A rung that requires `scoreSort`, `pageSize`, and `-columnFilter` is attempted only for an ordinary ranked search. A facet request or a column-only filter skips it and runs `next`. The page offset is not a predicate. Stop uses the total, which does not change from page to page, so the same search keeps the same rung on every `from`/`size` page. Scroll does not use that rule, because scroll does not walk.

`multiWord`, `dotted`, and `spacelessIdentifier` are also shape flags the body can reference. The other shape flags are body and window-sizer inputs. They are not selection predicates. See [Query variations](#query-variations).

**Stop** looks at the result, and at the query shape when the result is empty:

| Predicate   | Keeps the rung when                                      |
| ----------- | -------------------------------------------------------- |
| `minHits`   | The total is at least N. The usual value is 1.           |
| `emptyWhen` | The total is zero and the named shape predicate matches. |

`emptyWhen: spacelessIdentifier` is the case where an empty strict search is the answer, because the broader query would not add a hit. The same flag is the cascade skip for both the six-digit hash and the four-part delimiter case. A shard failure or an exception is not a stop predicate. The walk follows `next` anyway, so a broken rung cannot hide results. When the failing rung is the last one, the search fails.

**Next** is a template URN. The walk merges that template on its own. A rung renders its fields, clauses, function score, aggregations, sort, rescore, ranking, retrieval, and `knn`. Nothing is copied from the rung that just ran except through ordinary parent inheritance, when the next template declares that parent. A cycle, a missing URN, a different dialect, a section this release will not run, or a walk longer than the parent-chain limit fails the search. `next` set with an empty `stop` is rejected at write time on a sequential rung. An empty `stop` matches nothing, so that rung would never be kept. A parallel `any` or `allEmpty` group may set `next` without a group `stop`, because the join decides. A `merge` group that sets `next` requires a group `stop`. `next` of another dialect is rejected the same way an explicit template of another dialect is.

The cache key includes every rung the walk consults, with that rung's modification time. A change to `select`, `stop`, or `next` on the entry template changes which URN is served without changing the winner's body, so the entry's stamp has to be in the key as well. The kept rung is the served rung. A rerank is armed only when a rung has `knn`. The route chain is part of this key only for a request that sets `includeExplain`, whose page reports it. The final template's stamp is, and so are the request inputs of a conditional route that fired.

An explicit `SearchFlags.searchTemplate` starts routing at that URN and, on a search, follows `next` after routing finishes. A caller who wants one executor names a template whose merged `routes` are empty and which has no `next`. Scroll starts at that same URN, follows routes, and does not follow `next`; it requires the standard executor.

Surface, engine, and the fulltext flag choose the seed. Routes choose the template under that seed. The ladder does not replace that routing. It decides which template under the chosen entry actually searches. A template with no `next` is the last rung. A deployment that wants strict-then-broad writes a child, sets `next`, and points the slot at the child. `scope` limits a rung, a parallel member included, to the requested entity types it lists. A rung whose scope covers none of them is skipped, as a selection failure is. A stage can run several templates together and then continue in sequence. [Parallel rungs](#parallel-rungs) describes when that group is warranted.

### Parallel rungs

The walk above is one `next` at a time, on a search. A parallel rung is a stage that runs several template URNs together, joins their results, and then optionally follows one sequential `next`. The default stays sequential. A group is explicit. Scroll does not run a group. See [Search and scroll](#search-and-scroll).

```yaml
ladder:
  parallel:
    - urn:li:searchTemplate:exactName
    - urn:li:searchTemplate:identifier
  join: allEmpty
  next: urn:li:searchTemplate:fulltextBroad
```

Each member is a template, with its own `select` and `stop`. Each member is assigned the requested entity types in its own `scope`, independently of the others, so two members whose scopes overlap both search the shared type. The group adds a join and one `next` for the stage after the group. Three joins cover the cases below.

| Join       | What is served                                                                                                                           | When `next` runs                                                                                      |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `any`      | The first member to match its stop. Members still running are cancelled.                                                                 | Every member finished without matching its stop.                                                      |
| `allEmpty` | After every member finishes, the first listed member whose stop matched.                                                                 | Every member was skipped or finished without matching its stop.                                       |
| `merge`    | The member hit lists combined by `fusion`, up to `fusionCap`, with facets named by `facets`. The group does not issue another retrieval. | When `next` is set and the group's `stop` misses. A `merge` that sets `next` requires a group `stop`. |

`any` does not wait for a slower member once one stop has matched. `allEmpty` does wait, so listing the keyword template first serves it when both would have stopped. `any` and `allEmpty` decide `next` from member stops, so they can omit a group `stop`. `merge` combines the member lists with `fusion` on the group instead of adding them inside one `dis_max`. That combination does not issue another retrieval. `next` runs when the group's `stop` misses, so a `merge` that sets `next` requires a group `stop`. A member that times out, returns partial hits, or fails a shard is dropped, and the others are served. When no member remains, the walk follows `next`. When the group is last, the search fails.

`merge` names whose hits are kept, how many rows a later member may add, and whose facets are returned:

```yaml
ladder:
  parallel:
    - urn:li:searchTemplate:exactName
    - urn:li:searchTemplate:columnName
  join: merge
  fusion: fillFreeSlots
  fusionCap: 10
  facets: patchFromMembers
  stop:
    - minHits: 1
  next: urn:li:searchTemplate:fulltextBroad
```

| Fusion             | What is served                                                                                                                   |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `interleave`       | The member hit lists interleaved in listed order.                                                                                |
| `weightedBlend`    | One score blend across the member lists. Parameters are set on the group.                                                        |
| `fillFreeSlots`    | The first member's hits, then later members' hits in free slots of the window, up to `fusionCap`, skipping URNs already present. |
| `keywordPreferred` | The keyword member when it matched its stop, otherwise the vector member.                                                        |

| Facets             | What is returned                                                                    |
| ------------------ | ----------------------------------------------------------------------------------- |
| `firstMember`      | The first member's aggregations.                                                    |
| `patchFromMembers` | The first member's aggregations, plus the counts for rows `fillFreeSlots` appended. |
| `candidates`       | The vector-rung follow-up. Illegal on a keyword group.                              |

Two shapes that look like parallelism are not a group.

**One body.** Exact name and an identifier term against the same index belong in one `query`, as a `dis_max` or a `bool` `should`. That is one round trip, one total, and one set of facets. Splitting them into two searches repeats filters, authorization, and aggregations, and then needs a rule for which total to return. Column name is a field the exact and fulltext bodies can query directly, which is this form. That form is one body, not a parallel group.

**Speculative `next`.** The service may start the sequential `next` while the current rung is still running, and discard it when that rung stops. The ladder is unchanged. The broad query's latency hides behind the strict one. This is an execution choice, not a join.

The cases that need a real group are the ones that cannot be a single query body.

**Cheap probes, then one broad query.** An exact-name template and an identifier template run together. `join: allEmpty` serves whichever probe stopped, and runs the broad template only when both are empty. Use a group when the probes must not share a request: different indexes, or stop rules that disagree. When they can share a request, they are one first rung whose `next` is the broad template, and the group is unnecessary.

**Column recall as its own request.** Column recall and the non-dataset name supplement add hits to a query that already matched. A deployment points the exact slot at the group above when the column probe has to be its own request, with its own cap and its own timeout. `fusion: fillFreeSlots` keeps the first member's hits and fills free slots from later members, up to `fusionCap`. `facets: patchFromMembers` adds the counts for the rows `fillFreeSlots` appended, which is the column-facet correction the supplementary query does today. The group's `stop` is one hit, so `fulltextBroad` runs only when the fused total is zero. A member that times out or returns partial hits is dropped, and the others are served.

**Keyword retrieval beside corpus vector retrieval.** The keyword template and a vector template run together. The vector leg is not a clause of the keyword DSL, and it is not document rerank, which reorders the keyword window and adds no hits. `join: merge` keeps both hit lists. The group applies `fusion` and does not issue another retrieval. `keywordPreferred` serves the keyword member when it matched its stop, and the vector member otherwise. `weightedBlend` parameters are set on that group. `join: any` serves whichever retrieval matches its stop first and cancels the other, so there is no second list to fuse. Preferring the keyword result when both would have matched, without combining the lists, is `allEmpty`, with the keyword template listed first. Overlapping the two retrievals is the latency win. Unioning their hits is a recall change, and it is a different feature from document rerank. The merge group is hybrid retrieval for the Document entity. Vector-only hits wait on a fusion policy. See [Vector retrieval](#vector-retrieval).

**Entity templates, then a broadening step.** A dataset template and a chart template would run together because each index has its own body. `join: merge` fuses the cross-entity lists on the group. Its `next`, when the fused total misses the group's `stop`, searches more loosely across both. Each member's `scope` limits it to its entity type. Until a fusion policy exists, `join: merge` fails, `any` or `allEmpty` serves one member's page, and a vector member is never merged with a keyword member.

The cache key includes every member that ran and the join, along with the sequential rung if it ran. Two requests that differ in which member stopped do not share a hit.

### Query composition

`query`, `clauses`, `functionScore`, and `rescore` are separate because each one is a different kind of change, and a child should be able to replace one without rewriting the others.

The service builds one request from them, in order. It renders `query`, substituting resolved paths and filling the weighted field list from the queried set. It does not emit a clause because a mode was set. It then attaches one whole-value term per included structured property, at that field's weight, and the name-and-value `dis_max` when `nameAndValue.weight` is set. Both sit before `functionScore`. See [Name and value](#name-and-value) and [Structured properties](#structured-properties). `bool`, `dis_max`, and `match_all` are valid roots. A missing `query` renders as `match_all`. It merges `clauses` (`must`, `must_not`, `filter`, `should`) into the bool wrapped around that body. It wraps `functionScore` once, with that template's `functions`, `score_mode`, and `boost_mode`, and appends one weight function for each structured boost. It attaches `rescore` after the wrapper. A top-level `function_score` inside `query` is rejected, because the wrapper belongs in `functionScore`. `rescore` cannot be combined with `sort`, or with `boost_mode: replace`. While `rescore` is applied, `sort` is not attached, so an unset `sort` is not yet score, then URN. Scroll drops `rescore`, `ranking`, and `knn`, and that default sort returns. The composed keyword query still runs. The [query sequence](#query-sequence) attaches `aggregations` and `sort` on the rung that the walk kept. [Ranking](#ranking) runs after the engine returns.

`rescore` reorders only the top `window_size` hits of a `from`/`size` search. Scroll continues with `search_after`, and the engine rejects `rescore` on that request. The service drops `rescore`, `ranking`, and `knn` on every scroll execution and sorts by score, then URN. A standard template is not split to get there: the same URN serves search and scroll, and a scroll surface points at a different URN only when scroll should rank differently for a reason of its own. The scroll order is the query order, not the rescored order, and a scroll cursor is not a continuation of a rescored page. The rescored ranking exists only inside that window. Past it, the engine has not computed the order. A caller `sortCriteria` replaces engine `rescore`, `ranking`, and `knn` on a search. On scroll those three are already dropped, and `sortCriteria` replaces the score, then URN sort. The full split is in [Search and scroll](#search-and-scroll).

`sort` may name the numeric document fields `queryCountLast30Days` and `viewCountLast30Days`. Most Queried and Most Visited are those sorts, including when the caller sends them as `sortCriteria`. The `usage` ranking preset reorders the window by usage signals and does not replace either sort.

A property joins a `dis_max` when the author fills `extraDisMaxQueries`, or when a replaced `query` writes its path into a clause. [Ranking](#ranking) states where that order survives the later window. [Query holes](#query-holes) is the splice.

### Ranking

V2.5 order is decided after the query returns. An engine `rescore` block cannot carry that formula. Script scoring stays forbidden, so the formula cannot move into `functionScore` either. `ranking` names a preset, plus parameters. A stored `formula` is kept and is not evaluated.

```yaml
ranking:
  preset: usageAndMatchTier
  window: 100
  windowSizer: tokenCurve
  parameters:
    matchTierBand: 10
```

| Preset              | What the service does          |
| ------------------- | ------------------------------ |
| `none`              | Leaves the query order.        |
| `usage`             | Not executed. Selection fails. |
| `usageAndMatchTier` | Not executed. Selection fails. |

`windowSizer` is the same kind of closed set. `fixed` uses `window`. This release implements `none` and `fixed`. `usage` and `usageAndMatchTier` fail selection. `tokenCurve` fails selection unless the resolved ranking stores a non-empty signal list. That list does not change the window.

A ranking may store `signals`, an optional `formula`, or `ranking.template`. A reference and inline fields on the same ranking are rejected. `add` and `remove` patch the inherited list by signal name and may sit beside the reference. A non-empty `signals` list replaces the inherited list and cannot be combined with those patches. Signals or `ranking.template` require `rankingSignals`. A formula requires `rankingFormula`. Elasticsearch `rescore` is not combined with either. This release merges the stored section and does not score it.

A `postProcess` component at version 1 is a registered component. OSS registers an empty implementation when no other implementation uses that name and version. The empty implementation accepts the stored config, does not interpret it, and does not reorder hits.

`fields.override` changes the engine score the ranking step reads. It applies only to a field that remains in the queried set. `ranking` and `rescore` do not carry a field-weight list, and they do not copy the query weights into one. A saved ranking is a child whose section is `fields.override`, as [Field weights](#field-weights) specifies. `ranking` still only reorders the fetched window.

The author writes the boost and the `dis_max` in the body. Inside the weighted field list, equal weights tie and a higher weight ranks a property above `name`. A numeric boost written in the body stays that boost. A property joins that clause when the author fills `extraDisMaxQueries`, or when a replaced `query` writes its path there, and the boost on that clause stays the boost written there. Every included property also gets a whole-value term at its field weight. `functionScore` wraps the composed keyword query, and each structured boost is a weight function in that wrapper. A weight there can place another clause ahead in the engine score when the template applies it. `matchTierBand` is the `usageAndMatchTier` preset's own band inside the window. It is the boost the preset applies inside that window.

Those clauses run in the composed keyword query, before `functionScore`, engine `rescore`, `ranking`, and `knn`. Downstream scoring crosses them only when the selected template applies `functionScore`, `rescore`, `ranking`, or `knn`.

`rescore`, `ranking`, and `knn` then reorder inside their windows. Inside a window another clause can move ahead. Past those windows the served order is the query score. Scroll runs the composed keyword query and drops all three. A quoted query requires the phrase. See [Selection](#selection).

The name-and-value group sits in that query, before `functionScore`. It is its own `dis_max`. It is not a ranking preset. See [Name and value](#name-and-value).

Order on a from/size search, for the keyword rung the walk kept:

```mermaid
flowchart LR
  render["Composed keyword query, then engine rescore"] --> ranking["ranking reads that score"]
  ranking --> knn["knn document rerank, if armed"]
  knn --> slice["Slice the page"]
```

1. Render the composed keyword query: `query`, whole-value terms, the name-and-value group, `clauses`, and `functionScore`, then attach engine `rescore`.
2. Run `ranking` on that window. It reads the score from step 1.
3. Run `knn` document rerank, if armed. It reads the score from step 2.
4. Slice the page.

The ranking window is the set the service fetches from offset 0 and reorders before it slices. Engine `rescore` keeps its own `window_size` on that request. Ranking reads the score the engine returned for each hit in the ranking window. `keywordRerank`, when the page starts inside that window, reorders the same set, and a page that starts past the window keeps keyword order and is not a slice of the reranked order. The legacy `knn` document rerank reorders the search's first result batch instead, read from offset 0.

Scroll drops engine `rescore`, `ranking`, and `knn`, and sorts by score, then URN. A caller `sortCriteria` on a search replaces all three. On scroll they are already dropped, and `sortCriteria` replaces the score, then URN sort.

The combined ranking multiplier does not become a preset. It is a numeric field read with a missing value of `1.0`, written as a `field_value_factor` on the shared parent `functionScore`. An empty query, a quoted query, and full text select the `list`, `exact`, and `fulltext` templates through [Selection](#selection). A regex stays out of the query body. It is legal only as a `routes` pattern.

### Document rerank

`knn` reorders document rows already in the keyword window. It adds none and removes none. The keyword total and the keyword facets stand. The candidates are every document URN in that window. `k` and `num_candidates` are not fields.

```yaml
retrieval:
  combine: keywordRerank
knn:
  vectorWeight: 0.5
  lexical:
    inputMin: 0
    inputMax: 500
    steepness: 6
  blendFloor: 0.6
  others: keepKeywordPlaces
```

`retrieval.combine: keywordRerank` runs the keyword query, then rescores the eligible rows in its window with the `knn` blend. The blend is a sigmoid of the score from the ranking step, then `vectorWeight` against the normalized vector score. Each unset value takes the hybrid reranker's default shown above. `blendFloor` is the minimum normalized vector score, from 0 to 1, for a row to be blended. A row below it keeps its keyword position. It does not drop the row. The request flag `minScore` is a deprecated second gate on that same score. The section's own `minScore` is not read. `others: keepKeywordPlaces` applies to rows that are not rescored. They are not sent to kNN. They stay at the positions the keyword order gave them, and the rescored rows are reordered in the remaining slots. `model` and `field` are deprecated names of the embedding profile id. `knn` cannot be combined with a `sort` that renders clauses at write time. A `sort` that renders empty at write time is accepted: a query it orders keeps that order and is not reranked, and a query for which it renders empty is reranked.

A `knn` section with no `combine` that names both `model` and `field` is the earlier keywordRerank shape, and it is read as `keywordRerank`. Those names stay opaque, so the rerank resolves the entity's default profile. A `knn` section with only `field` is the legacy document rerank. It fails the search when document rerank is unavailable.

A `keywordRerank` rung binds whether or not the rerank can run, and the default hybrid reranker never runs on it. It reranks when this edition's hybrid read flag is on, semantic read is on, the entity types the rung searches include Document, the sort is by score, the query is full text, and the page starts inside the 100-row window. When the sort the served request applies is the rung's own `sort` (the request has no sort and the section renders one), that sort orders the window and the search keeps it. Only window rows whose entity type passes the entity gates are scored. Rows that fail the gates, rows without a vector, and rows under the floor keep their keyword places. The floor is the higher of `knn.blendFloor` and `SearchFlags.minScore`, both on the engine [0, 1] score, and a row at the floor is blended. The resolved profile must be the one the query embedding service embeds. When the rerank cannot run, fails, or times out, the search keeps keyword order and counts `search.template.keywordRerank.kept` with one reason, not `search.template.fallback`, because several reasons are expected states: `hybridReadOff`, `semanticReadOff`, `noDocument`, `noEligibleRows` (fewer than two rows can be blended), `profileNotEmbeddable`, `rejected` (the hybrid read executor was full, or the provider throttled the query embedding, as a vector rung reports it, unless the search's deadline had already passed, which reports `timeout`), `failed`, `timeout`, or `paused`. A throttle still counts toward the hybrid read pause, as a failure does, and a full executor does not. The keyword total and facets stand either way. When the rung cannot run document rerank at all, because semantic read is off or the types it searches have no Document, the rung fetches only the requested page, not the 100-row window. A rung's scope can leave Document out of a request that includes it. With `includeExplain`, `SearchResultMetadata.keywordRerank` reports the outcome, that reason, the eligible and ineligible rows in the window, and the `SearchFlags.minScore` deprecation with `knn.blendFloor` as its replacement. The cache identity of a `keywordRerank` rung adds the rerank's execution version, the resolved profile, and whether the request can rerank. Scroll drops `keywordRerank`.

For a kept `keywordRerank` rung, the rerank runs when semantic read is on, the request includes documents, the sort is by score, and the page starts inside the ranking window. The service reranks that window and then slices, so every page that starts inside the window is a slice of that one order, and a page that starts past the window keeps keyword order. A kept rung with a legacy `knn` section reranks the search's first result batch instead, read from offset 0. Scroll does not rerank. A later rung's `knn` is not applied when an earlier rung was kept.

Clearing `knn` leaves the order from the ranking step. An embedding or kNN failure on a process that advertises `documentRerank` does the same and increments the existing failure metric. The search does not fail. A template that requires `documentRerank` on a process that does not advertise it fails the capability check instead. While the stale-vector acknowledgement is off, the process does not advertise the capability. [Analyzers and hybrid documents](#analyzers-and-hybrid-documents) covers the vector fields. The [query sequence](#query-sequence) covers when the rerank step runs.

### Embedding profiles and gates

A semantic shape needs a capability. `combine: keywordRerank` needs `documentRerank`, `combine: vector` needs `vectorRetrieval`, and `querySource: anchorEntity` also needs `similarity`. `scope` needs `entityPartition`. Several embedding profiles (including `retrieval.profileScore`), `vectorBody`, the native mixed query, `vectorThenKeyword`, and `vectorFilter` each have their own name. A build that does not advertise a capability the template needs fails the template when it is selected. This release advertises `documentRerank`, `vectorRetrieval` for `combine: vector` only, `entityPartition`, and `similarity`, so every other vector shape still fails. Each name is advertised by the release that runs it. keywordRerank is the exception: without `documentRerank` the rung keeps keyword order, and its profile selection, which only the rerank uses, does not fail the search.

An embedding profile id is the `SemanticContent` embeddings map key, so a profile has no second name and needs no reindex. This release registers one profile: the Document default, keyed by the embeddings key semantic search derives from the configured model, class `textChunks`, available on Elasticsearch and OpenSearch. Its metric is the model's `spaceType`.

`profiles` resolves per entity type. Unset is the entity default. `replace` is the list. Otherwise `add` and `remove` patch the entity default. A named profile that does not cover the entity is replaced by the entity default, or dropped when the entity has no default, so the ids that cover it still resolve. A profile that is not available on this deployment's engine, and an id the registry does not know, fail coverage for that entity and do not fall back. On a template with `combine`, `knn.model`, `knn.field`, `retrieval.model`, and `retrieval.field` name the profile id and must agree, and `profiles` wins over them. A `knn` with no `combine` keeps `field` as before.

An entity joins the vector leg when it is in `semanticSearch.enabledEntities`, this build runs it for its profile's class, and its profile covers it. For a vector leg this build runs for the request, the entities that can join must select one profile, counting a selected profile that is unavailable here, `retrieval.minSimilarity` needs a cosine profile, and with `scope` set, a named profile must cover every scoped type. Otherwise the template fails, and every rung a ladder tries is checked the same way. An entity that cannot join is left out of the vector leg. Under keywordRerank its rows keep their keyword places, and when the request cannot rerank, the selection is not checked at all.

### Vector retrieval

Corpus kNN is a different rung, not a `knn` section. A rung has one retrieval mode. `keyword` is the default and uses `query`. `combine: vector`, or the legacy `mode: vector`, retrieves by embedding and ignores `query`, `clauses`, `functionScore`, `boosts`, engine `rescore`, `sort`, a caller `sortCriteria`, and an inherited `component`. The from/size order in [Ranking](#ranking) does not apply to this rung.

```yaml
retrieval:
  combine: vector
  candidateCap: 500
  minSimilarity: 0.8
  onFailure: next
```

The service embeds the query for the rung's embedding profile, which must be the profile the query embedding service embeds, and sends one kNN request to the Search V3 index of each entity type that joins the vector leg. That request carries the filter the keyword leg applies: the caller's filter, soft delete and hidden lifecycle stages with their opt-out flags, latest-version filtering, and the searched entity types. Neighbors are retrieved inside that filter, so a filter changes which neighbors come back. The service does not trim a global top-k. Filters the GraphQL resolvers add, such as the published-document default, reach both legs inside the caller's filter. View authorization is applied after the top-k, as on the keyword leg, so a page may come back short. Putting it inside the kNN filter would change which neighbors are retrieved.

The rung requests one window from offset 0. With `candidateCap`, the window is that cap. Without it, the window is the one semantic search fetches for the page: `max(size, min(500, ceil((from + size) * 1.2)))`. Either is clamped to the store's maximum of 1,000 neighbors. A count-only request counts every candidate a page could reach: the cap, else 500. The search cache runs a template that can reach a vector rung once for the caller's page, not in batches, and caches that one result under the page, so the rung and the ladder's predicates see the caller's page. Such a template fails at bind with `vectorWithLegacyKnn` when it can also reach a field-only `knn`, the legacy document rerank, because that rerank reorders the cache's first batch; `combine: keywordRerank` reranks inside the search and is allowed. Offset pagination of an approximate nearest-neighbor query is not used.

`retrieval.minSimilarity` is a raw cosine, from -1 to 1. Unset inherits `semanticSearch.minSimilarity` (`ELASTICSEARCH_SEMANTIC_SEARCH_MIN_SIMILARITY`), which defaults to unset, meaning no floor. The engines semantic read allows score cosine as `(1 + cosine) / 2`, so each score is converted with `2 * score - 1` before the comparison, and a hit at the floor is kept. A template floor on a profile whose metric is not cosine fails the template at bind. A floor inherited from configuration applies only to a cosine profile: on another metric it is skipped, and explain reports `minSimilaritySkipped: profileNotCosine`. The deprecated `SearchFlags.minScore` still drops a hit below the engine score, and a hit must pass both. Neither floor refills the window. `SearchFlags.minSimilarity` is the caller's raw cosine floor, from -1 to 1. With no floor in effect it sets one. With `retrieval.minSimilarity` or `semanticSearch.minSimilarity` in effect it can raise the floor and never lower it: the higher of the two applies. A profile whose metric is not cosine, or a value outside -1 to 1, fails the search as a bad request, and explain counts the hits only the caller's raise dropped (`belowCallerMinSimilarity`). Semantic search on its legacy path has no raw cosine floor, so it rejects the flag instead of ignoring it.

The page is sliced from the hits that survive the floors. The total is their count, not a corpus count, and a page past them is empty. Facets are a keyword aggregation, with no hits, over the same filter and restricted to the surviving URNs. This is the one case where a rung's aggregations are not the aggregations of its own retrieval. `facets: candidates` on a keyword parallel group is illegal. With hybrid read on, the default hybrid reranker does not reorder a vector page.

Of the requested types the rung's scope assigns it, one that fails the entity gates is left out of the vector leg, gets no keyword result in its place, and is reported in explain. A type outside the scope is not assigned to the rung and is not reported as dropped. When every assigned type fails, the page is empty and nothing is embedded or searched. A failed embedding, kNN, or facet request follows `retrieval.onFailure`: `fail`, the default, fails the search, and `next` follows the ladder's `next`, which counts `search.template.fallback`. The page the next rung serves is not cached, so the next search tries the vector rung again. On the `semanticSearch` surface a failure always fails the search, whatever `onFailure` says, so only the caller's flag below decides a fallback. A query the service could not embed fails with a reason: `timeout`, when the embedding did not finish by the search's deadline, or `rejected` (throttled), `reserveExhausted`, `budgetExhausted`, or `failed`. This release raises `timeout`, `failed`, and `rejected`, for a provider that throttled the query embedding: an HTTP 429 from Vertex AI, or a throttling error from the Bedrock client. Neither retries a throttled query embedding, since a retry's backoff would outlast the deadline and end the throttle as `timeout`. Bedrock retries a transient server or connection error once, quickly. Other providers report a throttle as `failed`. The other two are for an embedding service that budgets query embeddings, which raises them itself. GraphQL returns `timeout` as `DEADLINE_EXCEEDED`, `rejected`, `reserveExhausted`, and `budgetExhausted` as `SERVICE_UNAVAILABLE`, and `failed` as `SERVER_ERROR`, which may not clear on a retry, each with the reason in the message, from semantic search and from a keyword search, `searchDocuments` included, that names a vector template in `SearchFlags.searchTemplate`. With `SearchFlags.keywordFallbackOnEmbeddingFailure`, semantic search, and `searchAcrossEntities`, `search`, and `searchDocuments` through the template they name, serve the keyword search results for the same request instead, without the named template, and `SearchResultMetadata.keywordFallback` names the reason. The `searchDocuments` result does not return that reason. Only a failed query embedding falls back: a failed kNN or facet request, and similarity, which embeds nothing, still fail. GraphQL returns a similarity search that fails as `noAnchor` as `BAD_REQUEST`. The failed semantic search caches nothing. A process without semantic read or a real embedding provider, and a profile the query embedding service does not embed, fail a vector rung closed whatever `onFailure` says, also inside a parallel group. So does the DataHub index on OpenSearch before 3.5, for a rung it serves. That rung fails before it embeds the query, so `SearchFlags.keywordFallbackOnEmbeddingFailure` does not cover it either. Before 3.5 the k-NN plugin applies a nested kNN pre-filter in the chunk scope when it names an underscore-prefixed field such as `_entityType`, which the rung's filter always does, and there the filter can leave out documents it should match, so the rung could serve an empty or incomplete page, depending on the filter. A search of this kind that returned results before 3.5 fails now too. That covers a search that names a vector template, a URN that `semantic` routes to `similarity`, and a ladder's vector rung. GMS reads the engine the first time such a rung runs, logs a warning once, and keeps a version it has read until it restarts, so after an upgrade to OpenSearch 3.5 or later these rungs serve once GMS restarts. While GMS cannot read the engine type or the OpenSearch version, or reads a version of 7 or above, which a cluster in compatibility mode reports whatever it runs, the rung runs, since no other path serves it, and GMS reads them again at most once a minute. A rung an external store serves is not affected, and neither is `keywordRerank`, whose kNN request scores only the window's rows by URN. Similarity embeds nothing, so it needs semantic read and no embedding provider. The other decisions are expected states. They are counted on `search.template.vectorRetrieval` by `outcome` (`served`, `empty`, `pastWindow`, `noEligibleEntities`, `capClamped`, `floorDropped`, `failed`, `unavailable`, or `noAnchor`), not as a fallback. With `includeExplain`, `SearchResultMetadata.vectorRetrieval` reports the outcome, the profile, the searched entity types and the excluded ones with their gate, the window and any clamp, the hits the store returned, the floor and the hits each floor dropped, that the filter ran inside the kNN request, and the `SearchFlags.minScore` deprecation, with `SearchFlags.minSimilarity` as the caller's replacement and `retrieval.minSimilarity` as a template's.

The cache identity of a vector rung adds its execution version, the resolved profile, the system serving it, and the floor it inherits from configuration. A page from an external store also carries the window of that store's `resultTtlMillis`, so the cache stops serving it once the TTL passes. A similarity rung also adds the anchor URN and when each of its stored embeddings was generated. Bind checks the anchor before it reads it, and bind runs before the result cache. When the selected template is a similarity rung, an anchor the viewer may not see, or one the default search filters or the document visibility rule leaves out, fails the search there as `noAnchor` without being read, so no cached page is served for it. A similarity rung that only a ladder reaches is stamped unusable without being read, because the search may stop before it, and it fails if it runs. When its check or read fails or runs out of time, bind leaves it undecided: the page is not cached, and the rung checks the anchor if it runs. The rung reuses the answer bind recorded, usable or not. The check and both reads of the anchor, bind's and the rung's, run within the vector retrieval budget, which bind starts for a selected similarity rung and the rung's store calls continue. They run on a small pool of their own, and when it is full the search fails as it would on a failed check. Scroll fails on a vector rung, because a cursor has no keyword query to continue. It does not fall back to one. The document explain API fails the same way when its walk reaches a vector rung. `bridge` stays stored on the rung and is not run: a rung that sets it fails when it is selected.

An embedding profile's systems list names where its vectors live. A profile that lists the engine serving the search runs on the DataHub index, as above. One that lists only a store named under `semanticSearch.stores` runs on that store's adapter. When it lists more than one, the store whose name sorts first serves it, whatever order the configuration declares them in. A store named `elasticsearch` or `opensearch` is ignored, since those names always mean the DataHub index. DataHub ships no adapter, so that rung fails closed. The store's entry sets its `capabilities`, `timeoutMillis`, `maxK`, and `resultTtlMillis`. The service embeds the query and sends the vector. The store returns each hit's URN and raw cosine, and the service scores the hit `(1 + cosine) / 2`, as a cosine kNN on the DataHub index does. An entity the store returns more than once, such as once per chunk, becomes one hit at its best cosine, and the hits are ordered by it, before authorization and hydration. A request with a caller filter fails closed on a store without `preFilter`, whatever `onFailure` says, instead of trimming a global top-k. The system filters are not a caller filter. The GraphQL resolvers that add the published-document default (search, scroll, aggregate, metadata analytics, and semantic search through the `semanticSearch` surface) record on the search context whether the caller's own filter, with its view, had a criterion before they added it, so that default alone is not a caller filter either. Without that record, as on Rest.li and OpenAPI, which cannot set it, every filter criterion is the caller's. Every hit the store returns is hydrated in one request on the DataHub index: the hits' URNs inside the keyword leg's filter, which also counts the facets. A hit the caller cannot view is removed before that request, so it reaches no facet, total, or page. With `includeRestricted`, it is masked instead, as on the keyword leg. A hit the request does not return, because the entity is deleted, hidden, or outside the filter, is dropped. Each call runs on the hybrid read executor, or a similarity rung's on its anchor pool, and the service stops waiting at the store's deadline and cancels the call. A store whose calls fail three times in a row is paused for 30 seconds. A paused, failed, or timed-out call follows `onFailure`. Similarity needs `findById`, and a similarity search on a store without it fails closed. The DataHub index finds an entity's vectors in its `semanticContent`. With `includeExplain`, `vectorRetrieval` adds the store, the adapter latency, and the hits hydration removed.

#### Similarity

`querySource: anchorEntity` finds the entities nearest the one the query names. The query is that entity's URN (`anchor.urnParameter: raw`), read as routing reads it: a leading structured prefix is dropped, the rest is trimmed, and the `\/` the GraphQL search resolvers write for each `/` is read as `/`. Nothing is embedded: the store serving the profile returns the anchor's chunk vectors by URN (`findById`; the DataHub index reads them from the anchor's `semanticContent`), and the service sends that store one search per chunk, each with that chunk as the query vector. A neighbor scores its best chunk-to-chunk cosine, and the window is the `candidateCap` best neighbors. Equal scores are ordered by URN, so every page slices the same order. `anchor.excludeSelf` keeps the anchor out of its own neighbors: each search asks for one neighbor more than the window, and the anchor is dropped after the merge, so it does not take a place in the window and no store has to filter it out. A window already at the store's maximum `k` cannot ask for one more, so it can come back one short.

```yaml
retrieval:
  combine: vector
  querySource: anchorEntity
  candidateCap: 500
  minSimilarity: 0.8
  anchor:
    urnParameter: raw
    excludeSelf: true
```

The anchor's entity type selects the embedding profile, through the same entity gates a requested type passes. The rung's `scope` assigns only the requested types it searches, so it does not apply to the anchor: an anchor whose type the scope leaves out still selects its profile. A requested type the gates leave out, or one under another profile than the anchor's (`notAnchorProfile`), is left out and listed in explain, and the search runs for the rest. A query that is not a URN, an anchor the viewer may not see, an anchor the default search filters or the document visibility rule leaves out, an anchor whose type has no profile here, an anchor with no stored vectors for that profile, and one with more than 100 stored chunks for it, the most the default embedding pipeline stores for a document, fail the search, counted as `noAnchor`. Each chunk sends its own search, so the cap bounds one search's requests. Four run at a time on the anchor pool, a quarter of its sixteen workers, so one long anchor cannot hold half of it. They run under the vector retrieval budget, each with the time left as its timeout, and the merged neighbors are cut back to the window after each chunk. On an external store, each call, the anchor read included, also ends by the store's `timeoutMillis` and counts toward its breaker, and the neighbors are hydrated as for any vector rung. The default search filters are the ones every neighbor passes: an anchor that is soft-deleted, or in a lifecycle stage hidden from search, fails unless the request sets `includeSoftDeleted` or `includeHiddenLifecycleStages`. `searchAcrossEntities`, `aggregateAcrossEntities`, `scrollAcrossEntities`, `browseV2`, and `semanticSearchAcrossEntities` and `semanticSearch` through the `semanticSearch` surface also record the document defaults they add on the search context, and the anchor must pass them, so a viewer cannot anchor on a document that search would not show them. A document in a lifecycle stage other than published, a legacy document with no stage whose state is unpublished, and one not shown in global context fail. A viewer who can manage documents may anchor on that legacy document, and `includeHiddenLifecycleStages` lifts the defaults, as it does for the results. They apply whatever types the request searches, since a request without documents can still name one as its anchor. `searchDocuments` records its own visibility rule instead, without the caller's criteria, `states`, or view: a published document, an unpublished one the viewer or one of their groups owns, or any unpublished one for a viewer who can manage documents, and never a draft or a document kept out of global context. A recorded rule that names the lifecycle stage takes the place of the hidden-stage clause for the anchor, as it does for the results, so an owner's document in a hidden stage other than draft can anchor `searchDocuments`. Metadata analytics, which cannot name a template, records nothing, and neither does single-entity `search`, which adds no document defaults. Rest.li and OpenAPI add no document defaults, so there the default search filters alone apply. The caller's filter selects neighbors and does not apply to the anchor. These checks run in the rung before the requested types are joined, so once the rung runs they fail the search even when no requested type is eligible. A search that ends before routing runs none of them: semantic search returns an empty page when it has no entity type to search, because the request names none and no index holds documents, or its view leaves none. There is no keyword fallback. A failed store call fails the search whatever `onFailure` says, and similarity cannot set `next` or `knn`. The floors, the page, the total, and the facets are those of any vector rung.

Three features, three shapes:

| Feature          | Template                                                                               | Hits                                               | Total and facets                    |
| ---------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------- | ----------------------------------- |
| Document rerank  | Keyword rung with `knn` set                                                            | The keyword hits, reordered                        | The keyword query's                 |
| Hybrid retrieval | Parallel keyword rung and vector rung, `join: merge`, combined by the group's `fusion` | The fusion policy's combination. This can add hits | Named by the fusion policy          |
| Pure semantic    | A vector rung and no keyword sibling                                                   | The kNN candidates that pass the floors            | Their total, facets over those URNs |

`join: any` serves one member and cancels the other. It does not fuse, and it does not add the other member's hits.

In OSS, document rerank, hybrid retrieval, and pure semantic search run for the Document entity only. No other entity gets semantic search. The shipped `fulltext` template sets `retrieval.combine: keywordRerank` and `knn.others: keepKeywordPlaces`, with no floor. `light` leaves both unset and inherits them, so the keyword surface reranks the same way whichever of the two rungs serves. Scroll drops the rerank, and a request that cannot rerank keeps keyword order. The shipped `semantic` template is `combine: vector` with the page window, no `next`, and no floor. The shipped `similarity` template is the shape above with a cap of 500 and a floor of 0.8. A search runs it when it names it in `SearchFlags.searchTemplate`, or through `semantic`, whose one route sends a query that is an entity URN (lowercase `urn:li:`, an entity type, a colon, and a non-empty key) to `similarity`. Any other query stays on `semantic` and is embedded. Neither template names a model, a vector field, a store, or an entity type, and `profiles` is unset, so each entity type resolves to its registry default. A search runs `semantic` when it names it in `SearchFlags.searchTemplate`.

Semantic search (`semanticSearchAcrossEntities`) runs `semantic` through the `semanticSearch` surface, whose slot is `surfaces.semanticSearch.fulltext.default`, when that surface's rollout serves the actor: `SEARCH_V3_TEMPLATES_ROLLOUT_SEMANTIC_SEARCH=active`, with `SEARCH_V3_TEMPLATES_ROLLOUT_SEMANTIC_SEARCH_PERCENT` for a canary by the same actor and surface bucket as the other surfaces. The setting ships blank, and a blank or missing setting keeps the surface off whatever the global rollout mode and percent say, so a configuration that lacks the key never turns it on. A value that is not a rollout mode, such as `on`, or a percent outside 0 to 100 with the mode active, keeps it off too, with one warning per value, instead of failing every semantic search. Off, and `shadow`, keep semantic search on its own kNN search, unchanged. On OpenSearch before 3.5, semantic search stays on its own kNN search whatever the rollout says, and GMS logs a warning once: before 3.5 the k-NN plugin applies a nested kNN pre-filter in the chunk scope when it names an underscore-prefixed field such as `_entityType`, which the template's filter always does. A vector rung that another search runs on the DataHub index fails closed there instead, as `unavailable` (see [Vector retrieval](#vector-retrieval)). While GMS cannot read the engine type or the OpenSearch version, or reads a version of 7 or above, which compatibility mode reports, semantic search stays there too, with one warning for each reason, and GMS reads them again at most once a minute, so granting the `cluster:monitor/main` permission needs no restart. GMS keeps a version it has read until it restarts, so after an upgrade to OpenSearch 3.5 or later the surface takes effect only once GMS restarts. The resolver decides once per request by the coordinator's rule. On the template path it adds the document defaults keyword search adds (the published-document default and placement, as `DefaultEntityFiltersUtil` builds them), records whether the caller's own filter had a criterion, and leaves those default fields out of the facets. On keyword and semantic search alike, a caller filter on `showInGlobalContext` lifts the placement clause in the OR branches of the filter that name that field, and only there. A caller filter on `lifecycleStage` or `state` is combined with those defaults; it does not lift the published-document default. `includeHiddenLifecycleStages` drops them, and a caller who can manage documents also gets unpublished documents that have no lifecycle stage. The default search filters below the resolvers do lift theirs: a caller `removed` filter lifts the soft-delete clause, and a caller `lifecycleStage` filter lifts the hidden-stage clause. Semantic search sets `fulltext`, since its query is only embedded, so the surface reads the fulltext seed. The vector rung's page, total, and facets stand: no keyword facet request runs beside it, so the facets count the neighbors that survived. They come back by default, from one more engine request after the kNN request, which `skipAggregates` skips. The legacy search returns no facets unless the request asks for structured property facets. Only `document` has an embedding profile in this release, so the entity gates leave out every other type in `ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`, which the legacy search served, and a request for only such types gets an empty page, with the outcome `noEligibleEntities` under `includeExplain`. Unlike the legacy search, the template path also leaves out soft-deleted entities and entities in a hidden lifecycle stage unless the request opts in, applies `semanticSearch.minSimilarity` where the legacy search reads only `SearchFlags.minScore`, and answers a query it could not embed with a typed error where the legacy search returns a generic server error. The page window is the one the legacy search fetches. The query embedding and the kNN request share the vector retrieval budget of two seconds, where the legacy search sets no timeout on either: an embedding that runs past it is `timeout`, and a kNN request that runs past it fails the search, which the caller's keyword fallback does not cover. With the search cache on, a template page is cached per actor, query, filter, flags, and page for the cache TTL, where the legacy search was never cached. Only a vector rung serves the surface. A slot or a caller `searchTemplate` that selects a keyword template, the structured seed, a ladder that serves a keyword rung, and a search that leaves the template path after bind, as one with the skip-template flag set inside it does, fail the search instead of returning keyword results. A query that is an entity URN takes `semantic`'s route to `similarity` on this surface as well. Single-entity `semanticSearch` follows the same rollout, decided the same way, and runs this path for its one entity type: the document defaults and their anchor filter, `SearchFlags.minSimilarity`, facets by default, the typed errors, and the keyword fallback. Its keyword fallback is the cross-entity keyword search over that type with the document defaults added, since single-entity `search` adds none. Off, it keeps its own kNN search and rejects `SearchFlags.minSimilarity`.

Hybrid retrieval adds vector-only hits once a fusion policy is chosen. Until then a hybrid result does not add vector-only rows.

### Render context

`parameters` exists so a body can name a value, such as a deprecation weight, and a child can change that value without rewriting the body. The builder also passes the queried field names, the keyword field, the URN field, a separate placeholder for each query variation, and `defaultQueryFieldsJson`.

`defaultQueryFieldsJson` is the analyzed paths in the queried set, each as `path^weight`, using the resolved field weight. Stemmed subfields are included at half that weight, except `name`, which has no stemmed subfield. A string or rich-text structured property in the set is `structuredProperties.<field>.text^weight`. A clause reads this list when the body names it. `fields.override` changes an entry's weight. `fields.remove` omits the field. A numeric boost written beside a path in the body is not this list, and override does not rewrite it.

#### Query variations

The builder rewrites the query before any clause sees it. The service computes the variations. The template names a placeholder. It does not supply a rewrite expression. A clause uses `raw` for the delimited-identity match and for the unsplit whole token, and `escaped` or `alphanumericSplit` for the analyzed match.

| Placeholder         | Value                                                                                                           |
| ------------------- | --------------------------------------------------------------------------------------------------------------- |
| `raw`               | The query after trim, before operator escaping and alphanumeric splitting.                                      |
| `escaped`           | `raw` after simple-query-string operator escaping.                                                              |
| `normalized`        | `escaped` after spelling-variant synonym normalization.                                                         |
| `alphanumericSplit` | `normalized` with a space inserted at each letter/digit boundary. Equal to `normalized` when nothing was split. |
| `fuzzy`             | The fuzzy form of `normalized`.                                                                                 |
| `unquoted`          | `raw` with one pair of surrounding quotes removed.                                                              |
| `shortened`         | `normalized` cut on a word boundary.                                                                            |
| `lastSegment`       | The last `.` / `_` / `-` / `/` segment of `raw`.                                                                |
| `synonyms`          | The synonym list for the whole query.                                                                           |

Shape flags are computed by the service and referenced from the body:

| Flag                      | True when                                                                                     |
| ------------------------- | --------------------------------------------------------------------------------------------- |
| `multiWord`               | Two or more words.                                                                            |
| `longPhrase`              | Four or more words.                                                                           |
| `dotted`                  | `raw` contains a `.`.                                                                         |
| `fullyQualifiedName`      | Two or more dots.                                                                             |
| `deepPath`                | Three or more dots. Available to a window sizer.                                              |
| `longExactName`           | Five or more delimiter tokens and no space.                                                   |
| `spacelessIdentifier`     | No space, and either six or more digits or four or more parts split on `.`, `_`, `-`, or `/`. |
| `operatorEscaped`         | `escaped` differs from `raw`.                                                                 |
| `flags.alphanumericSplit` | The `alphanumericSplit` text differs from `normalized`.                                       |
| `quoted`                  | `raw` has one surrounding pair of quotes.                                                     |

`multiWord`, `dotted`, and `spacelessIdentifier` are also ladder predicates. `spacelessIdentifier` replaces both skips in the cascade: the six-digit hash pattern and the four-part delimiter skip. The other flags are body and window-sizer inputs. `fullyQualifiedName` and `deepPath` are shape flags. `quoted` is a flag the exact template can read, because a quoted query selects exact. A single term with no spaces does not select exact. They are not routes. The placeholder `alphanumericSplit` is the rewritten text. The boolean is `flags.alphanumericSplit`, and it is true when that text differs from `normalized`.

A quoted query selects exact and requires the phrase: `unquoted` must appear as a case-insensitive substring, words in order, in a queried text field. A document that does not contain that phrase is not a hit. Whole-value identity selects `identity`, a URN or a storage path, and a keyword term.

The same map receives values the service injects for the request. `viewerUrn` is the caller. `viewerGroupsJson` is a JSON array of that caller's groups. `viewerDomainsJson` is a JSON array of the domains listed on the caller. `domainsJson` is a JSON array of domain URNs: domains the caller owns, domains named by an active policy that grants the caller a privilege on that domain, and the child domains under either. A platform-wide policy leaves this list empty. A clause references one of these to narrow. View authorization stays outside the template. When a rung's clause or boost names an injected value, that value is a service discriminator on the result cache key, so two callers do not share those hits. A template that names none of them keeps a cache key with no caller in it. A structured boost uses the same four names. See [List boosts](#list-boosts).

### Write-time syntax

Creating or updating a template resolves structured-property URNs to field names, renders each `queryFragments` value and then `query`, `clauses`, `functionScore`, `rescore`, `aggregations`, and `sort` with a validation context, then parses the result. A child that fills a hole the inherited query does not contain is rejected. A `query` write queries templates whose indexed `parent` is this URN and walks inheriting children, and it is rejected when the new text drops a hole one of them still fills. Where a remaining hole sits is not checked. See [Query holes](#query-holes). See [Structured properties](#structured-properties). For the Elasticsearch dialect, each of those sections has to be valid JSON, and `query`, `clauses`, and `functionScore` have to be valid query syntax. `functionScore` is parsed as a `function_score` wrapper, so a filter snippet on a function is parsed with that function. A render failure or a parse failure rejects the write. The check runs on the section as written and again on the body after parent merge. The Postgres dialect is rendered only. `structuredPropertyQuery` is checked for shape only: `analyzedValue`, `exactValue`, and `nameAndValue`, or an explicit empty section. A running `nameAndValue` clause needs a positive `weight`. That check does not expand the definition catalog. URN-to-field resolution still applies to `query`, `clauses`, `fields`, and the other sections that name a property.

`ranking`, `knn`, `retrieval`, `highlightFields`, `boosts`, `routes`, and the ladder fusion fields are structured sections. Write time checks them against the closed sets: ranking preset and window sizer, `knn.others`, retrieval mode, `combine`, `querySource`, `profileScore`, the anchor `urnParameter`, retrieval and bridge `onFailure`, retrieval `total` (`candidates`), `facets` (`candidateUrns`), `page`, and `authorization`, fusion, group facets, and the injectable named by a boost. `page` and `authorization` have no value yet, so any value is rejected, and no release reads any of those four yet. A template stored with another value before this check still loads. It also rejects a template that no build can run: `candidateCap` below 1, `minSimilarity` outside -1 to 1, on `keywordRerank`, on a keyword rung, or with `profileScore: weightedSum`; `querySource`, `profileScore`, or `vectorBody` on a keyword rung; `anchorEntity` without `combine: vector`; similarity with `knn` or `next`; `vectorBody` with `profileScore`; an empty `profiles.replace`; and the native embedding placeholders together with `combine` or `vectorBody`. A later combine, several embedding profiles, `vectorBody`, and the native mixed query stay valid to store. A `functionScore` snippet that references one of the injectables is rejected. `replace` on `highlightFields` cannot be combined with `add` or `remove`. `replace` on `fields` cannot be combined with `add`, `remove`, or `override`. An `override` of a field that is not in the queried set after `add` and `remove` is rejected. A `.keyword` or `.text` suffix is legal on string and rich text. Either suffix is rejected on a date, a number, or a URN. A query `_name` of `exact`, `prefix`, `contains`, or `nameAndValue` is not a closed write-time set. The result reports that mode only when the matching clause carries it and names one structured-property field.

Each `routes` entry is a RE2 pattern and a template URN. Write time compiles the pattern, rejects a pattern or a list over its length limit, rejects a missing target, rejects a target of another dialect, and rejects a target that is the template being written. A parent cycle is still rejected on the parent chain. A route cycle that depends on which pattern matches is left to bind, which fails the search when the walk repeats a URN.

An unknown `capabilities` name, and an `apiVersion` other than 1 or 2, are rejected when the template is written and when it is loaded. A ranking preset other than `none`, a window sizer other than `fixed`, `vectorThenKeyword`, `vectorFilter`, `vectorBody`, `profiles` on a keyword rung, the native embedding placeholders, a bridge, and a merge join fail when that template is selected. A vector rung fails on scroll. A `scope` that leaves out a requested entity type fails a scroll, filter, aggregation, or count.

### Stamps

`created` and `lastModified` are set by the server. Callers omit them. `created` is set on the first write and kept after that. Every accepted write sets `lastModified`. The [query sequence](#query-sequence) puts that stamp, for every rung the walk consults, into the result cache key.

## Documents

The index is one mapping per entity. Shared field names are unified: the same name means the same thing on every entity. Aspect fields stay under that aspect. Structured properties, analyzers, and hydration are specified after that mapping.

### One mapping, per-entity aspects

#### Shared fields

`@Searchable` on the traditional PDL model maps a source field onto a shared field. `v3.searchLabel` is the shared name and `v3.degree` is the distance. Unset degree is 0, the value on the entity. The annotation author sets a higher degree for a more distant or more weakly associated value. `name` collects title and name. `description` collects every description: info aspects, properties, ingested, edited, and generated. Platform is its own field, separate from the name. Analyzed text is copied onto the shared fields. Root fields stay keywords for filters, facets, and sort. `_entityType` names the entity on every document.

Every shared field follows the same rule. Each degree above 0 is indexed as a flat sibling of the degree-0 field, named `<sharedField>_<degree>`: `tags` and `tags_1`, `glossaryTerms` and `glossaryTerms_1`, `owners` and `owners_1`. Tags, glossary terms, and owners are examples, not a closed list. For a dataset, degree 1 holds the values copied from its columns. The degree belongs to that aspect location, so every entity that includes the location uses the same degree. The suffix means the same distance on every entity. `v3.queryByDefault` and `v3.boostScore` apply to that indexed field. The top-level `queryByDefault` and `boostScore` stay the Search V2 values. Column name, description, tags, and glossary terms stay out of an unqualified query. Schema validation rejects two annotations on one entity that set the same field and degree with different defaults, a `v3.boostScore` that increases with degree, a different degree for the same aspect location, and a different `v3.queryByDefault` or `v3.boostScore` for the same indexed name. See [Degrees of separation](#degrees-of-separation). A filter, a facet, or a template names one of the fields.

#### Fields under each aspect

Each aspect keeps its own fields under `_aspects.<aspect>`, unanalyzed. A root name does not collide with an aspect path, and an aspect-scoped filter still addresses the original field. That separation is what adds fields. A dataset and a dashboard share `name` and `description` and otherwise contribute the fields of their own aspects. Embeddings and other non-aspect data use reserved root names.

#### Logical children and usage

`hasLogicalParent` is indexed on the child. `hasLogicalChildren` is a boolean on the parent, maintained when `logicalParent` is written or removed. Logical role is a term filter on those two flags.

`queryCountLast30Days` and `viewCountLast30Days` are numeric document fields. A template `sort` and a caller `sortCriteria` may name them. Most Queried is `queryCountLast30Days`. Most Visited is `viewCountLast30Days`. The `usage` ranking preset does not replace either sort.

#### Index and document id

Documents live in `{entity}index_v3`. Shared field names mean the same thing on every index. An aspect field may exist on only one index in a multi-index query. A clause on it matches nothing on the others, and the query is still valid. The document `_id` is a hash of the entity URN. The URN stays a document field.

### Structured properties

A template names a structured property by its definition URN, such as `urn:li:structuredProperty:certification.status`. The same qualified name is the same field on every entity type that definition allows.

`searchConfiguration.queryByDefault` decides whether the default query includes it. Unset leaves it out. `boostScore` on the definition is the weight when a query includes it. `add`, `remove`, `override`, and `replace` each put that URN on `fieldName`:

- `fields.add` queries a property the definition left out, at the weight on the entry.
- `fields.remove` drops one the definition included.
- `fields.override` changes the weight of one already in the set.

The examples are in [Examples](#examples). A string, rich-text, or URN value matches as one whole term at its field weight, whether or not the body also names the field. A multi-word query matches when the whole value is that term. One identifier token keeps a hyphenated value as one term. A date or a number stays on its typed field. In a query body, a clause, or a highlight, write the URN as `{{urn:li:structuredProperty:certification.status}}`. The template names the form after that reference. See [Structured property references](#structured-property-references).

A string or rich-text structured property keeps its keyword form and also has one analyzed text form, using the shared `word_delimited` and `query_word_delimited` analyzers on the `.text` multi-field. URN, date, and number stay keyword, date, or number. Nothing is copied into `_search.structuredProperties` or `customProperties`. Adding the analyzed form is a Search V3 mapping change and a rebuild. Search V2 does not index the analyzed form. Search V2 and Search V2.5 keep their current structured-property full-text behavior until those paths are deprecated.

#### Numeric property scores

`fieldValueScores` names the property URN. That entry is what turns scoring on. A `score` on the property definition does not. The shipped templates do not set `fieldValueScores`. A deployment adds it by writing the section on its own template.

Omitted `inputMin`, `inputMax`, and `maxMultiplier` come from the definition's `score`. A value on the template replaces the definition. If the definition and the template together still lack a finite range with `inputMax` greater than `inputMin` and a multiplier of at least 1, that entry is skipped.

A raw index field has no definition, so the template sets all three extents.

Values at or below `inputMin` stay at multiplier 1. Values at or above `inputMax` get `maxMultiplier`. The span maps as `1 + slope * (value - inputMin)`, where `slope` is `(maxMultiplier - 1) / (inputMax - inputMin)`. A missing field stays at 1. A multi-valued field uses its smallest value.

```yaml
fieldValueScores:
  - propertyUrn: urn:li:structuredProperty:io.example.priority
```

```yaml
score:
  inputMin: 0
  inputMax: 100
  maxMultiplier: 2
```

#### Name and value

A template may also request name-and-value matching. Search V3 still retrieves an asset by the assigned value on that definition's field. When the query contains the property's display name or qualified name and the unmatched remainder equals a value assigned to the same definition, an additional clause matches the asset. A name alone does not retrieve an asset that merely possesses the property. The name and the value resolve to the same definition. One property's value does not satisfy another property's name.

Assigned values stay on the existing per-definition field under `structuredProperties`. That field path is the correlation between a definition and its value. A shared copied field cannot express it. `displayName`, `qualifiedName`, and `description` stay on `StructuredPropertyDefinition`. They are not copied onto the asset document. There is no nested key/value object and no sibling name field. A display-name or qualified-name change does not rebuild asset documents. A definition version or value-type change remains a mapping change and a rebuild. Multiple values for one definition stay on that definition's one field. Any of those values may satisfy the remainder. Versioned definitions remain distinct fields, and resolution uses the current definition. An omitted collision field still cannot be targeted.

Name-and-value matching applies to string and rich-text properties. Those values keep the whole-term keyword contract above. URN, date, and number properties stay on their existing value or typed matching.

```yaml
structuredPropertyQuery:
  analyzedValue:
    enabled: true
  exactValue:
    enabled: true
  nameAndValue:
    weight: 8
```

`structuredPropertyQuery` has three clauses: `analyzedValue`, `exactValue`, and `nameAndValue`. A child replaces only the clauses it sets. Shipped fulltext enables analyzed and exact value matching. `nameAndValue.weight` enables name-and-value matching at that positive weight, which is what makes `certification status gold` match the value `gold`. Leaving `structuredPropertyQuery` unset inherits the parent section. An explicit empty section drops it, including an inherited section. The name-and-value weight is not a `fields.override` and is not a `functionScore` weight.

The executor expands the section at search time from the current definitions. The template does not store one clause per property, a snapshot of names, or resolved field paths. Expansion includes only string and rich-text properties that remain in the resolved queried set. `fields.add`, `fields.remove`, `fields.override`, and `fields.replace` control participation the same way they control value querying. Removing a property drops it from the weighted field list, drops its whole-value term, and drops its name-and-value clause. After render, a clause that targets only the removed field is removed. A multi-field clause loses that field and is removed when no field remains. `fields.override` changes that property's whole-value term boost and its `path^weight` in the weighted field list. It does not rewrite a numeric boost written in the body, and it does not change `nameAndValue.weight`.

Comparison is case-insensitive against `displayName` and `qualifiedName`. A display name matches a whole-word span. A qualified name matches as one span, with its dots preserved. A trailing word such as `status` does not match `certification.status`. The longest matching span is the one used. A shorter overlapping span is not also applied. The unmatched remainder must be non-empty. An empty remainder produces no asset clause. The remainder matches that property's assigned value as one whole term, the same string and rich-text rule as above.

The matches are `should` clauses grouped in one `dis_max`. An asset receives the best matching property's score, not the sum across properties. The group is placed in the rendered query before `functionScore`. The expanded clause carries the query name `nameAndValue`. The group is not a `functionScore` weight, a `rescore`, or a ranking preset. See [Ranking](#ranking).

Value-only matching still queries the whole query against each included value field. Name-and-value matching is the additional per-definition remainder clause. That is what allows `certification status gold` to match the value `gold`. The clauses run on search and scroll because they are part of the composed keyword query. Dropping `rescore`, `ranking`, or `knn` does not drop them. See [Search and scroll](#search-and-scroll).

Write time checks the section shape: the three clauses, or an explicit empty section. A running `nameAndValue` clause needs a positive `weight`. It does not expand the definition catalog. Search time resolves the definitions again. A definition-name change must not keep serving an expansion built from the previous catalog. The catalog stamp used for that expansion is part of the result-cache key. See [Query sequence](#query-sequence).

Search V2 and Search V2.5 keep their current structured-property full-text behavior until those paths are deprecated. On V3 a string or rich-text value stays on the per-definition field the mapping already builds, with the analyzed `.text` form beside the keyword form. The definition does not set an analyzer. V3 does not copy the value into `_search.structuredProperties` or `customProperties`. An entity type the definition does not allow has no field. Changing the definition's version or value type is a mapping change and a rebuild.

| Case                                                  | Result                                                                                                                                                                          |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fields.add`, left out of the name clauses            | Whole-value term at that weight, and the analyzed path in the weighted field list. A name-only clause the author wrote stays name-only.                                         |
| Filled into `extraDisMaxQueries`                      | A branch of that `dis_max`. The boost on that branch stays the boost the author wrote.                                                                                          |
| Same weight as `name`, in the weighted field list     | Business Name and the entity name tie inside that list.                                                                                                                         |
| Higher weight than `name`, in the weighted field list | Business Name ranks above `name` inside that list. It does not outrank a numeric boost the author wrote on a name clause unless the author wrote the property into that clause. |
| `fields.override`                                     | Changes the whole-value boost and the weighted-list entry. A numeric boost in the body stays. `nameAndValue.weight` stays.                                                      |
| Removed from the queried set                          | The weighted-list entry, the whole-value term, a clause that targeted only that field, and the name-and-value clause are absent.                                                |

#### Structured property references

`StructuredPropertyUtils` resolves the definition URN from the current definition before rendering, so versioning and field-name resolution are not stored on the template. The same resolver accepts a logical name. The URN constant resolves to that path with no subfield. Name-and-value expansion uses that same resolution and the same per-definition field. The template does not store resolved paths for `structuredPropertyQuery`. See [Name and value](#name-and-value).

`toElasticsearchFieldName` returns the suffix only. The caller adds the `structuredProperties.` prefix for every definition. An unversioned definition is the qualified name with each `.` replaced by `_`, so `certification.status` becomes `structuredProperties.certification_status`. A versioned definition is `structuredProperties._versioned.<name>.<version>.<type>`. That versioned path is the resolver's output. A caller passes a definition URN, or the logical name `structuredProperties.<qualified name>`. Passing the versioned path in as a logical name is rejected, because lookup would read it as a qualified name.

The template names the form. A structured-property reference resolves to the indexed field. The keyword form is that field. A template may still write `.keyword` after a string or rich-text reference; rendering drops it, because Search V3 does not map that subfield. The analyzed form is the same field plus the `.text` multi-field. The builder substitutes those paths into the body. It does not emit a clause because a mode was set. `.keyword` and `.text` are rejected on a date, a number, or a URN. When two qualified names sanitize to one field and the mappings are identical, the collision resolver keeps the lexicographically smallest URN. When the mappings differ, it omits the field. A template that names an omitted field is rejected at write time.

Inline text injects the URN as a Mustache constant. The tag body is the whole URN. The form is text after the tag, from a closed set: `.keyword` or `.text`.

```
{{urn:li:structuredProperty:certification.status}}.keyword
{{urn:li:structuredProperty:certification.status}}.text
```

`query`, `clauses`, `functionScore`, `rescore`, `aggregations`, `sort`, and `highlightFields` use that constant. Rendering replaces the tag with the indexed field. A `.text` suffix stays. A `.keyword` suffix is dropped, because the property field is the keyword.

`fields` add, remove, override, and replace name the definition URN on `fieldName`. The form is named in the body, not on the entry. The resolved field is the merge key, so a `remove` and an `override` of the same definition name the same field. Removing it drops the weighted-list entry, the whole-value term, and a rendered clause that targeted only that field, for either form. A `boosts` entry may name `keyword` or `text` as `subfield` when that boost needs one form. A reference that does not resolve to a definition is rejected at write time. Rendering resolves the URN again from the current definition, then the body is parsed.

### Hydrating from `_source`

TODO. Search hits are to carry the projected aspects in `_source`. The projection names structured-property values, custom properties, and the usage numbers `queryCountLast30Days` and `viewCountLast30Days`. A request that does not name them does not read them.

Logical-children chips stay a page-level relationship query. The document holds `hasLogicalChildren` only.

A read that only needs those projected fields would build the aspect from the hit instead of reading it from the primary store. Aspects the document does not hold, and requests that need the stored record rather than the search projection, would still hydrate from the primary store. The choice would be per aspect, so a search response could mix document-sourced aspects with primary-store aspects. Search responses currently read aspects from the primary store.

### Analyzers and hybrid documents

Analyzed text stays on the shared text fields. A structured property stays on its per-definition field. Search V3 indexes one analyzed text form beside the keyword form for string and rich text, using the shared `word_delimited` and `query_word_delimited` analyzers on `.text`. The template names the keyword field or `.text`. A written `.keyword` suffix names that same keyword field. A property in the queried set gets a whole-value term at its field weight and, for string and rich text, an entry in the weighted field list. A property name is not copied onto the asset. Search V2 does not index the analyzed form. See [Structured properties](#structured-properties). Ordinary aspect fields stay keywords, numbers, dates, and booleans. Word-gram fields, ngram fields, and the per-field analyzer chain do not come forward.

A keyword field filtered either with or without case is stored in both forms: the original value, and the value lowercased and ASCII-folded. An annotation-derived URN is two fields. The logical name is the parsed name. `<field>_urn` is the full URN used by filters, facets, sorts, and ranges, and `<field>_urn.lowercase` is the case-insensitive full URN. Callers still use `tags`, `owners`, and `parent`. Full text reads the logical field only when that field is included in full text, so `owners` has neither `text` nor `stemmed`. The document `urn` is the exact entity URN, and `urn_name` is its parsed identity and is not full text. A string or rich-text structured property follows the dual-form rule. A structured-property URN stays on `structuredProperties.<field>` and `__urn_name`. A date or a number has no case form.

`v3.queryByDefault` selects each indexed field, at any degree, and `v3.boostScore` is its default weight until a template `fields.add`, `fields.override`, or `fields.replace` sets one. When `v3.boostScore` is unset, Search V3 uses the top-level `boostScore`. When `v3.queryByDefault` is unset, the field stays out of an unqualified Search V3 query; set it true to opt in. The top-level `boostScore` stays the Search V2 weight. Column degrees stay out of an unqualified query. Schema validation requires one `v3.queryByDefault` and one `v3.boostScore` for each indexed name. A structured property uses `queryByDefault` and `boostScore` on its `searchConfiguration` the same way, and that pair is one value for every entity type the definition allows.

The Document entity carries kNN vector fields with classical document embeddings. Document rerank, pure semantic search, and hybrid retrieval query those fields. In OSS they apply to the Document entity only.

Document rerank is a keyword rung with `knn` set. The weights reorder document rows already in the keyword window, and clearing `knn` leaves the ranking-step order.

Pure semantic search is a rung with `retrieval.mode: vector` and no keyword sibling.

Hybrid retrieval is a parallel keyword rung and vector rung whose fusion can add hits. That shape stays unwired until a fusion policy is chosen. See [Document rerank](#document-rerank) and [Vector retrieval](#vector-retrieval).

## Related documents

- [Search](../how/search.md) covers the operator-facing description of query templates, custom search configuration, and advanced queries.
- [Semantic search configuration](../how-to/semantic-search-configuration.md) covers document embeddings.
