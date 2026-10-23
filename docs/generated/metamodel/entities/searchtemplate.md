# SearchTemplate
A Mustache-rendered search query template, addressed by URN and selected at query time. Managed through the generic entity API. Not included in default search, browse, or autocomplete entity-type lists.


## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### searchTemplateProperties

Python class: `SearchTemplatePropertiesClass`

A Mustache-rendered search query template. Sections that are unset are inherited from
{@code parent}. An explicitly present empty string for {@code query}, {@code clauses},
{@code functionScore}, {@code rescore}, {@code knn}, or {@code ladder} drops that inherited
section. An explicitly present empty {@code ranking} or {@code retrieval} drops that inherited
section. An explicitly present empty {@code fields}, {@code highlightFields},
{@code aggregations}, or {@code sort} drops the inherited override and falls back to the schema
or Java default. An explicitly present empty {@code boosts} drops an inherited list.
An explicitly present empty {@code structuredPropertyQuery} drops an inherited section.
A child replaces only the structured-property clauses it sets. An explicitly present
empty {@code normalization} drops inherited query-normalization settings. A child
replaces only the normalization fields it sets. Each non-empty {@code queryFragments}
value adds to the root-first comma join for that hole. An empty string drops inherited
pieces for that hole.
{@code routes} is replaced as a whole list: unset keeps the parent list, an empty list
drops it, and a non-empty list replaces it. {@code defaultRoute} is inherited on its own.
Unset keeps the parent default. A record with a template replaces it. A record with no
template clears it.

{@code apiVersion} is the template and Mustache-context contract. Unset keeps the parent
version. When no ancestor sets it, the resolved version is 1. A value other than 1 is rejected
when the template is written and when it is loaded. An accepted write stores 1 when the caller
omits it. {@code capabilities} names features this body requires. Unset keeps the parent list.
An empty list drops it. A non-empty list replaces it. A name this release does not implement is
rejected when the template is written and when it is loaded.

{@code dialect}, {@code apiVersion}, {@code parent}, and {@code editable} are searchable with
{@code queryByDefault} false. A write uses {@code parent} to find its children; the other
fields support template administration filters. {@code aspectJson} stores this aspect's JSON,
the same JSON stored in SQL, at
{@code _aspects.searchTemplateProperties._aspectJson} without indexing it, so a template
reload can read the body from search. Write time and actor stay on system metadata
({@code aspectCreated}, {@code aspectModified}): SQL stores them on the aspect row, and search
copies them to {@code _system}. The entity stays out of the default search, browse, and
autocomplete entity-type lists.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name. |  |
| description | string |  | What this template ranks or matches. Not searchable. |  |
| dialect | SearchTemplateDialect | ✓ | Body language. Elasticsearch and OpenSearch share {@code ELASTICSEARCH}. {@code POSTGRES} stores ... | Searchable |
| apiVersion | int |  | Template and Mustache-context contract. This release implements 1. Unset inherits the parent vers... | Searchable |
| capabilities | string[] |  | Features this body requires. Unset inherits the parent list. An empty list drops it. A non-empty ... |  |
| query | string |  | Mustache text for the query. For {@code ELASTICSEARCH} this renders to one query DSL object. For ... |  |
| queryFragments | map |  | JSON spliced into a named hole in {@code query}. Each level that sets a non-empty key adds a piec... |  |
| clauses | string |  | Mustache JSON object with {@code must}, {@code must_not}, {@code filter}, and {@code should} arra... |  |
| functionScore | string |  | Mustache JSON for one function_score wrapper: {@code functions}, {@code score_mode}, {@code boost... |  |
| rescore | string |  | Mustache JSON for one rescore clause ({@code window_size}, {@code query}). Unset means no rescore. |  |
| knn | string |  | Mustache JSON for document rerank weights: {@code keyword_weight} and {@code vector_weight}. {@co... |  |
| ranking | SearchTemplateRanking |  | Service ranking. Unset keeps the parent section. An explicit empty value drops it. OSS runs {@cod... |  |
| retrieval | SearchTemplateRetrieval |  | Retrieval mode and vector settings. Unset is keyword. An explicit empty value drops an inherited ... |  |
| ladder | SearchTemplateLadder |  | Selection, stop, and the next template. Unset means this template is one query. An explicit empty... |  |
| aggregations | string |  | Mustache JSON object of aggregation name to aggregation body. Unset keeps the annotation default ... |  |
| sort | string |  | Mustache JSON array of sort clauses. Unset, or an explicit empty value, keeps the Java default of... |  |
| fields | SearchFieldOverrides |  | Template default for queried fields. |  |
| structuredPropertyQuery | StructuredPropertyQuery |  | Analyzed, exact, and name-and-value clauses for structured properties. Unset inherits the parent ... |  |
| normalization | SearchTemplateNormalization |  | Wildcard, synonym, fuzzy, and broad-query limits. Unset inherits field by field. An explicit empt... |  |
| highlightFields | HighlightFieldOverrides |  | Template default for highlight fields. |  |
| parameters | map |  | User Mustache variables, for example {@code deprecatedBoost}. |  |
| boosts | SearchTemplateBoost[] |  | List-membership weights. Each entry names a field, an injectable list, and a weight. An empty arr... |  |
| routes | SearchTemplateRoute[] |  | Ordered RE2 routes. The first pattern that matches the whole trimmed query selects the template. ... |  |
| defaultRoute | SearchTemplateDefaultRoute |  | Template used when no route pattern matches. Unset keeps the parent default. A record with a temp... |  |
| parent | string |  | Parent template. The child replaces only the sections it sets. The parent must use the same diale... | Searchable, → IsPartOf |
| editable | boolean | ✓ | When false, only the system actor may change this template. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "aspectJson": true,
    "name": "searchTemplateProperties"
  },
  "name": "SearchTemplateProperties",
  "namespace": "com.linkedin.search.template",
  "fields": [
    {
      "type": "string",
      "name": "name",
      "doc": "Display name."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "What this template ranks or matches. Not searchable."
    },
    {
      "Searchable": {
        "fieldName": "dialect",
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "type": {
        "type": "enum",
        "name": "SearchTemplateDialect",
        "namespace": "com.linkedin.search.template",
        "symbols": [
          "ELASTICSEARCH",
          "POSTGRES"
        ]
      },
      "name": "dialect",
      "default": "ELASTICSEARCH",
      "doc": "Body language. Elasticsearch and OpenSearch share {@code ELASTICSEARCH}. {@code POSTGRES}\nstores Mustache text for a future SQL compiler and is not executed by this release."
    },
    {
      "Searchable": {
        "fieldName": "apiVersion",
        "fieldType": "COUNT",
        "queryByDefault": false
      },
      "type": [
        "null",
        "int"
      ],
      "name": "apiVersion",
      "default": null,
      "doc": "Template and Mustache-context contract. This release implements 1. Unset inherits the parent\nversion, or 1 when no ancestor sets it. Any other value is rejected at write and at load."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "capabilities",
      "default": null,
      "doc": "Features this body requires. Unset inherits the parent list. An empty list drops it. A\nnon-empty list replaces it. This release implements {@code keyword}, {@code rescore},\n{@code functionScore}, {@code routes}, {@code ladder}, {@code aggregations},\n{@code structuredPropertyQuery}, {@code normalization}, {@code fieldOverrides},\n{@code highlights}, {@code sort}, {@code boosts}, and {@code queryFragments}. Any other\nname is rejected at write and at load."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "query",
      "default": null,
      "doc": "Mustache text for the query. For {@code ELASTICSEARCH} this renders to one query DSL object.\nFor {@code POSTGRES} it is opaque text and is not parsed as JSON."
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "queryFragments",
      "default": null,
      "doc": "JSON spliced into a named hole in {@code query}. Each level that sets a non-empty key adds a\npiece, joined root-first with commas. An unset key adds nothing. An empty string drops\ninherited pieces for that key. {@code parameters} stays a separate map of named values."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "clauses",
      "default": null,
      "doc": "Mustache JSON object with {@code must}, {@code must_not}, {@code filter}, and {@code should}\narrays. Merged into the resolved query after parent inheritance."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "functionScore",
      "default": null,
      "doc": "Mustache JSON for one function_score wrapper: {@code functions}, {@code score_mode},\n{@code boost_mode}. Java wraps it around the rendered query once."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "rescore",
      "default": null,
      "doc": "Mustache JSON for one rescore clause ({@code window_size}, {@code query}). Unset means no\nrescore."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "knn",
      "default": null,
      "doc": "Mustache JSON for document rerank weights: {@code keyword_weight} and {@code vector_weight}.\n{@code k} and {@code num_candidates} are unused; the candidates are the keyword rows already\nreturned. Unset means this template stays in keyword order. An explicit empty value drops an\ninherited section, so a child keeps the parent's clauses and functionScore. Applied only when\nsemantic read is on and the request includes document entities."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateRanking",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "preset",
              "default": null,
              "doc": "Ranking formula. {@code none}, {@code usage}, or {@code usageAndMatchTier}."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "window",
              "default": null,
              "doc": "How many hits the formula reranks."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "windowSizer",
              "default": null,
              "doc": "How the window size is chosen. {@code fixed} or {@code tokenCurve}."
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "parameters",
              "default": null,
              "doc": "Formula parameters, for example a usage weight."
            }
          ],
          "doc": "Service ranking applied to the keyword page. An explicitly present empty value drops an\ninherited ranking section. {@code usage}, {@code usageAndMatchTier}, and {@code tokenCurve}\nare stored here and executed only by a deployment that advertises them."
        }
      ],
      "name": "ranking",
      "default": null,
      "doc": "Service ranking. Unset keeps the parent section. An explicit empty value drops it. OSS\nruns {@code preset none} and {@code windowSizer fixed} only."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateRetrieval",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "mode",
              "default": null,
              "doc": "{@code keyword} or {@code vector}."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "model",
              "default": null,
              "doc": "Embedding model name for vector retrieval."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "field",
              "default": null,
              "doc": "Vector field."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "candidateCap",
              "default": null,
              "doc": "Cap on vector candidates."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "total",
              "default": null,
              "doc": "How the vector total is counted."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "facets",
              "default": null,
              "doc": "How vector facets are produced."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "page",
              "default": null,
              "doc": "How a vector page is chosen."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "authorization",
              "default": null,
              "doc": "How authorization is applied to vector candidates."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "onFailure",
              "default": null,
              "doc": "What to do when vector retrieval fails."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "SearchTemplateRetrievalBridge",
                  "namespace": "com.linkedin.search.template",
                  "fields": [
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "pivotField",
                      "default": null,
                      "doc": "Field used to find the related candidates."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "onFailure",
                      "default": null,
                      "doc": "What to do when the bridge leg fails. The keyword page is the fallback."
                    }
                  ],
                  "doc": "Second retrieval leg that fills the page from a pivot field. Stored as data. A deployment\nthat does not advertise bridge retrieval fails the search when this section is set."
                }
              ],
              "name": "bridge",
              "default": null,
              "doc": "Optional second retrieval leg."
            }
          ],
          "doc": "How candidates are retrieved before ranking. An explicitly present empty value drops an\ninherited retrieval section. Unset mode is keyword. Vector mode and a bridge are stored as\ndata and executed only by a deployment that advertises them."
        }
      ],
      "name": "retrieval",
      "default": null,
      "doc": "Retrieval mode and vector settings. Unset is keyword. An explicit empty value drops an\ninherited retrieval section."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateLadder",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "select",
              "default": null,
              "doc": "Predicates that must all match before this template is searched. Empty means always attempt.\nA single leading {@code -} inverts a predicate."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "SearchTemplateLadderStop",
                    "namespace": "com.linkedin.search.template",
                    "fields": [
                      {
                        "type": [
                          "null",
                          "int"
                        ],
                        "name": "minHits",
                        "default": null,
                        "doc": "Keep the rung when {@code numEntities} is at least this value."
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "emptyWhen",
                        "default": null,
                        "doc": "Keep an empty rung when the query has this shape. One of {@code scoreSort}, {@code pageSize},\n{@code columnFilter}, {@code singleTerm}, {@code multiWord}, {@code dotted},\n{@code spacelessIdentifier}, {@code identity}, or {@code longQuery}. A single leading\n{@code -} inverts the predicate. {@code columnFilter} is a column constraint.\n{@code longQuery} is four or more tokens after alphanumeric splitting."
                      }
                    ],
                    "doc": "One stop condition. Stop is a disjunction: the rung is kept when any condition matches.\n{@code minHits} matches when the total is at least this value. {@code emptyWhen} matches when\nthe total is zero and the named query shape holds."
                  }
                }
              ],
              "name": "stop",
              "default": null,
              "doc": "Conditions that keep this rung. Required when {@code next} is set and {@code parallel} is\nnot, and when a {@code merge} group sets {@code next}. {@code any} and {@code allEmpty} may\nset {@code next} with no group stop."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "next",
              "default": null,
              "doc": "Template to run when this rung is skipped, fails, or does not stop. A parallel {@code merge}\nfuses member hits and does not search this URN."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "parallel",
              "default": null,
              "doc": "Keyword templates to run together. Each member is one query. The group's {@code next} is the\nfallback when no member remains."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "join",
              "default": null,
              "doc": "How a parallel group is served: {@code any}, {@code allEmpty}, or {@code merge}."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "fusion",
              "default": null,
              "doc": "Merge policy name. {@code interleave}, {@code weightedBlend}, {@code fillFreeSlots}, or\n{@code keywordPreferred}. Stored and checked. The walk's merge stays reciprocal rank."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "fusionCap",
              "default": null,
              "doc": "Cap on hits a merge policy may take from each member."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "facets",
              "default": null,
              "doc": "How merge facets are built. {@code firstMember}, {@code patchFromMembers}, or\n{@code candidates}. {@code candidates} is legal only on a vector retrieval group."
            }
          ],
          "doc": "How this template hands off to another. {@code select} is a conjunction evaluated before the\nquery. {@code stop} is a disjunction evaluated after. {@code next} is the following template.\n{@code parallel} runs those templates together and {@code join} decides which page is served.\n{@code fusion} names how a merge group would combine hits. An explicitly present empty ladder\ndrops an inherited ladder."
        }
      ],
      "name": "ladder",
      "default": null,
      "doc": "Selection, stop, and the next template. Unset means this template is one query. An explicit\nempty value drops an inherited ladder."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "aggregations",
      "default": null,
      "doc": "Mustache JSON object of aggregation name to aggregation body. Unset keeps the annotation\ndefault facets."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "sort",
      "default": null,
      "doc": "Mustache JSON array of sort clauses. Unset, or an explicit empty value, keeps the Java\ndefault of {@code _score} descending then {@code urn} ascending."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchFieldOverrides",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "SearchFieldBoost",
                    "namespace": "com.linkedin.search.template",
                    "fields": [
                      {
                        "type": "string",
                        "name": "fieldName",
                        "doc": "Indexed field name, or a structured-property definition URN. A subfield is separate.\nA nested contribution uses its degree-suffixed indexed name, such as {@code tags_1} for tags\nthat live one relationship away from the entity. {@code tags} is the direct contribution.\nEach degree has its own boost."
                      },
                      {
                        "type": "double",
                        "name": "boost",
                        "doc": "Boost applied to this field. Matches {@code boostScore} on {@code @Searchable}."
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "subfield",
                        "default": null,
                        "doc": "Subfield appended after a structured-property field. Valid values are {@code keyword} and\n{@code text}."
                      }
                    ],
                    "doc": "A queried field and its boost, used by a search template's field overrides."
                  }
                }
              ],
              "name": "add",
              "default": null,
              "doc": "Fields added on top of the base set."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "remove",
              "default": null,
              "doc": "Indexed field names removed from the base set. A nested contribution is removed by its\ndegree-suffixed name, such as {@code tags_1}, which leaves the direct field in the set."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "com.linkedin.search.template.SearchFieldBoost"
                }
              ],
              "name": "replace",
              "default": null,
              "doc": "Wholesale replacement of the base set."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "com.linkedin.search.template.SearchFieldBoost"
                }
              ],
              "name": "override",
              "default": null,
              "doc": "Weight changes for fields that remain in the queried set. Does not add or remove a field."
            }
          ],
          "doc": "Template override of the queried-field set. Applied to the PDL {@code @Searchable}\n{@code queryByDefault} fields. {@code replace} cannot be combined with {@code add},\n{@code remove}, or {@code override}. An explicitly present empty object clears an inherited\noverride. {@code add}, {@code remove}, and {@code override} merge by field with the parent."
        }
      ],
      "name": "fields",
      "default": null,
      "doc": "Template default for queried fields."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "StructuredPropertyQuery",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "StructuredPropertyClause",
                  "namespace": "com.linkedin.search.template",
                  "fields": [
                    {
                      "type": [
                        "boolean",
                        "null"
                      ],
                      "name": "enabled",
                      "default": true,
                      "doc": "When false, this clause does not run. Unset means the clause runs."
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "enum",
                          "name": "StructuredPropertyOperator",
                          "namespace": "com.linkedin.search.template",
                          "symbols": [
                            "AND",
                            "OR"
                          ]
                        }
                      ],
                      "name": "operator",
                      "default": null,
                      "doc": "How analyzed tokens combine. Exact and name-and-value clauses ignore this.\nUnset means OR."
                    },
                    {
                      "type": [
                        "null",
                        "float"
                      ],
                      "name": "weight",
                      "default": null,
                      "doc": "Positive multiplier. Analyzed and exact clauses multiply the field weight by\nthis value; unset means 1. Name-and-value uses this value as the term weight\nand requires it when the clause runs."
                    }
                  ],
                  "doc": "One structured-property clause. The template decides that the clause runs.\nJava resolves the current property definitions and keeps the clause on fields\nthe template already selected. {@code enabled} false keeps the clause in an\ninherited section and does not run it."
                }
              ],
              "name": "analyzedValue",
              "default": null,
              "doc": "Analyzed {@code .text} and {@code .stemmed} matching. Unset does not run this\nclause unless a parent set it."
            },
            {
              "type": [
                "null",
                "com.linkedin.search.template.StructuredPropertyClause"
              ],
              "name": "exactValue",
              "default": null,
              "doc": "Exact keyword or whole-term matching. Unset does not run this clause unless a\nparent set it."
            },
            {
              "type": [
                "null",
                "com.linkedin.search.template.StructuredPropertyClause"
              ],
              "name": "nameAndValue",
              "default": null,
              "doc": "Name-and-value span matching for string and rich-text properties. Unset does\nnot run this clause unless a parent set it. A running clause requires a\npositive weight."
            }
          ],
          "doc": "Structured-property clauses added beside the rendered query. Java resolves which\nproperties are indexed, their field paths, and their value types. The template\nchooses which clauses run and their operator and weight. An explicitly present\nempty value drops an inherited section. A child replaces only the clauses it sets.\nAn unset clause keeps the parent clause."
        }
      ],
      "name": "structuredPropertyQuery",
      "default": null,
      "doc": "Analyzed, exact, and name-and-value clauses for structured properties. Unset inherits\nthe parent clause by clause. An explicit empty value drops an inherited section. The\ntemplate does not store per-property clauses or resolved field paths. A running\nname-and-value clause requires a positive weight. Other weights, when set, must be\npositive."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateNormalization",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                "int"
              ],
              "name": "wildcardMinimumLength",
              "default": null,
              "doc": "Minimum characters of a single token before a contains-wildcard is added.\nUnset inherits, or 5 when no ancestor sets it."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "synonymCap",
              "default": null,
              "doc": "Maximum synonym expansions placed in the query context. Unset inherits, or 8\nwhen no ancestor sets it. The clause budget can lower this further."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "fuzzyTokenThreshold",
              "default": null,
              "doc": "Minimum token length before a fuzzy edit is applied. Unset inherits, or 5\nwhen no ancestor sets it."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "mediumFuzzyTokenThreshold",
              "default": null,
              "doc": "Token length where {@code mediumFuzzyEdits} replaces {@code shortFuzzyEdits}.\nUnset inherits, or two characters above {@code fuzzyTokenThreshold} when no\nancestor sets it."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "broadQueryTokenCap",
              "default": null,
              "doc": "Maximum tokens kept in {@code broadQuery}. Unset inherits, or 8 when no\nancestor sets it. A template may raise this up to 256. The clause budget\ncan keep fewer."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "shortFuzzyEdits",
              "default": null,
              "doc": "Fuzzy edits for a token at least {@code fuzzyTokenThreshold} long and shorter\nthan {@code mediumFuzzyTokenThreshold}. Unset inherits, or 1 when no ancestor\nsets it."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "mediumFuzzyEdits",
              "default": null,
              "doc": "Fuzzy edits for a token at least {@code mediumFuzzyTokenThreshold} long.\nUnset inherits, or 2 when no ancestor sets it."
            }
          ],
          "doc": "Limits and edit distances for query text Java prepares before rendering.\nUnset keeps the parent value. An explicitly present empty value drops the\ninherited section. Java clamps every value to a safe range and to the clause\nbudget. Authorization and system filters are not part of this section."
        }
      ],
      "name": "normalization",
      "default": null,
      "doc": "Wildcard, synonym, fuzzy, and broad-query limits. Unset inherits field by field. An\nexplicit empty value drops an inherited section. Java clamps each value and the clause\nbudget can keep fewer synonyms, fuzzy expansions, or broad-query tokens."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "HighlightFieldOverrides",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "add",
              "default": null,
              "doc": "Field names added on top of the base set."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "remove",
              "default": null,
              "doc": "Field names removed from the base set."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "replace",
              "default": null,
              "doc": "Wholesale replacement of the base set."
            }
          ],
          "doc": "Template override of the highlight-field set. Applied to the schema-derived highlight fields.\n{@code replace} cannot be combined with {@code add} or {@code remove}. An explicitly present\nempty object clears an inherited override. Whether highlighting runs stays on\n{@code SearchFlags.skipHighlighting}."
        }
      ],
      "name": "highlightFields",
      "default": null,
      "doc": "Template default for highlight fields."
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "parameters",
      "default": null,
      "doc": "User Mustache variables, for example {@code deprecatedBoost}."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "SearchTemplateBoost",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": "string",
                "name": "field",
                "doc": "Field the terms filter matches. A structured property is its definition URN. A nested\ncontribution uses its degree-suffixed indexed name, such as {@code tags_1}."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "subfield",
                "default": null,
                "doc": "Subfield appended after a structured-property field. {@code keyword} or {@code text}."
              },
              {
                "type": "string",
                "name": "values",
                "doc": "Injectable list the service fills for the request."
              },
              {
                "type": "double",
                "name": "weight",
                "doc": "Weight applied when the field matches one value in the list."
              }
            ],
            "doc": "A closed list-membership boost. The service renders a terms filter for {@code values} and\napplies {@code weight}. {@code values} is one of {@code viewerUrn}, {@code viewerGroupsJson},\n{@code viewerDomainsJson}, or {@code domainsJson}."
          }
        }
      ],
      "name": "boosts",
      "default": null,
      "doc": "List-membership weights. Each entry names a field, an injectable list, and a weight. An empty\narray drops an inherited list. A child that sets this replaces the parent's list."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "SearchTemplateRoute",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": "string",
                "name": "queryRegex",
                "doc": "RE2 pattern matched against the whole trimmed query."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "template",
                "doc": "Template to run when the pattern matches."
              }
            ],
            "doc": "One ordered route. The pattern is RE2 and must match the whole trimmed query."
          }
        }
      ],
      "name": "routes",
      "default": null,
      "doc": "Ordered RE2 routes. The first pattern that matches the whole trimmed query selects the\ntemplate. Unset keeps the parent list. An empty list drops it. A non-empty list replaces it."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateDefaultRoute",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "template",
              "default": null,
              "doc": "Template to run when no pattern matches. Absent clears an inherited default."
            }
          ],
          "doc": "Where a query goes when no route pattern matches. An explicit record with no\ntemplate clears an inherited default, and the miss stays on this template."
        }
      ],
      "name": "defaultRoute",
      "default": null,
      "doc": "Template used when no route pattern matches. Unset keeps the parent default. A record with\na template replaces it. A record with no template clears it, and the miss stays on this body."
    },
    {
      "Relationship": {
        "entityTypes": [
          "searchTemplate"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "fieldName": "parent",
        "fieldType": "URN",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parent",
      "default": null,
      "doc": "Parent template. The child replaces only the sections it sets. The parent must use the same\ndialect. Indexed so a write can load the templates whose parent is this URN. The IsPartOf\nedge is removed when this template or its parent is hard deleted."
    },
    {
      "Searchable": {
        "fieldName": "editable",
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": "boolean",
      "name": "editable",
      "default": true,
      "doc": "When false, only the system actor may change this template."
    }
  ],
  "doc": "A Mustache-rendered search query template. Sections that are unset are inherited from\n{@code parent}. An explicitly present empty string for {@code query}, {@code clauses},\n{@code functionScore}, {@code rescore}, {@code knn}, or {@code ladder} drops that inherited\nsection. An explicitly present empty {@code ranking} or {@code retrieval} drops that inherited\nsection. An explicitly present empty {@code fields}, {@code highlightFields},\n{@code aggregations}, or {@code sort} drops the inherited override and falls back to the schema\nor Java default. An explicitly present empty {@code boosts} drops an inherited list.\nAn explicitly present empty {@code structuredPropertyQuery} drops an inherited section.\nA child replaces only the structured-property clauses it sets. An explicitly present\nempty {@code normalization} drops inherited query-normalization settings. A child\nreplaces only the normalization fields it sets. Each non-empty {@code queryFragments}\nvalue adds to the root-first comma join for that hole. An empty string drops inherited\npieces for that hole.\n{@code routes} is replaced as a whole list: unset keeps the parent list, an empty list\ndrops it, and a non-empty list replaces it. {@code defaultRoute} is inherited on its own.\nUnset keeps the parent default. A record with a template replaces it. A record with no\ntemplate clears it.\n\n{@code apiVersion} is the template and Mustache-context contract. Unset keeps the parent\nversion. When no ancestor sets it, the resolved version is 1. A value other than 1 is rejected\nwhen the template is written and when it is loaded. An accepted write stores 1 when the caller\nomits it. {@code capabilities} names features this body requires. Unset keeps the parent list.\nAn empty list drops it. A non-empty list replaces it. A name this release does not implement is\nrejected when the template is written and when it is loaded.\n\n{@code dialect}, {@code apiVersion}, {@code parent}, and {@code editable} are searchable with\n{@code queryByDefault} false. A write uses {@code parent} to find its children; the other\nfields support template administration filters. {@code aspectJson} stores this aspect's JSON,\nthe same JSON stored in SQL, at\n{@code _aspects.searchTemplateProperties._aspectJson} without indexing it, so a template\nreload can read the body from search. Write time and actor stay on system metadata\n({@code aspectCreated}, {@code aspectModified}): SQL stores them on the aspect row, and search\ncopies them to {@code _system}. The entity stays out of the default search, browse, and\nautocomplete entity-type lists."
}
```





### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- IsPartOf (via `searchTemplateProperties.parent`)
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
