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
section. An explicitly present empty {@code component} drops an inherited component.
An explicitly present empty {@code fields}, {@code highlightFields},
{@code aggregations}, or {@code sort} drops the inherited override and falls back to the schema
or Java default. An explicitly present empty {@code boosts}, {@code fieldValueScores},
{@code fieldFactors}, {@code termWeights}, or {@code entityTypeBoosts} drops an inherited
list. Unset {@code scoreMode}, {@code boostMode}, and {@code searchType} keep the parent value.
An explicitly present empty {@code structuredPropertyQuery} drops an inherited section.
A child replaces only the structured-property clauses it sets. An explicitly present
empty {@code normalization} drops inherited query-normalization settings. A child
replaces only the normalization fields it sets. Each non-empty {@code queryFragments}
value adds to the root-first comma join for that hole. An empty string drops inherited
pieces for that hole.
{@code ranking.template} unset inherits, empty drops, and a URN replaces. On an inline
ranking, {@code signals} follows {@code fieldFactors}: unset inherits, an empty list drops,
and a non-empty list replaces. {@code formula} unset inherits, and an empty string drops it.
A reference and any inline ranking field together are invalid. An empty {@code ranking}
record drops the section. {@code routes} is replaced as a whole list: unset keeps the parent
list, an empty list drops it, and a non-empty list replaces it. {@code defaultRoute} is
inherited on its own.
Unset keeps the parent default. A record with a template replaces it. A record with no
template clears it.
{@code matchedFields} is inherited. When no template in the chain sets it, it resolves to true.

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
| scoreMode | SearchTemplateScoreMode |  | How functions inside the function score combine. Unset keeps the parent mode. When no ancestor se... |  |
| boostMode | SearchTemplateBoostMode |  | How the function score combines with the query score. Unset keeps the parent mode. When no ancest... |  |
| searchType | SearchTemplateSearchType |  | Elasticsearch search type for this template. Unset keeps the parent type. When no ancestor sets i... |  |
| fieldFactors | FieldFactor[] |  | Numeric field factors appended inside the function score. Each entry names an index field or a st... |  |
| termWeights | TermWeight[] |  | Term weights appended inside the function score, after {@code fieldFactors}. Each entry names an ... |  |
| rescore | string |  | Mustache JSON for one rescore clause ({@code window_size}, {@code query}). Unset means no rescore. |  |
| knn | string |  | Mustache JSON for document rerank weights: {@code keyword_weight} and {@code vector_weight}. {@co... |  |
| ranking | SearchTemplateRanking |  | Service ranking. Unset keeps the parent section. An explicit empty value drops it. A reference ({... |  |
| component | SearchTemplateComponent |  | Named component. Unset inherits the parent. An explicit empty record drops it. {@code mode} is re... |  |
| retrieval | SearchTemplateRetrieval |  | Retrieval mode and vector settings. Unset is keyword. An explicit empty value drops an inherited ... |  |
| ladder | SearchTemplateLadder |  | Selection, stop, and the next template. Unset means this template is one query. An explicit empty... |  |
| aggregations | string |  | Mustache JSON object of aggregation name to aggregation body. Unset keeps the annotation default ... |  |
| sort | string |  | Mustache JSON array of sort clauses. Unset, or an explicit empty value, keeps the Java default of... |  |
| fields | SearchFieldOverrides |  | Template default for queried fields. |  |
| structuredPropertyQuery | StructuredPropertyQuery |  | Analyzed, exact, and name-and-value clauses for structured properties. Unset inherits the parent ... |  |
| fieldValueScores | FieldValueScore[] |  | Numeric values that multiply the rendered query. Each entry names an index field or a structured ... |  |
| entityTypeBoosts | EntityTypeBoost[] |  | Entity-type weights multiplied onto the rendered query. Unset inherits. An empty array drops the ... |  |
| normalization | SearchTemplateNormalization |  | Wildcard, synonym, fuzzy, and broad-query limits. Unset inherits field by field. An explicit empt... |  |
| highlightFields | HighlightFieldOverrides |  | Template default for highlight fields. |  |
| matchedFields | boolean |  | Whether this template reports non-highlight matched fields from relevance clauses and caller-supp... |  |
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
    "name": "searchTemplateProperties",
    "schemaVersion": 4
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
      "doc": "Features this body requires. Unset inherits the parent list. An empty list drops it. A\nnon-empty list replaces it. This release implements {@code keyword}, {@code rescore},\n{@code functionScore}, {@code routes}, {@code ladder}, {@code aggregations},\n{@code structuredPropertyQuery}, {@code fieldValueScores}, {@code fieldFactors},\n{@code termWeights}, {@code entityTypeBoosts},\n{@code normalization}, {@code fieldOverrides},\n{@code highlights}, {@code sort}, {@code boosts}, {@code queryFragments}, and\n{@code component}. Any other name is rejected at write and at load."
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
      "doc": "Mustache JSON for one function_score wrapper: {@code functions}, {@code score_mode},\n{@code boost_mode}. Java wraps it around the rendered query once. {@code fieldFactors}\nand {@code termWeights} are appended to {@code functions}. {@code scoreMode} and\n{@code boostMode}, when set, replace the modes in this JSON."
    },
    {
      "type": [
        "null",
        {
          "type": "enum",
          "name": "SearchTemplateScoreMode",
          "namespace": "com.linkedin.search.template",
          "symbols": [
            "MULTIPLY",
            "SUM",
            "AVG",
            "FIRST",
            "MAX",
            "MIN"
          ],
          "doc": "How functions inside one function_score combine. Unset leaves the mode in the\nfunction-score JSON."
        }
      ],
      "name": "scoreMode",
      "default": null,
      "doc": "How functions inside the function score combine. Unset keeps the parent mode. When no\nancestor sets this, the mode in {@code functionScore} stands."
    },
    {
      "type": [
        "null",
        {
          "type": "enum",
          "name": "SearchTemplateBoostMode",
          "namespace": "com.linkedin.search.template",
          "symbols": [
            "MULTIPLY",
            "REPLACE",
            "SUM",
            "AVG",
            "MAX",
            "MIN"
          ],
          "doc": "How a function_score combines with the query score. Unset leaves the mode in the\nfunction-score JSON."
        }
      ],
      "name": "boostMode",
      "default": null,
      "doc": "How the function score combines with the query score. Unset keeps the parent mode. When\nno ancestor sets this, the mode in {@code functionScore} stands."
    },
    {
      "type": [
        "null",
        {
          "type": "enum",
          "name": "SearchTemplateSearchType",
          "namespace": "com.linkedin.search.template",
          "symbols": [
            "QUERY_THEN_FETCH",
            "DFS_QUERY_THEN_FETCH"
          ],
          "doc": "Elasticsearch search type applied when this template is executed. It is not part of the query\nJSON. Unset keeps the parent value. When no ancestor sets it, the template request uses\n{@code QUERY_THEN_FETCH}. The request flag is not consulted."
        }
      ],
      "name": "searchType",
      "default": null,
      "doc": "Elasticsearch search type for this template. Unset keeps the parent type. When no ancestor\nsets it, the template request uses {@code QUERY_THEN_FETCH}. A set value applies to that\ntemplate, including each ladder step that selects it. The request flag is not consulted.\nThe value is not part of the query JSON."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FieldFactor",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "field",
                "default": null,
                "doc": "Index field whose stored number is the value. Unset when {@code propertyUrn} names the\nvalue."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "propertyUrn",
                "default": null,
                "doc": "Structured property definition. Unset when {@code field} names the value. The property\nis a single-valued number."
              },
              {
                "type": [
                  "null",
                  "double"
                ],
                "name": "factor",
                "default": null,
                "doc": "Positive multiplier applied to the field value. Unset omits {@code factor} from the\nfunction, which then uses 1. A negative stored value is not scored, except under\n{@code SQUARE}, whose result stays non-negative."
              },
              {
                "type": [
                  "null",
                  "double"
                ],
                "name": "missing",
                "default": null,
                "doc": "Non-negative value used when the field is absent. {@code RECIPROCAL} requires it to be\npositive. Unset omits {@code missing} from the function."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "enum",
                    "name": "FieldFactorModifier",
                    "namespace": "com.linkedin.search.template",
                    "symbols": [
                      "LOG1P",
                      "LN1P",
                      "SQRT",
                      "SQUARE",
                      "RECIPROCAL"
                    ],
                    "doc": "Elasticsearch field_value_factor modifier. Unset leaves the modifier off the function."
                  }
                ],
                "name": "modifier",
                "default": null,
                "doc": "Modifier applied to the field value. Unset omits {@code modifier} from the function."
              }
            ],
            "doc": "One field_value_factor appended inside the template function score. The value comes\nfrom an index field or from a structured property definition. Java resolves a property\nto its current number field. An omitted factor or missing value is omitted from the\nfunction. An entry names a field or a property."
          }
        }
      ],
      "name": "fieldFactors",
      "default": null,
      "doc": "Numeric field factors appended inside the function score. Each entry names an index\nfield or a structured property. Unset inherits. An empty array drops the inherited list.\nA non-empty array replaces it. Omitted factor and missing values are omitted from the\nfunction. The template does not store a property's resolved field path."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "TermWeight",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "field",
                "default": null,
                "doc": "Index field the term matches. Unset when {@code propertyUrn} names the value."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "propertyUrn",
                "default": null,
                "doc": "Structured property definition. Unset when {@code field} names the value."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "subfield",
                "default": null,
                "doc": "Subfield appended after a structured-property field. {@code keyword} or {@code text}.\nUnset uses the property field itself."
              },
              {
                "type": "string",
                "name": "value",
                "doc": "Term value. One JSON scalar: a boolean, number, or quoted string."
              },
              {
                "type": "double",
                "name": "weight",
                "doc": "Weight applied when the field equals {@code value}."
              }
            ],
            "doc": "One term filter and weight appended inside the template function score. The term\nmatches an index field or a structured property definition. Java resolves a property\nto its current index field. {@code value} is one JSON scalar, so {@code true} stays a\nboolean. An entry names a field or a property."
          }
        }
      ],
      "name": "termWeights",
      "default": null,
      "doc": "Term weights appended inside the function score, after {@code fieldFactors}. Each entry\nnames an index field or a structured property. Unset inherits. An empty array drops the\ninherited list. A non-empty array replaces it. {@code value} is one JSON scalar. The\ntemplate does not store a property's resolved field path."
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
              "doc": "Ranking preset. {@code none}, {@code usage}, or {@code usageAndMatchTier}. Unset when\n{@code signals} is set. {@code none} means no signals and no {@code template}."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "window",
              "default": null,
              "doc": "How many hits the formula reranks. Passed to {@code windowSizer}."
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
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "template",
              "default": null,
              "doc": "Search-template URN whose inline ranking this query uses. One hop. The target is inline.\nThe target's query and functionScore are not used."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": {
                    "type": "record",
                    "name": "RankingSignal",
                    "namespace": "com.linkedin.search.template",
                    "fields": [
                      {
                        "type": "string",
                        "name": "name",
                        "doc": "Formula variable. Matches {@code [A-Za-z_][A-Za-z0-9_]*}."
                      },
                      {
                        "type": {
                          "type": "enum",
                          "name": "RankingSignalKind",
                          "namespace": "com.linkedin.search.template",
                          "symbols": [
                            "SCORE",
                            "FIELD_NUMBER",
                            "FIELD_BOOLEAN",
                            "FIELD_KEYWORD",
                            "FIELD_AGE_DAYS",
                            "NAME_MATCH",
                            "QUALIFIED_NAME_MATCH",
                            "ALL_TERMS_MATCH",
                            "NON_PRODUCTION_NAME",
                            "BACKUP_NAME",
                            "SYSTEM_TAG",
                            "TAG_WEIGHTS",
                            "STRUCTURED_PROPERTY"
                          ],
                          "doc": "How a ranking signal reads a candidate. The value is normalized, then raised to the signal boost."
                        },
                        "name": "kind"
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "field",
                        "default": null,
                        "doc": "Index field for a field kind. Unset for score, match, penalty, tag, and structured-property\nsignals."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "enum",
                            "name": "RankingValueAggregation",
                            "namespace": "com.linkedin.search.template",
                            "symbols": [
                              "MAX",
                              "MIN",
                              "SUM",
                              "AVERAGE"
                            ],
                            "doc": "How a numeric ranking signal combines multiple values from one candidate."
                          }
                        ],
                        "name": "valueAggregation",
                        "default": null,
                        "doc": "How a numeric field, timestamp, or structured-property signal combines multiple values.\nUnset uses {@code MAX}. A number or structured property may set {@code MIN}, {@code SUM},\nor {@code AVERAGE}. An age may set {@code MIN}. {@code MAX} is the newest timestamp and\n{@code MIN} is the oldest. {@code SUM} and {@code AVERAGE} are rejected on an age."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "array",
                            "items": "string"
                          }
                        ],
                        "name": "entityTypes",
                        "default": null,
                        "doc": "Registry entity names this signal applies to. At least one when set. A document of another\ntype contributes the neutral value. Unset does not restrict the type. A name also listed in\n{@code excludedEntityTypes} is rejected."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "array",
                            "items": "string"
                          }
                        ],
                        "name": "excludedEntityTypes",
                        "default": null,
                        "doc": "Registry entity names this signal skips. At least one when set. A document of a listed type\ncontributes the neutral value. Unset skips nothing."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "record",
                            "name": "RankingBooleanMatch",
                            "namespace": "com.linkedin.search.template",
                            "fields": [
                              {
                                "type": [
                                  "null",
                                  {
                                    "type": "enum",
                                    "symbolDocs": {
                                      "AND": "The signal is true when every value matches.",
                                      "OR": "The signal is true when any value matches. {@code NOT_EXISTS} with {@code FALSE} is true when\nthe field is missing or false."
                                    },
                                    "name": "RankingBooleanOperator",
                                    "namespace": "com.linkedin.search.template",
                                    "symbols": [
                                      "OR",
                                      "AND"
                                    ],
                                    "doc": "How the values in a {@code booleanMatch} combine. Unset is {@code OR}."
                                  }
                                ],
                                "name": "operator",
                                "default": null,
                                "doc": "Unset is {@code OR}."
                              },
                              {
                                "type": {
                                  "type": "array",
                                  "items": {
                                    "type": "enum",
                                    "symbolDocs": {
                                      "EXISTS": "The stored field is present, including a stored false.",
                                      "FALSE": "The stored value is false. A missing field does not match.",
                                      "NOT_EXISTS": "The stored field is missing.",
                                      "TRUE": "The stored value is true. A missing field does not match."
                                    },
                                    "name": "RankingBooleanValue",
                                    "namespace": "com.linkedin.search.template",
                                    "symbols": [
                                      "TRUE",
                                      "FALSE",
                                      "EXISTS",
                                      "NOT_EXISTS"
                                    ],
                                    "doc": "One way a stored boolean can match. A {@code booleanMatch} lists one or more of these."
                                  }
                                },
                                "name": "values",
                                "doc": "At least one of {@code TRUE}, {@code FALSE}, {@code EXISTS}, or {@code NOT_EXISTS}."
                              }
                            ],
                            "doc": "When a boolean signal is true. Unset reads the stored boolean, the same as {@code OR} of\n{@code TRUE}. Quote {@code TRUE} and {@code FALSE} in YAML."
                          }
                        ],
                        "name": "booleanMatch",
                        "default": null,
                        "doc": "When a boolean signal is true. Only a {@code FIELD_BOOLEAN} signal may set this. Unset reads\nthe stored boolean."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "record",
                            "name": "RankingKeywordMatch",
                            "namespace": "com.linkedin.search.template",
                            "fields": [
                              {
                                "type": [
                                  "null",
                                  "com.linkedin.search.template.RankingBooleanOperator"
                                ],
                                "name": "operator",
                                "default": null,
                                "doc": "How {@code values} combine, and how that clause combines with {@code presence}. Unset is\n{@code OR}."
                              },
                              {
                                "type": [
                                  "null",
                                  {
                                    "type": "array",
                                    "items": "string"
                                  }
                                ],
                                "name": "values",
                                "default": null,
                                "doc": "Tokens that count. At least one when set. {@code OR} matches any token. {@code AND} matches\nevery token."
                              },
                              {
                                "type": [
                                  "null",
                                  {
                                    "type": "array",
                                    "items": "string"
                                  }
                                ],
                                "name": "excludedValues",
                                "default": null,
                                "doc": "Tokens that do not count. At least one when set. A stored token from this list fails the\nvalue clause. A token also listed in {@code values} is rejected."
                              },
                              {
                                "type": [
                                  "null",
                                  {
                                    "type": "enum",
                                    "symbolDocs": {
                                      "EXISTS": "The field has at least one token.",
                                      "NOT_EXISTS": "The field has no token."
                                    },
                                    "name": "RankingKeywordPresence",
                                    "namespace": "com.linkedin.search.template",
                                    "symbols": [
                                      "EXISTS",
                                      "NOT_EXISTS"
                                    ],
                                    "doc": "Whether a keyword field is present. A stored token is present. A missing field or an empty list\nis not."
                                  }
                                ],
                                "name": "presence",
                                "default": null,
                                "doc": "Presence clause. {@code AND} of {@code NOT_EXISTS} with either token list is rejected."
                              }
                            ],
                            "doc": "When a keyword signal is true. At least one of {@code values}, {@code excludedValues}, or\n{@code presence} is set."
                          }
                        ],
                        "name": "keywordMatch",
                        "default": null,
                        "doc": "When a keyword signal is true. Only a {@code FIELD_KEYWORD} signal may set this, and that\nkind requires it."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "propertyUrn",
                        "default": null,
                        "doc": "Structured property definition. Set only for {@code STRUCTURED_PROPERTY}."
                      },
                      {
                        "type": {
                          "type": "record",
                          "name": "RankingNormalization",
                          "namespace": "com.linkedin.search.template",
                          "fields": [
                            {
                              "type": {
                                "type": "enum",
                                "name": "RankingNormalizationType",
                                "namespace": "com.linkedin.search.template",
                                "symbols": [
                                  "SIGMOID",
                                  "LOG_SIGMOID",
                                  "LINEAR",
                                  "LINEAR_DECAY",
                                  "BOOLEAN",
                                  "NONE"
                                ],
                                "doc": "How a raw signal value becomes the number that the boost exponent is applied to."
                              },
                              "name": "type"
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "inputMin",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "inputMax",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "steepness",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "scale",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "outputMin",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "outputMax",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "trueValue",
                              "default": null
                            },
                            {
                              "type": [
                                "null",
                                "double"
                              ],
                              "name": "falseValue",
                              "default": null
                            }
                          ],
                          "doc": "Bounds and shape for one ranking signal. Only the fields for {@code type} are read."
                        },
                        "name": "normalization"
                      },
                      {
                        "type": "double",
                        "name": "boost",
                        "doc": "Exponent applied to the normalized value. The result is the signal contribution."
                      },
                      {
                        "type": [
                          "null",
                          {
                            "type": "map",
                            "values": "double"
                          }
                        ],
                        "name": "tagWeights",
                        "default": null,
                        "doc": "Tag URN to multiplier. Empty is neutral. Set only for {@code TAG_WEIGHTS}."
                      }
                    ],
                    "doc": "One Stage 2 signal. {@code name} is the formula variable. {@code field} and {@code propertyUrn}\nare never both set. A structured-property signal names the property definition; the index field\nis resolved when the signal is scored."
                  }
                }
              ],
              "name": "signals",
              "default": null,
              "doc": "Stage 2 signals. Unset when {@code template} is set. A non-empty list replaces the\ninherited list. Cannot be combined with {@code add} or {@code remove}."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "com.linkedin.search.template.RankingSignal"
                }
              ],
              "name": "add",
              "default": null,
              "doc": "Signals merged into the inherited list by {@code name}. A new name is appended. The same\nname replaces that signal and keeps its position. A name listed in {@code remove} is put\nback. Duplicate names in this list are rejected. May sit beside {@code template}."
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
              "doc": "Signal names dropped from the inherited list and from inherited {@code add}. May sit beside\n{@code template}. A name that is not in the resolved list is rejected."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "formula",
              "default": null,
              "doc": "Expression over signal contributions, each name meaning {@code pow(normalized, boost)}.\nUnset means the product of every contribution."
            }
          ],
          "doc": "Service ranking applied to the keyword page. An explicitly present empty value drops an\ninherited ranking section. A ranking is a reference or it is inline, never both.\n{@code template} unset inherits, empty drops, and a URN replaces. On an inline ranking,\n{@code signals} unset inherits, an empty list drops, and a non-empty list replaces.\n{@code add} and {@code remove} patch the inherited list by signal name and may sit beside\n{@code template}. A non-empty {@code signals} list discards those patches. {@code formula}\nunset inherits, and an empty string drops it. {@code preset} is an inline field.\n{@code usage}, {@code usageAndMatchTier}, and {@code tokenCurve} are stored here and\nexecuted only by a deployment that advertises them. A non-empty {@code signals} list is the\nJava rescore."
        }
      ],
      "name": "ranking",
      "default": null,
      "doc": "Service ranking. Unset keeps the parent section. An explicit empty value drops it. A\nreference ({@code template}) or an inline signal list, never both. {@code add} and\n{@code remove} patch the inherited signals by name and may sit beside a reference. OSS runs\n{@code preset none} and {@code windowSizer fixed} only, and does not ship ranking signals.\nCapability {@code rankingSignals} is required when signals or {@code ranking.template} are\nset. Capability {@code rankingFormula} is required when {@code formula} is set."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SearchTemplateComponent",
          "namespace": "com.linkedin.search.template",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "name",
              "default": null,
              "doc": "Registry name."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "version",
              "default": null,
              "doc": "Contract version. The registry key is name and version."
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "name": "SearchTemplateComponentMode",
                  "namespace": "com.linkedin.search.template",
                  "symbols": [
                    "POST_PROCESS",
                    "OWN_RUNG"
                  ],
                  "doc": "How a named template component runs. Stored on the template, not chosen by component code.\nPOST_PROCESS reorders the keyword window the standard executor already fetched.\nOWN_RUNG owns retrieval, ranking, and slicing, and leaves the template query unread."
                }
              ],
              "name": "mode",
              "default": null,
              "doc": "Required when the component is set. POST_PROCESS reorders the keyword window.\nOWN_RUNG owns the rung."
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "config",
              "default": null,
              "doc": "Opaque string configuration. The registered component interprets it."
            }
          ],
          "doc": "A code-registered component. The template stores a name, a contract version, a mode, and\nopaque config. It does not name a class. Unset inherits the parent. An explicit empty\nrecord drops an inherited component."
        }
      ],
      "name": "component",
      "default": null,
      "doc": "Named component. Unset inherits the parent. An explicit empty record drops it. {@code mode}\nis required when the component is set. OSS registers no components, so a set component fails\nclosed on an OSS process."
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
          "type": "array",
          "items": {
            "type": "record",
            "name": "FieldValueScore",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "field",
                "default": null,
                "doc": "Index field whose stored number is the value. Unset when {@code propertyUrn} names the\nvalue."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "propertyUrn",
                "default": null,
                "doc": "Structured property definition. Unset when {@code field} names the value."
              },
              {
                "type": [
                  "null",
                  "double"
                ],
                "name": "inputMin",
                "default": null,
                "doc": "Values at or below this keep a neutral multiplier. A property may omit this and use the\ndefinition. A field sets it here."
              },
              {
                "type": [
                  "null",
                  "double"
                ],
                "name": "inputMax",
                "default": null,
                "doc": "Values at or above this receive {@code maxMultiplier}. A property may omit this and use\nthe definition. A field sets it here."
              },
              {
                "type": [
                  "null",
                  "double"
                ],
                "name": "maxMultiplier",
                "default": null,
                "doc": "Multiplier at {@code inputMax}. Values between the bounds map linearly from 1. A\nproperty may omit this and use the definition. A field sets it here."
              }
            ],
            "doc": "One numeric value that multiplies the rendered query. An entry names an index field or a\nstructured property, never neither and never both. A field sets {@code inputMin},\n{@code inputMax}, and {@code maxMultiplier} here. A property may omit any of those three.\nEach omitted value is read from that property's {@code score} when the query is rendered.\nA value set here replaces the definition. Java resolves a property to its current index\nfield. The template does not store that path."
          }
        }
      ],
      "name": "fieldValueScores",
      "default": null,
      "doc": "Numeric values that multiply the rendered query. Each entry names an index field or a\nstructured property, never neither and never both. Unset inherits. An empty array drops\nthe inherited list. A non-empty array replaces it. A field sets its extents here. A\nproperty may omit them; each omitted value is read from that property's {@code score}\nwhen the query is rendered, and a value set here replaces the definition. The template\ndoes not store a property's resolved field path. Naming a property is what turns scoring\non. The shipped templates do not set this section."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "EntityTypeBoost",
            "namespace": "com.linkedin.search.template",
            "fields": [
              {
                "type": "string",
                "name": "entityType",
                "doc": "Entity type name, such as {@code glossaryTerm}."
              },
              {
                "type": "double",
                "name": "weight",
                "doc": "Positive multiplier for documents of {@code entityType}."
              }
            ],
            "doc": "A positive weight multiplied onto documents of one entity type. Each entry sets both\n{@code entityType} and {@code weight}. A weight of 1 is omitted when the query is\nrendered because it does not change order."
          }
        }
      ],
      "name": "entityTypeBoosts",
      "default": null,
      "doc": "Entity-type weights multiplied onto the rendered query. Unset inherits. An empty array\ndrops the inherited list. A non-empty array replaces it. Each entry sets an entity type\nand a positive weight. A weight of 1 is omitted when the query is rendered. A request\nthat names one entity type skips the section."
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
        "boolean"
      ],
      "name": "matchedFields",
      "default": null,
      "doc": "Whether this template reports non-highlight matched fields from relevance clauses and\ncaller-supplied optional filters. Unset inherits; the resolved default is true."
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
  "doc": "A Mustache-rendered search query template. Sections that are unset are inherited from\n{@code parent}. An explicitly present empty string for {@code query}, {@code clauses},\n{@code functionScore}, {@code rescore}, {@code knn}, or {@code ladder} drops that inherited\nsection. An explicitly present empty {@code ranking} or {@code retrieval} drops that inherited\nsection. An explicitly present empty {@code component} drops an inherited component.\nAn explicitly present empty {@code fields}, {@code highlightFields},\n{@code aggregations}, or {@code sort} drops the inherited override and falls back to the schema\nor Java default. An explicitly present empty {@code boosts}, {@code fieldValueScores},\n{@code fieldFactors}, {@code termWeights}, or {@code entityTypeBoosts} drops an inherited\nlist. Unset {@code scoreMode}, {@code boostMode}, and {@code searchType} keep the parent value.\nAn explicitly present empty {@code structuredPropertyQuery} drops an inherited section.\nA child replaces only the structured-property clauses it sets. An explicitly present\nempty {@code normalization} drops inherited query-normalization settings. A child\nreplaces only the normalization fields it sets. Each non-empty {@code queryFragments}\nvalue adds to the root-first comma join for that hole. An empty string drops inherited\npieces for that hole.\n{@code ranking.template} unset inherits, empty drops, and a URN replaces. On an inline\nranking, {@code signals} follows {@code fieldFactors}: unset inherits, an empty list drops,\nand a non-empty list replaces. {@code formula} unset inherits, and an empty string drops it.\nA reference and any inline ranking field together are invalid. An empty {@code ranking}\nrecord drops the section. {@code routes} is replaced as a whole list: unset keeps the parent\nlist, an empty list drops it, and a non-empty list replaces it. {@code defaultRoute} is\ninherited on its own.\nUnset keeps the parent default. A record with a template replaces it. A record with no\ntemplate clears it.\n{@code matchedFields} is inherited. When no template in the chain sets it, it resolves to true.\n\n{@code apiVersion} is the template and Mustache-context contract. Unset keeps the parent\nversion. When no ancestor sets it, the resolved version is 1. A value other than 1 is rejected\nwhen the template is written and when it is loaded. An accepted write stores 1 when the caller\nomits it. {@code capabilities} names features this body requires. Unset keeps the parent list.\nAn empty list drops it. A non-empty list replaces it. A name this release does not implement is\nrejected when the template is written and when it is loaded.\n\n{@code dialect}, {@code apiVersion}, {@code parent}, and {@code editable} are searchable with\n{@code queryByDefault} false. A write uses {@code parent} to find its children; the other\nfields support template administration filters. {@code aspectJson} stores this aspect's JSON,\nthe same JSON stored in SQL, at\n{@code _aspects.searchTemplateProperties._aspectJson} without indexing it, so a template\nreload can read the body from search. Write time and actor stay on system metadata\n({@code aspectCreated}, {@code aspectModified}): SQL stores them on the aspect row, and search\ncopies them to {@code _system}. The entity stays out of the default search, browse, and\nautocomplete entity-type lists."
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
