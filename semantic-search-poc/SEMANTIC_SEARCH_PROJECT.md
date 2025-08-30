# DataHub Semantic Search Project

## 🎯 **Project Overview**

This project enhances DataHub's search capabilities by adding semantic search functionality to the existing OpenSearch infrastructure. The goal is to enable natural language queries that can intelligently find relevant data assets based on meaning rather than just keyword matching.

## 📊 **Dataset Overview: Long Tail Companions**

### **Business Domain**

Long Tail Companions is a pet adoption platform that connects potential pet owners with animals in need of homes. The dataset represents a realistic enterprise data ecosystem spanning multiple platforms and business functions.

### **Data Architecture**

#### **Scale & Complexity**

- **21,522 metadata records** across 5 entity types
- **2,063 datasets** with rich schema and lineage information
- **226 charts** and **45 dashboards** for business intelligence
- **105 users** with governance roles and permissions
- **952 lineage relationships** showing data flow dependencies

#### **Platform Ecosystem**

| Platform       | Usage             | Role                     |
| -------------- | ----------------- | ------------------------ |
| **Snowflake**  | 38,467 references | Primary data warehouse   |
| **Looker**     | 11,373 references | BI & visualization layer |
| **dbt**        | 3,355 references  | Data transformation      |
| **Databricks** | 881 references    | Advanced analytics       |
| **PostgreSQL** | 22 references     | Operational databases    |

#### **Database Structure**

```
long_tail_companions/
├── analytics/                    # Business intelligence data
│   ├── visionary_etailers       # Customer analytics
│   ├── b2c_portals              # Platform usage metrics
│   ├── opensource_interfaces     # API usage data
│   ├── daily_platforms          # Daily operational metrics
│   ├── customer_last_purchase_date # Purchase behavior
│   └── crossplatform_users_old   # User segmentation
├── adoption_events/              # Pet adoption tracking
│   └── event_contact_details    # Adoption event records
├── marketing/                   # Marketing campaigns
│   └── collaboration_outreach   # Outreach campaign data
└── view/                       # Looker business views
    ├── missioncritical_vortals  # Critical business metrics
    ├── versatile               # Multi-purpose views
    ├── ameliorated            # Improved data views
    └── users                  # User-focused analytics
```

#### **Metadata Richness**

- **Schema Metadata**: Column definitions, data types, constraints (2,061 records)
- **Data Lineage**: Upstream/downstream relationships (952 connections)
- **Business Glossary**: Terminology and definitions (769 terms)
- **Data Governance**: Ownership, domains, tags (364 ownership assignments)
- **Data Quality**: Test results and monitoring (2,328 test records)

## 👥 **User Personas & Natural Language Questions**

### 🔬 **Data Scientists**

#### **Data Discovery & Quality**

- "What datasets contain customer behavior data for pet adoptions?"
- "Which tables have the most recent pet adoption events?"
- "Are there any data quality issues with the customer purchase data?"
- "What's the schema of the pet profiles dataset?"
- "Which datasets contain demographic information about pet adopters?"
- "Show me all datasets with customer lifetime value calculations"

#### **Analysis & Modeling**

- "What data sources feed into the customer segmentation models?"
- "Which tables contain features for predicting adoption success?"
- "Are there any datasets with pet health or temperament data?"
- "What's the lineage of the customer_last_purchase_date table?"
- "Which datasets have time-series data for forecasting models?"
- "Show me tables with A/B test results for recommendation algorithms"

#### **Technical Dependencies**

- "Which dbt models transform the raw adoption event data?"
- "What happens if the pet_profiles table schema changes?"
- "Which downstream dashboards would break if I modify the analytics.daily_platforms table?"
- "What datasets depend on the Salesforce integration?"

### 📊 **Product Managers**

#### **Business Metrics & KPIs**

- "What dashboards show our key adoption metrics?"
- "Which datasets track user engagement with our platform?"
- "Where can I find conversion rates from browsing to adoption?"
- "What data shows the effectiveness of our matching algorithm?"
- "Which tables contain A/B test results for new features?"
- "Show me datasets tracking customer satisfaction scores"

#### **User Experience & Behavior**

- "What datasets show user journey through the adoption process?"
- "Which tables track abandoned adoption applications?"
- "Where is data about user preferences and search behavior?"
- "What datasets show mobile vs web usage patterns?"
- "Which data sources contain user feedback and ratings?"
- "Show me data about feature adoption and usage patterns"

#### **Strategic Planning**

- "What data exists about seasonal adoption trends?"
- "Which datasets help us understand our most successful adoption centers?"
- "Where can I find data about our competitive positioning?"
- "What tables contain information about partnership performance?"
- "Show me data for market expansion analysis"

### 📈 **Marketing Professionals**

#### **Campaign Performance**

- "Which dashboards show marketing campaign effectiveness?"
- "What datasets contain email marketing engagement metrics?"
- "Where can I find social media campaign performance data?"
- "Which tables track the customer acquisition cost by channel?"
- "What data shows the ROI of our Google Ads campaigns?"
- "Show me attribution data for multi-touch campaigns"

#### **Customer Segmentation**

- "What datasets help identify our highest-value customer segments?"
- "Which tables contain demographic data for targeting?"
- "Where is data about customer preferences for different pet types?"
- "What datasets show geographic distribution of our customers?"
- "Which tables contain information about repeat adopters?"
- "Show me data for lookalike audience modeling"

#### **Content & Messaging**

- "What data shows which pet photos get the most engagement?"
- "Which datasets contain information about successful adoption stories?"
- "Where can I find data about what messaging resonates with different segments?"
- "What tables track the performance of our blog content?"
- "Which datasets show seasonal trends in pet preferences?"
- "Show me data about content personalization effectiveness"

### 🔄 **Cross-Functional Questions**

#### **Operational Insights**

- "What datasets show our busiest adoption periods?"
- "Which tables contain information about shelter capacity and utilization?"
- "Where is data about staff productivity and adoption success rates?"
- "What datasets track the time from listing to adoption?"
- "Show me data about operational efficiency metrics"

#### **Compliance & Governance**

- "Which datasets contain personally identifiable information?"
- "What data needs to be retained for regulatory compliance?"
- "Which tables contain sensitive information about adopters?"
- "Where is the audit trail for changes to pet adoption records?"
- "Show me datasets that need GDPR compliance review"

## 🎯 **Technical Approach**

### **Semantic Search Architecture**

- **Hybrid Search**: Combine semantic similarity with traditional keyword search
- **Embedding Integration**: Add OpenAI text-embedding-3-large vectors to existing OpenSearch indices
- **Filter Preservation**: Maintain all existing DataHub filtering capabilities
- **Multi-Model Support**: Architecture supports multiple embedding models in parallel

### **Searchable Indices Coverage**

Based on DataHub's `SEARCHABLE_ENTITY_TYPES`, semantic search will include **all 23 entity types** that are currently searchable:

| Index                       | Entity Type          | Current Count | Include in Semantic Search |
| --------------------------- | -------------------- | ------------- | -------------------------- |
| `datasetindex_v2`           | `DATASET`            | 2,063         | ✅                         |
| `chartindex_v2`             | `CHART`              | 226           | ✅                         |
| `dashboardindex_v2`         | `DASHBOARD`          | 45            | ✅                         |
| `corpuserindex_v2`          | `CORP_USER`          | 106           | ✅                         |
| `domainindex_v2`            | `DOMAIN`             | 3             | ✅                         |
| `tagindex_v2`               | `TAG`                | 7             | ✅                         |
| `datajobindex_v2`           | `DATA_JOB`           | 0             | ✅ (future-ready)          |
| `dataflowindex_v2`          | `DATA_FLOW`          | 0             | ✅ (future-ready)          |
| `mlmodelindex_v2`           | `MLMODEL`            | 0             | ✅ (future-ready)          |
| `mlmodelgroupindex_v2`      | `MLMODEL_GROUP`      | 0             | ✅ (future-ready)          |
| `mlfeaturetableindex_v2`    | `MLFEATURE_TABLE`    | 0             | ✅ (future-ready)          |
| `mlfeatureindex_v2`         | `MLFEATURE`          | 0             | ✅ (future-ready)          |
| `mlprimarykeyindex_v2`      | `MLPRIMARY_KEY`      | 0             | ✅ (future-ready)          |
| `glossarytermindex_v2`      | `GLOSSARY_TERM`      | 0             | ✅ (future-ready)          |
| `glossarynodeindex_v2`      | `GLOSSARY_NODE`      | 0             | ✅ (future-ready)          |
| `roleindex_v2`              | `ROLE`               | 0             | ✅ (future-ready)          |
| `corpgroupindex_v2`         | `CORP_GROUP`         | 0             | ✅ (future-ready)          |
| `containerindex_v2`         | `CONTAINER`          | 0             | ✅ (future-ready)          |
| `dataproductindex_v2`       | `DATA_PRODUCT`       | 0             | ✅ (future-ready)          |
| `notebookindex_v2`          | `NOTEBOOK`           | 0             | ✅ (future-ready)          |
| `businessattributeindex_v2` | `BUSINESS_ATTRIBUTE` | 0             | ✅ (future-ready)          |
| `schemafieldindex_v2`       | `SCHEMA_FIELD`       | 0             | ✅ (future-ready)          |
| `applicationindex_v2`       | `APPLICATION`        | 0             | ✅ (future-ready)          |

**Total Coverage**: 2,456 entities across 23 indices (with room for future growth)

### **Embedding Strategy**

**Primary Approach: Combined Field Embeddings with Natural Language Generation**

#### **Text Generation for Embeddings**

Since embeddings work best with natural language rather than structured concatenations, we will generate natural descriptions for each entity:

##### Facet Filtering vs Embedding Text

Not everything should be included in the generated text used for embeddings. We pre-filter candidates via OpenSearch facets and reserve the text for semantic intent.

- **Use as filters (keep out of embedding text)**:

  - **owner/owners**, **domain**, **platform**, **origin/environment**, **entity type**, **tags**, **glossary terms** (as facets), **source location** (catalog/database, schema, project), **contains_pii**, **business_critical**, **model_maturity**, **materialization**, **language**, **dataset type/format**, **created/updated time** (for recency)

- **Keep in embedding text (semantic intent)**:

  - **natural description** (purpose and what it’s about)
  - **usage context** when evidenced (analytics, segmentation, reporting)
  - **domain vocabulary** in human-readable form (not URNs)
  - **a few representative field roles** (identifier, metric, date) rather than full lists
  - **data grain/time coverage** if present

- **Exclude from embeddings**:

  - **IDs/UUIDs/URNs**, owner emails, account/project IDs, full `qualifiedName` paths
  - platform/env boilerplate, long field enumerations, system timestamps/flags

- **Query understanding & execution**:

  - An LLM decomposes the user query into structured **filters** + concise **semantic text**
  - Apply filters in OpenSearch to trim the candidate set
  - Run dense vector search over the filtered set (optional hybrid BM25 + cosine re-ranking)
  - Relax filters progressively if no results

- **Glossary terms**:
  - Prefer as filter facets; optionally include human-readable terms in text when they add real semantic signal (avoid URNs).

**Template-Based Natural Language (MVP Approach)**

- Generate readable, contextual descriptions using smart templates
- Fast processing suitable for 20K+ entities
- Predictable quality and immediate implementation

**Example Transformation**:

- **Raw Data**:

  - Name: `pet_status_history`
  - Description: `Incremental table containing all historical statuses of a pet`
  - Fields: `[profile_id, status, as_of_date]`
  - Domain: `Pet Adoptions`
  - Tags: `[business_critical, prod_model]`

- **Generated Natural Description**:
  > "Incremental table containing all historical statuses of a pet. Also known as pet status history. Contains fields such as profile_id, status, as_of_date. Part of the Pet Adoptions domain. Tagged as business critical, prod model."

#### **Evolution Path**

1. **Phase 1 (MVP)**: Template-based generation for all entities
2. **Phase 2**: Hybrid approach - LLM-generated descriptions for high-value entities
3. **Phase 3**: Full LLM coverage with caching for optimal quality

**Example LLM-Enhanced Description**:

> "The pet_status_history table tracks the complete lifecycle of pets in the adoption system, maintaining an incremental record of all status changes over time. This business-critical dataset includes profile identifiers, current status, and temporal information, making it essential for analyzing adoption patterns and pet journey analytics within the Pet Adoptions domain."

#### **Technical Details**

- **Embedding Model**: OpenAI text-embedding-3-large (3072 dimensions)
- **Storage**: Add `text_embedding_3_large` field to each existing OpenSearch index
- **Content**: Natural language descriptions combining name, description, schema, domain, and tags

**Benefits of Natural Language Approach**:

- ✅ Optimized for embedding models trained on natural text
- ✅ Better semantic understanding and similarity matching
- ✅ More intuitive search results for natural language queries
- ✅ Maintains all benefits of combined field approach

**Future Enhancement Opportunity**:

- **High-Value Individual Fields**: Later add separate embeddings for critical fields (e.g., `name_embedding_3_large`) when precision matching is needed
- **Contextual Embeddings**: Include lineage and usage patterns in descriptions for richer context

### **Embedding Storage Schema**

#### **Document Structure**

Each entity in OpenSearch indices will include an `embeddings` nested object supporting multiple models and chunking:

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)",
  "name": "pet_status_history",
  "description": "Incremental table containing all historical statuses of a pet",
  // ... other existing fields ...
  "embeddings": {
    "text_embedding_3_large": {
      "chunks": [
        {
          "vector": [0.123, -0.456, ...],  // 3072 dimensions
          "text": "Incremental table containing all historical statuses of a pet. Also known as pet status history. Contains fields such as profile_id, status, as_of_date. Part of the Pet Adoptions domain. Tagged as business critical, prod model.",
          "position": 0,
          "token_count": 58
        }
        // Additional chunks added only if content exceeds 8K tokens
      ],
      "total_chunks": 1,
      "total_tokens": 58,
      "model_version": "2024-01",
      "generated_at": "2024-01-15T10:30:00Z",
      "chunking_strategy": "adaptive_8k_overlap_500"
    }
    // Future: Can add text_embedding_3_small or other models here
  }
}
```

#### **OpenSearch Mapping**

```json
{
  "embeddings": {
    "type": "object",
    "properties": {
      "text_embedding_3_large": {
        "type": "object",
        "properties": {
          "chunks": {
            "type": "nested",
            "properties": {
              "vector": {
                "type": "dense_vector",
                "dims": 3072,
                "index": true,
                "similarity": "cosine"
              },
              "text": {
                "type": "text",
                "index": false
              },
              "position": {
                "type": "integer"
              },
              "token_count": {
                "type": "integer"
              }
            }
          },
          "total_chunks": {
            "type": "integer"
          },
          "total_tokens": {
            "type": "integer"
          },
          "model_version": {
            "type": "keyword"
          },
          "generated_at": {
            "type": "date"
          },
          "chunking_strategy": {
            "type": "keyword"
          }
        }
      }
    }
  }
}
```

#### **Sample Semantic Search Query**

Hybrid search combining semantic similarity with traditional filters:

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "embeddings.text_embedding_3_large.chunks",
            "query": {
              "knn": {
                "embeddings.text_embedding_3_large.chunks.vector": {
                  "vector": [0.234, -0.567, ...],  // Query embedding
                  "k": 50
                }
              }
            },
            "score_mode": "max"  // Use highest scoring chunk
          }
        }
      ],
      "filter": [
        {
          "term": {
            "platform": "snowflake"  // Traditional filter
          }
        },
        {
          "terms": {
            "tags": ["business_critical"]  // Tag filter
          }
        }
      ]
    }
  },
  "size": 10,
  "_source": ["urn", "name", "description", "platform", "tags"]
}
```

#### **Key Design Decisions**

- **Chunks array**: Always present, even for short content (single chunk)
- **Nested type**: Enables efficient chunk-level vector search
- **Metadata preservation**: Track model version, generation time, and chunking strategy
- **Multi-model ready**: Structure supports multiple embedding models in parallel
- **Adaptive chunking**: Single chunk for content < 8K tokens, multiple overlapping chunks for longer content

### **Implementation Strategy**

1. **Phase 1**: Add embedding schema to existing OpenSearch indices with chunk support
2. **Phase 2**: Implement hybrid query processing (semantic + keyword + filters)
3. **Phase 3**: Advanced features (contextual search, feedback loops, field-specific embeddings)

### **Expected Benefits**

- **Improved Discoverability**: Users can find relevant data using natural language
- **Reduced Time-to-Insight**: Faster discovery of relevant datasets and dashboards
- **Better User Experience**: Intuitive search that understands intent, not just keywords
- **Enhanced Data Governance**: Better visibility into data relationships and usage

## 📈 **Success Metrics**

1. **Search Relevance**: Semantic queries return more relevant results than keyword-only search
2. **User Adoption**: Increased usage of search functionality across all personas
3. **Query Success Rate**: Higher percentage of searches that lead to user engagement with results
4. **Time-to-Discovery**: Reduced time from query to finding relevant data assets
5. **Coverage**: Ability to handle 90%+ of the natural language questions identified above

## 🚀 **Next Steps**

1. **Proof of Concept**: Implement embedding generation for a subset of datasets
2. **Query Processing**: Develop hybrid search query logic
3. **User Testing**: Validate search quality with real user scenarios
4. **Production Rollout**: Deploy to production DataHub environment
5. **Monitoring & Optimization**: Track usage patterns and continuously improve relevance

---

_This project represents a significant enhancement to DataHub's search capabilities, enabling intuitive data discovery through natural language queries while preserving all existing functionality._
