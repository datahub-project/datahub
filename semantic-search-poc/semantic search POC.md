## **Overview**

This document outlines a proof of concept (POC) for adding semantic search capabilities to DataHub. The POC will be built as a **separate** application before integrating into the main DataHub codebase.

## **Objectives**

### **Primary Goals**

1. **Entity Transformation Optimization**: Determine what transformations are needed on DataHub entities (JSON format) to work effectively with embeddings
2. **Chunking Strategy Evaluation**: Assess whether chunking is necessary or if entire entities can be used to produce embeddings
3. **Graph Validation**: Test the search functionality across customer graphs to ensure it meets diverse needs

## **Architecture**

### **Components**

- **Input**: Entity graph passed as JSON
- **Vector Store**: Local OpenSearch instance
- **Operating Modes**:
  - **Population Mode**: Processes entities and populates OpenSearch with embeddings
  - **Search Mode**: Performs semantic search queries against the stored embeddings

## **Implementation Approach**

The POC will be implemented as a standalone application to avoid the complexity of modifying the existing DataHub codebase (tracking entity changes, updating legacy code, etc.) until the approach is validated.

## **Success Criteria**

- Identify optimal entity transformation strategies for embedding generation
- Determine appropriate chunking approach (if needed)
- Validate search quality across different customer graphs
- Establish a clear path for integration into the main DataHub application

# **DataHub Semantic Search Implementation Plan \- Risk-Optimized Approach**

## **Executive Summary**

This document outlines a risk-optimized approach to implementing semantic search in DataHub. Rather than building complex real-time infrastructure upfront, we focus on validating the core value proposition through offline experimentation before investing in production-grade integration.

## **Risk Analysis & Mitigation Strategy**

### **Core Project Risks**

Risk Category 1: Search Quality & User Value

- Will semantic search actually improve Slack bot results?
- What should go into semantic documents for optimal results?

Risk Category 2: Technical Integration Complexity

- How complex is real-time embedding computation and index updates?
- How do we handle the DataHub entity lifecycle accurately?

Risk Category 3: Content Strategy

- What metadata should be included/excluded in embeddings?
- How do we handle different entity types optimally?

Risk Category 4: Cost & Operational Efficiency

- What are the ongoing API and infrastructure costs?
- How do we optimize for cost efficiency at scale?

### **Risk Mitigation Through Project Slicing**

Phase 1 (Risk Reduction Focus): Offline Pipeline \+ Content Experimentation

- De-risks categories 1 & 3 through rapid experimentation
- Minimal infrastructure investment
- Fast iteration on content strategy

Phase 2 (Production Planning): Technical Integration & Cost Optimization

- Addresses categories 2 & 4 once value is proven
- Informed by Phase 1 learnings
- Right-sized for validated use cases

## **Phase 1: Offline Validation & Content Optimization**

Timeline: 4-6 weeks | Focus: Risk Categories 1 & 3

### **1.1 Minimal Infrastructure Setup**

Goal: Create experimentation environment with minimal complexity

#### **Quick Index Extension**

- Add semantic_embedding field to existing Elasticsearch indexes
- No real-time updates \- populate via batch scripts only
- Use existing index infrastructure (no new clusters)
- Add simple vector search endpoint to DataHub API

#### **Offline Embedding Pipeline**

python

```py
# Simple batch script approach
def generate_embeddings_offline():
    entities = fetch_all_entities_via_graphql()
    for entity in entities:
        embedding_text = transform_entity(entity)  # Experiment with different strategies
        embedding = call_openai_api(embedding_text)
        store_in_elasticsearch(entity.urn, embedding)
```

### **1.2 Content Strategy Experimentation**

Goal: Determine optimal semantic document composition

#### **Experimental Frameworks**

Create multiple semantic document variants for A/B testing:  
Variant A \- Minimal: Name \+ Description only  
python

```py
def transform_minimal(entity):
    return f"{entity.name}: {entity.description}"
```

Variant B \- Metadata Rich: Full context  
python

```py
def transform_rich(entity):
    parts = [
        f"Name: {entity.name}",
        f"Description: {entity.description}",
        f"Platform: {entity.platform}",
        f"Tags: {', '.join(entity.tags)}",
        f"Owners: {', '.join(entity.owners)}",
        f"Domain: {entity.domain}",
        f"Environment: {entity.environment}"
    ]
    return " | ".join(filter(None, parts))
```

Variant C \- Schema Focused: For datasets  
python

```py
def transform_schema_focused(entity):
    schema_info = []
    for field in entity.schema_fields:
        schema_info.append(f"{field.name}: {field.description}")

    return f"{entity.name} - {entity.description}\nFields: {'; '.join(schema_info)}"
```

Variant D \- Usage Context: Including popularity/lineage  
python

```py
def transform_usage_context(entity):
    usage = f"Used by {len(entity.downstream)} datasets" if entity.downstream else "New dataset"
    return f"{entity.name}: {entity.description} | {usage} | Tags: {', '.join(entity.tags)}"
```

#### **What NOT to Include (Initial Hypothesis)**

- Raw query text: Too noisy, platform-specific
- Technical identifiers: URNs, GUIDs don't help semantic matching
- Timestamps: Dates add noise to semantic similarity
- Detailed lineage: Too complex for initial matching
- Usage statistics numbers: Specific metrics less useful than relative context

### **1.3 Slack Bot Integration (Offline Mode)**

Goal: Validate user experience improvements with real queries

#### **Simple Integration Approach**

- Route natural language Slack queries to new semantic search endpoint
- Compare results side-by-side with existing keyword search
- Collect user feedback ("Was this helpful? Better than before?")
- No real-time embedding updates \- work with offline-computed embeddings

#### **Evaluation Framework**

python

```py
class SearchComparison:
    def evaluate_query(self, user_query):
        keyword_results = existing_search(user_query)
        semantic_results = semantic_search(user_query)

        return {
            'query': user_query,
            'keyword_results': keyword_results[:5],
            'semantic_results': semantic_results[:5],
            'user_preference': None  # Filled by user feedback
        }
```

### **1.4 Content Strategy Validation**

Goal: Determine optimal transformation strategy through data

#### **Automated Quality Metrics**

- Semantic clustering: Do similar entities cluster together?
- Query-result relevance: Manual evaluation of top search results
- Coverage analysis: What percentage of entities get good embeddings?

#### **User-Driven Validation**

- A/B test different content strategies with real Slack users
- Track click-through rates and user satisfaction
- Collect qualitative feedback on result quality

Phase 1 Deliverables:

- Working offline embedding pipeline with multiple content strategies
- Slack bot integration with semantic search (offline embeddings)
- Data-driven recommendation for optimal content strategy
- User validation of search quality improvements
- Clear go/no-go decision for Phase 2

## **Phase 2: Production Integration & Cost Optimization**

Timeline: 6-8 weeks | Focus: Risk Categories 2 & 4 _Only execute if Phase 1 validates significant user value_

### **2.1 Real-Time Integration Architecture**

Goal: Build production-grade embedding updates based on Phase 1 learnings

#### **DataHub Lifecycle Integration**

Based on Phase 1 content strategy, implement:

- Entity change detection and embedding regeneration
- Incremental updates for modified entities
- Batch reprocessing for content strategy changes

#### **Smart Update Strategy**

python

```py
class EmbeddingUpdateManager:
    def should_regenerate_embedding(self, entity_change):
        # Only regenerate if semantically relevant fields changed
        relevant_fields = ['name', 'description', 'tags', 'schema']  # From Phase 1
        return any(field in entity_change.modified_fields for field in relevant_fields)
```

### **2.2 Cost Optimization**

Goal: Minimize operational costs based on actual usage patterns

#### **Cost Reduction Strategies**

- Embedding caching: Cache embeddings for identical content
- Batch processing: Group API calls to reduce per-request costs
- Selective embedding: Only embed entities that users actually search for
- Model optimization: Use smaller/cheaper models if quality is sufficient

#### **Cost Monitoring**

python

```py
class CostTracker:
    def track_embedding_cost(self, entity_count, model_used, tokens_processed):
        cost = calculate_api_cost(tokens_processed, model_used)
        self.log_cost(date=today(), cost=cost, entities=entity_count)

    def recommend_optimizations(self):
        # Analyze usage patterns and suggest cost reductions
        pass
```

### **2.3 Production Monitoring**

Goal: Ensure reliable operation with cost visibility

#### **Key Metrics**

- Embedding generation success rate: \>99%
- Search response time: \<500ms 95th percentile
- Daily embedding costs: Track and alert on budget
- User satisfaction: Ongoing feedback collection

Phase 2 Deliverables:

- Production-ready real-time embedding updates
- Cost-optimized operational model
- Comprehensive monitoring and alerting
- Full Slack bot integration with real-time updates

## **Simplified Technical Architecture**

### **Phase 1 Architecture (Offline-First)**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Batch Script  │    │    OpenAI API    │    │ Elasticsearch   │
│                 ├────┤                  ├────┤                 │
│ (Content Exp.)  │    │   (Embeddings)   │    │ (Vector Index)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
          │                                              │
          │            ┌──────────────────┐             │
          └────────────┤  DataHub GraphQL ├─────────────┘
                       │                  │
                       │   (Entity Data)  │
                       └──────────────────┘
```

### **Phase 2 Architecture (Production)**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ DataHub Events  │    │  Embedding Svc   │    │ Elasticsearch   │
│                 ├────┤                  ├────┤                 │
│ (Real-time)     │    │ (Cache + API)    │    │ (Vector Index)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## **Resource Requirements**

### **Phase 1 (Minimal Investment)**

- 1 Backend Engineer (3-4 weeks): Offline pipeline \+ search endpoint
- Infrastructure: \~$200-500/month (embedding API calls only)
- Risk: Low \- can abandon if results poor

### **Phase 2 (Production Investment)**

- 1 Backend Engineer (6-8 weeks): Real-time integration
- 0.5 DevOps Engineer (2-3 weeks): Monitoring and deployment
- Infrastructure: \~$800-2000/month (full operational costs)
- Risk: Medium \- justified by Phase 1 validation

## **Decision Framework**

### **Phase 1 Success Criteria (Go/No-Go for Phase 2\)**

1. User Preference: \>60% of users prefer semantic results to keyword results
2. Search Success Rate: \>75% of semantic queries return relevant top-3 results
3. Content Strategy Clarity: Clear winner among transformation approaches
4. Technical Feasibility: Offline pipeline runs reliably with acceptable costs

### **Phase 1 Failure Modes (Stop Here)**

- Users don't prefer semantic results over keyword search
- No transformation strategy produces good results
- Embedding costs exceed budget constraints
- Technical integration proves more complex than expected

## **Expected Outcomes**

### **Phase 1 Outcomes (4-6 weeks)**

- Clear data on whether semantic search improves user experience
- Validated content strategy for embedding generation
- Cost model based on actual usage
- Technical proof that integration is feasible

### **Phase 2 Outcomes (6-8 weeks, if Phase 1 succeeds)**

- Production-ready semantic search integrated into DataHub
- Cost-optimized operations with monitoring and alerting
- Enhanced Slack bot with real-time embedding updates
- Framework for future AI features built on semantic infrastructure

This approach minimizes risk by validating the core value proposition before investing in complex production infrastructure, while still providing a clear path to full implementation once value is proven.
