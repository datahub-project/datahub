:::tip Quick Start: Auto-Deploy for Semantic Search

To enable automatic semantic search indexing for your documents, deploy this source to DataHub with a simple command:

```bash
# Create minimal recipe and deploy with hourly schedule
cat > /tmp/datahub-docs.yml << 'EOF'
source:
  type: datahub-documents
  config: {}
EOF

datahub ingest deploy -c /tmp/datahub-docs.yml --name "document-embeddings" --schedule "0 * * * *"
```

This creates a managed ingestion source in DataHub that automatically processes documents every hour and generates embeddings for semantic search.

**What this does:**

- ✅ Deploys ingestion recipe to DataHub
- ✅ Runs hourly (cron: `0 * * * *`) to keep embeddings up-to-date
- ✅ Uses event-driven mode (only processes changed documents)
- ✅ Auto-configures from server (no manual embedding setup needed)

**Alternative schedules:**

```bash
# Every 15 minutes: "*/15 * * * *"
# Every 6 hours:    "0 */6 * * *"
# Daily at 2 AM:    "0 2 * * *"
```

> **Note:** In future DataHub versions, GMS will run this automatically. For now, manual deployment is required.

:::

## Overview

The DataHub Documents source processes Document entities already stored in DataHub and enriches them with semantic embeddings for semantic search. This source is designed to work with DataHub's native Document entities that have been created via GraphQL, Python SDK, or other ingestion sources (like Notion, Confluence, etc.).

### Key Features

#### 1. Smart Defaults with Server Configuration Sync

The source **automatically fetches embedding configuration from your DataHub server**, ensuring perfect alignment:

- Connects using environment variables (`DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`)
- Fetches embedding provider config (provider, model, region, dimensions)
- Validates local config against server (if provided)
- Works with minimal `config: {}` in your recipe

#### 2. Dual Operating Modes

**Event-Driven Mode (Recommended):**

- Processes documents in real-time from Kafka MCL events
- Only reprocesses when document content changes
- Efficient for continuous updates
- Automatically falls back to batch mode on first run

**Batch Mode:**

- Fetches all documents via GraphQL
- Good for initial setup or periodic full refreshes
- Processes all matching documents

#### 3. Incremental Processing

- Tracks document content hashes to skip unchanged documents
- Uses DataHub's stateful ingestion framework
- Reduces processing time and API costs
- Force reprocess option available

#### 4. Flexible Platform Filtering

- Process all NATIVE documents (default)
- Filter by specific platforms (e.g., `["notion", "confluence"]`)
- Process all documents regardless of source type

## Prerequisites

### 1. DataHub Server Configuration

**You MUST configure semantic search on your DataHub server before using this source.**

See the [Semantic Search Configuration Guide](/docs/how-to/semantic-search-configuration) for complete setup instructions.

Required server configuration:

- OpenSearch 2.17+ with k-NN plugin enabled
- AWS Bedrock or Cohere embedding provider configured
- Semantic search enabled in `application.yml`

### 2. Document Entities in DataHub

This source processes existing Document entities. Documents can be created through:

- **GraphQL API**: Create documents programmatically
- **Python SDK**: Use `datahub.sdk.document.Document`
- **External Sources**: Notion, Confluence, SharePoint sources that emit Document entities

### 3. AWS Credentials (for Bedrock)

If using AWS Bedrock for embeddings:

- IAM permissions for `bedrock:InvokeModel`
- AWS credentials configured (env vars, instance role, or ECS task role)
- Bedrock model access enabled in AWS Console

### 4. Environment Variables

```bash
# Required for DataHub connection
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your-token-here"

# Optional: AWS credentials (if not using instance/task roles)
export AWS_PROFILE="datahub-dev"
# OR
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-west-2"
```

## Common Use Cases

### 1. Event-Driven Processing (Production)

Process documents in real-time as they're created or updated:

```yaml
source:
  type: datahub-documents
  config:
    # Event mode enabled by default in recent versions
    event_mode:
      enabled: true
      idle_timeout_seconds: 60

sink:
  type: datahub-rest
  config: {}
```

**When to use:**

- Production deployments
- Continuous document updates
- Real-time semantic search needs

### 2. Batch Processing (Initial Load)

Process all documents in a single run:

```yaml
source:
  type: datahub-documents
  config:
    event_mode:
      enabled: false

    # Optional: Process specific platforms
    platform_filter: ["notion", "confluence"]

sink:
  type: datahub-rest
  config: {}
```

**When to use:**

- Initial setup
- Periodic full refreshes
- Backfilling embeddings

### 3. Platform-Specific Processing

Process documents from specific platforms only:

```yaml
source:
  type: datahub-documents
  config:
    # Process NATIVE documents + EXTERNAL from these platforms
    platform_filter: ["notion", "confluence"]

    incremental:
      enabled: true

sink:
  type: datahub-rest
  config: {}
```

### 4. Force Reprocessing

Reprocess all documents regardless of content changes:

```yaml
source:
  type: datahub-documents
  config:
    incremental:
      enabled: true
      force_reprocess: true # Reprocess everything

    # Useful when:
    # - Changing chunking strategy
    # - Updating embedding model
    # - Fixing processing issues

sink:
  type: datahub-rest
  config: {}
```

### 5. Custom Chunking and Embedding

Override server configuration with local settings:

```yaml
source:
  type: datahub-documents
  config:
    # Custom chunking
    chunking:
      strategy: by_title # or 'basic'
      max_characters: 1000 # Larger chunks
      combine_text_under_n_chars: 200

    # Override embedding config (validates against server)
    embedding:
      provider: bedrock
      model: cohere.embed-english-v3
      model_embedding_key: cohere_embed_v3
      aws_region: us-west-2
      batch_size: 50

sink:
  type: datahub-rest
  config: {}
```

**⚠️ Warning:** Custom embedding configs are validated against the server. Mismatches will cause errors.

## How It Works

### Processing Pipeline

```
1. Fetch Mode Selection
   ├─ Event Mode: Subscribe to Kafka MCL events
   └─ Batch Mode: GraphQL query for all documents

2. For Each Document:
   ├─ Check incremental state (skip if unchanged)
   ├─ Partition markdown → structured elements
   ├─ Chunk elements → semantic chunks
   │   ├─ by_title: Preserves document structure
   │   └─ basic: Fixed-size chunks with overlap
   ├─ Generate embeddings via LiteLLM
   │   └─ Batches of 25 (configurable)
   └─ Emit SemanticContent aspect → DataHub

3. State Management
   ├─ Batch Mode: Track document content hashes
   └─ Event Mode: Track Kafka offsets
```

### Event Mode Flow

**First Run (No State):**

1. Falls back to batch mode
2. Captures current Kafka offset BEFORE processing
3. Processes all documents
4. Saves offset to state
5. Next run continues from captured offset

**Subsequent Runs:**

1. Loads last committed offset from state
2. Consumes events from last position
3. Processes only changed documents
4. Updates offset after each batch
5. Exits after idle timeout (no new events)

### Incremental Processing

**Content Hash Calculation:**

```python
hash_input = {
    "text": document.text,
    "partition_strategy": config.partition_strategy,
    "chunking_strategy": config.chunking.strategy,
    "max_characters": config.chunking.max_characters,
    # ... other chunking params
}
content_hash = sha256(json.dumps(hash_input))
```

**When Documents Are Reprocessed:**

- Text content changes
- Chunking configuration changes
- Partition strategy changes
- `force_reprocess: true` is set

## Configuration Deep Dive

### Platform Filtering

The `platform_filter` setting controls which documents are processed:

**`None` (default):**

```yaml
platform_filter: null # or omit the field
```

- Processes all NATIVE documents (sourceType=NATIVE)
- Ignores EXTERNAL documents from other platforms

**Specific Platforms:**

```yaml
platform_filter: ["notion", "confluence"]
```

- Processes NATIVE documents
- PLUS EXTERNAL documents from specified platforms

**All Documents:**

```yaml
platform_filter: ["*"] # or ["ALL"]
```

- Processes ALL documents regardless of source type or platform

### Event Mode Configuration

```yaml
event_mode:
  enabled: true

  # Consumer ID for offset tracking
  consumer_id: "datahub-documents-{pipeline_name}" # Default

  # Kafka topics to consume
  topics:
    - "MetadataChangeLog_Versioned_v1"

  # Lookback window for first run
  lookback_days: null # null = start from latest, or specify days

  # Reset offsets to beginning (DANGEROUS - reprocesses everything)
  reset_offsets: false

  # Exit after N seconds with no new events
  idle_timeout_seconds: 30

  # Kafka poll settings
  poll_timeout_seconds: 2
  poll_limit: 100
```

### Chunking Strategies

**by_title (Recommended):**

```yaml
chunking:
  strategy: by_title
  max_characters: 500
  combine_text_under_n_chars: 100
```

- Preserves document structure
- Groups text under section headers
- Combines small chunks intelligently
- Better semantic coherence

**basic:**

```yaml
chunking:
  strategy: basic
  max_characters: 500
  overlap: 50 # Character overlap between chunks
```

- Simple fixed-size chunks
- Configurable overlap
- No structure awareness

### Embedding Configuration

**Default (Fetch from Server):**

```yaml
embedding: {} # or omit entirely
```

- Automatically fetches config from server
- Ensures alignment with server's semantic search
- **Recommended for production**

**Override (Validated Against Server):**

```yaml
embedding:
  provider: bedrock # bedrock, cohere, openai
  model: cohere.embed-english-v3
  model_embedding_key: cohere_embed_v3 # Must match server!
  aws_region: us-west-2
  batch_size: 25
  input_type: search_document # Cohere-specific
```

- Validates that config matches server
- Fails if mismatch detected
- Prevents broken semantic search

**Break-Glass Override (NOT RECOMMENDED):**

```yaml
embedding:
  allow_local_embedding_config: true
  provider: bedrock
  model: cohere.embed-english-v3
  # ... other settings
```

- Bypasses server validation
- **May break semantic search**
- Only use for debugging or special cases

### Stateful Ingestion

```yaml
stateful_ingestion:
  enabled: true # Enabled by default

  # State backend configuration
  state_provider:
    type: datahub # Store state in DataHub
    config:
      datahub_api:
        server: "http://localhost:8080"
        token: "${DATAHUB_TOKEN}"

  # Ignore previous state (fresh start)
  ignore_old_state: false

  # Don't commit new state (dry run)
  ignore_new_state: false
```

## Performance Tuning

### Batch Size

```yaml
embedding:
  batch_size: 25 # Default
  # Increase for faster processing (if provider supports):
  # - Cohere: Up to 96
  # - Bedrock: Up to 100 (but rate-limited)
```

### Event Mode Settings

```yaml
event_mode:
  poll_limit: 100  # Fetch up to 100 events per poll
  # Increase for high-volume scenarios:
  poll_limit: 500  # Process more events per batch
```

### Filtering

```yaml
# Skip short or empty documents
skip_empty_text: true
min_text_length: 50 # Characters

# Process fewer documents
platform_filter: ["notion"] # Only one platform
document_urns: # Specific documents only
  - "urn:li:document:abc123"
```

## Monitoring and Observability

### Report Metrics

The source reports the following metrics:

```python
report = {
    "num_documents_fetched": 100,       # Total documents fetched
    "num_documents_processed": 85,      # Successfully processed
    "num_documents_skipped": 15,        # Skipped (various reasons)
    "num_documents_skipped_unchanged": 10,  # Unchanged content
    "num_documents_skipped_empty": 5,   # Empty or too short
    "num_chunks_created": 425,          # Total chunks generated
    "num_embeddings_generated": 425,    # Total embeddings
    "processing_errors": []             # List of errors
}
```

### Logging

Enable debug logging for detailed insights:

```yaml
# In your ingestion recipe
source:
  type: datahub-documents
  config:
    # ... your config
# Set log level via environment variable
# export DATAHUB_DEBUG=true
```

Look for these log messages:

- `"Loading embedding configuration from DataHub server..."`
- `"✓ Loaded embedding configuration from server"`
- `"Incremental mode enabled, state file: ..."`
- `"Skipping document {urn} (unchanged content hash)"`

## Troubleshooting

### Issue: "Semantic search is not enabled on the DataHub server"

**Cause:** Server does not have semantic search configured.

**Solution:**

1. Configure semantic search on your DataHub server first
2. See [Semantic Search Configuration Guide](/docs/how-to/semantic-search-configuration)
3. Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` in server config

### Issue: "Server does not support semantic search configuration API"

**Cause:** Old DataHub server version (pre-v0.14.0).

**Solutions:**

**Option 1 (Recommended):** Upgrade DataHub server to v0.14.0+

**Option 2:** Provide local embedding config:

```yaml
embedding:
  provider: bedrock
  model: cohere.embed-english-v3
  model_embedding_key: cohere_embed_v3
  aws_region: us-west-2
```

### Issue: Embedding Configuration Validation Fails

**Error:**

```
Embedding configuration mismatch with server:
- Model: local='cohere.embed-english-v3', server='amazon.titan-embed-text-v1'
```

**Cause:** Local config doesn't match server configuration.

**Solution:**

1. Either remove local embedding config (use server config)
2. Or update server config to match local settings
3. Or update local config to match server

### Issue: No Documents Being Processed

**Possible Causes:**

1. **Platform Filter Too Restrictive:**

   ```yaml
   # If you have NATIVE documents but filter for external platforms:
   platform_filter: ["notion"]  # Won't process NATIVE documents!

   # Solution: Remove filter or use null
   platform_filter: null
   ```

2. **All Documents Unchanged:**

   - Check incremental mode is working correctly
   - Force reprocess if needed: `incremental.force_reprocess: true`

3. **Documents Have No Text:**
   - Verify documents have content in `Document.text` field
   - Check `min_text_length` threshold

### Issue: Event Mode Not Working

**Symptoms:** Falls back to batch mode every run.

**Possible Causes:**

1. **Stateful Ingestion Disabled:**

   ```yaml
   stateful_ingestion:
     enabled: true # Must be enabled for event mode
   ```

2. **Kafka Connection Issues:**

   - Check DataHub Kafka is accessible
   - Verify network connectivity
   - Check Kafka broker configuration

3. **State Provider Misconfigured:**
   ```yaml
   stateful_ingestion:
     state_provider:
       type: datahub
       config:
         datahub_api:
           server: "http://correct-host:8080" # Correct URL
   ```

### Issue: AWS Credentials Error

**Error:**

```
Unable to load credentials from any provider in the chain
```

**Solutions:**

1. **Verify AWS_PROFILE:**

   ```bash
   export AWS_PROFILE=datahub-dev
   cat ~/.aws/credentials  # Check profile exists
   ```

2. **For EC2 Instance Role:**

   ```bash
   # Check instance role is attached
   curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
   ```

3. **For ECS Task Role:**
   - Verify task definition has correct IAM role
   - Check ECS task logs for IAM-related errors

### Issue: Slow Processing

**Optimization Strategies:**

1. **Increase Batch Size:**

   ```yaml
   embedding:
     batch_size: 50 # Up from default 25
   ```

2. **Use Event Mode:**

   - Only processes changed documents
   - Much faster than batch mode for updates

3. **Filter Documents:**

   ```yaml
   platform_filter: ["notion"] # Process fewer platforms
   min_text_length: 100 # Skip short documents
   ```

4. **Optimize Chunking:**
   ```yaml
   chunking:
     max_characters: 1000 # Larger chunks = fewer embeddings
   ```

## Cost Estimation

### AWS Bedrock Pricing (Cohere Embed v3)

As of December 2024 in us-west-2:

- **$0.0001 per 1,000 input tokens** (~750 words)

**Example Costs:**

**One-time Processing:**

- 1,000 documents × 500 tokens each = 500,000 tokens = **$0.05**
- 10,000 documents × 500 tokens each = 5M tokens = **$0.50**
- 100,000 documents × 500 tokens each = 50M tokens = **$5.00**

**Incremental Updates (Event Mode):**

- 100 changed documents/day × 500 tokens = 50,000 tokens/day
- Monthly: 1.5M tokens = **$0.15/month**

**Query Embeddings (GMS):**

- Separate from this source (handled by GMS at search time)
- ~50 tokens per search query
- 10,000 queries = **$0.05**

## Limitations

### Processing Limitations

- **Text Only:** Only processes `Document.text` field (markdown format expected)
- **No Binary Content:** Images, PDFs, etc. must be converted to text first
- **Markdown Partitioning:** Uses `unstructured.partition.md` which may not handle all markdown variants

### Platform Filtering

- **Source Type Required:** Documents must have `sourceType` field (defaults to NATIVE if missing)
- **Platform Identification:** Relies on `dataPlatformInstance` or URL-based platform extraction

### State Management

- **State Size:** State file grows with number of documents (includes hash for each)
- **State Backend:** Requires DataHub or file-based state provider

## Related Documentation

- [Semantic Search Configuration](/docs/how-to/semantic-search-configuration) - **Start Here**
- [Notion Source](/docs/generated/ingestion/sources/notion) - Example document source with embeddings
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html) - Embedding models
- [AWS Bedrock Pricing](https://aws.amazon.com/bedrock/pricing/) - Cost estimation
- [DataHub Developer Guides](https://docs.datahub.com/docs/developers) - Additional developer documentation
