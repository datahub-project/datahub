### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

### Limitations

#### Processing Limitations

- **Text Only:** Only processes `Document.text` field (markdown format expected)
- **No Binary Content:** Images, PDFs, etc. must be converted to text first
- **Markdown Partitioning:** Uses `unstructured.partition.md` which may not handle all markdown variants

#### Platform Filtering

- **Source Type Required:** Documents must have `sourceType` field (defaults to NATIVE if missing)
- **Platform Identification:** Relies on `dataPlatformInstance` or URL-based platform extraction

#### State Management

- **State Size:** State file grows with number of documents (includes hash for each)
- **State Backend:** Requires DataHub or file-based state provider

#### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Issue: "Semantic search is not enabled on the DataHub server"

**Cause:** Server does not have semantic search configured.

**Solution:**

1. Configure semantic search on your DataHub server first
2. See [Semantic Search Configuration Guide](/docs/how-to/semantic-search-configuration)
3. Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` in server config

#### Issue: "Server does not support semantic search configuration API"

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

#### Issue: Embedding Configuration Validation Fails

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

#### Issue: No Documents Being Processed

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

#### Issue: Event Mode Not Working

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

#### Issue: AWS Credentials Error

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

#### Issue: Slow Processing

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

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
