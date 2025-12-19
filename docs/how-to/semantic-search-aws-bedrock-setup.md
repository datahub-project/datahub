# Setting Up Semantic Search with AWS Bedrock in DataHub OSS

This guide walks you through configuring semantic search in DataHub OSS using AWS Bedrock for embedding generation.

## Overview

DataHub's semantic search uses vector embeddings to find semantically similar entities. In OSS, embeddings are generated using AWS Bedrock with Cohere Embed models. This provides:

- **Natural language search**: Find datasets using conversational queries like "customer churn analysis"
- **Semantic understanding**: Match concepts even when exact keywords differ
- **Cross-entity search**: Search across datasets, dashboards, and other entities simultaneously

## Prerequisites

### 1. OpenSearch Requirements

- **OpenSearch 2.17.0 or higher** with k-NN plugin enabled
- Alternative: Elasticsearch with k-NN plugin (not officially tested)

Verify k-NN plugin is enabled:

```bash
curl -X GET "localhost:9200/_cat/plugins?v&s=component&h=name,component,version"
```

You should see `opensearch-knn` in the output.

### 2. AWS Bedrock Requirements

#### AWS Account Setup

1. **AWS Account** with Bedrock access
2. **Supported AWS Region**: Bedrock with Cohere Embed v3 is available in:
   - `us-west-2` (Oregon) - Recommended
   - `us-east-1` (N. Virginia)
   - Other regions - check [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html)

#### Enable Model Access

1. Go to AWS Console → Amazon Bedrock → Model access
2. Request access to **Cohere Embed English v3** (`cohere.embed-english-v3`)
3. Wait for approval (usually instant for Cohere models)

#### IAM Permissions

Your AWS credentials (IAM user or role) need:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["bedrock:InvokeModel"],
      "Resource": [
        "arn:aws:bedrock:*::foundation-model/cohere.embed-english-v3",
        "arn:aws:bedrock:*::foundation-model/cohere.embed-multilingual-v3"
      ]
    }
  ]
}
```

For broader access (all Bedrock models):

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:*::foundation-model/*"
}
```

## Configuration

### Step 1: Configure DataHub

Edit your `application.yaml` or set environment variables:

#### Option A: Environment Variables (Recommended for Production)

```bash
# Enable semantic search
export ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
export ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document  # Comma-separated: dataset,dashboard,chart

# Configure embedding provider
export EMBEDDING_PROVIDER_TYPE=aws-bedrock
export EMBEDDING_PROVIDER_AWS_REGION=us-west-2
export EMBEDDING_PROVIDER_MODEL_ID=cohere.embed-english-v3
export EMBEDDING_PROVIDER_MAX_CHAR_LENGTH=2048

# Vector index configuration
export ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION=1024
export ELASTICSEARCH_SEMANTIC_KNN_ENGINE=faiss
export ELASTICSEARCH_SEMANTIC_SPACE_TYPE=cosinesimil
```

#### Option B: application.yaml

```yaml
elasticsearch:
  index:
    semanticSearch:
      enabled: true
      enabledEntities: document # Or: dataset,dashboard,chart
      models:
        cohere_embed_v3:
          vectorDimension: 1024
          knnEngine: faiss
          spaceType: cosinesimil
          efConstruction: 128
          m: 16
      embeddingProvider:
        type: aws-bedrock
        awsRegion: us-west-2
        modelId: cohere.embed-english-v3
        maxCharacterLength: 2048
```

### Step 2: Configure AWS Credentials

Choose one of these authentication methods:

#### Option 1: AWS Profile (Development)

Create/edit `~/.aws/credentials`:

```ini
[datahub-dev]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Then set the profile:

```bash
export AWS_PROFILE=datahub-dev
```

#### Option 2: Environment Variables (CI/CD)

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
export AWS_REGION=us-west-2  # Optional, uses config default if not set
```

#### Option 3: EC2 Instance Role (Production - Recommended)

For production deployments on EC2:

1. Create an IAM role with Bedrock permissions (see IAM Permissions above)
2. Attach the role to your EC2 instance
3. **No additional configuration needed** - credentials auto-discovered via IMDS

#### Option 4: ECS Task Role (Container Deployments)

For ECS/Fargate deployments:

1. Create an IAM role with Bedrock permissions
2. Assign the role to your ECS task definition
3. **No additional configuration needed** - credentials auto-discovered

### Step 3: Restart DataHub

```bash
# Docker Compose
docker-compose restart datahub-gms

# Kubernetes
kubectl rollout restart deployment datahub-gms
```

### Step 4: Verify Configuration

Check DataHub logs for successful initialization:

```bash
# Look for these log messages
docker-compose logs datahub-gms | grep -i "semantic\|embedding"
```

Expected output:

```
Creating embedding provider with type: aws-bedrock
Configuring AWS Bedrock embedding provider: region=us-west-2, model=cohere.embed-english-v3, maxCharLength=2048
Initialized AwsBedrockEmbeddingProvider with region=us-west-2, model=cohere.embed-english-v3, maxCharLength=2048
```

## Usage

### GraphQL API

#### Single Entity Type Search

```graphql
query {
  semanticSearch(
    input: {
      type: DATASET
      query: "customer churn prediction models"
      start: 0
      count: 10
    }
  ) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          description
        }
      }
    }
  }
}
```

#### Multi-Entity Search

```graphql
query {
  semanticSearchAcrossEntities(
    input: {
      types: [DATASET, DASHBOARD, CHART]
      query: "revenue analysis last quarter"
      start: 0
      count: 10
    }
  ) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}
```

### Python SDK (Coming Soon)

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter("http://localhost:8080")

# Semantic search
results = emitter.semantic_search(
    query="customer data pipeline",
    types=["dataset"],
    start=0,
    count=10
)
```

## Backfilling Embeddings

After enabling semantic search, you need to backfill embeddings for existing entities:

### Using DataHub Upgrade CLI

```bash
# Navigate to the upgrade tool
cd datahub-upgrade/build/install/datahub-upgrade

# Run the embeddings backfill
./bin/datahub-upgrade -u SearchEmbeddingsUpdate

# Monitor progress
docker-compose logs -f datahub-gms | grep -i "embedding"
```

### Configuration for Backfill

Environment variables for tuning backfill performance:

```bash
# Batch size for processing entities
export ELASTICSEARCH_EMBEDDINGS_UPDATE_BATCH_SIZE=100

# Maximum text length to embed per entity
export ELASTICSEARCH_EMBEDDINGS_UPDATE_MAX_TEXT_LENGTH=8000
```

## Supported Models

### Cohere Embed v3 (Default)

| Model ID                       | Dimensions | Max Tokens | Languages      |
| ------------------------------ | ---------- | ---------- | -------------- |
| `cohere.embed-english-v3`      | 1024       | 512        | English        |
| `cohere.embed-multilingual-v3` | 1024       | 512        | 100+ languages |

### Amazon Titan Embed

| Model ID                       | Dimensions          | Max Tokens | Languages |
| ------------------------------ | ------------------- | ---------- | --------- |
| `amazon.titan-embed-text-v1`   | 1536                | 8192       | English   |
| `amazon.titan-embed-text-v2:0` | 1024 (configurable) | 8192       | English   |

**Note**: If you change models, ensure `vectorDimension` matches the model's output dimensions.

## Troubleshooting

### Issue: "Semantic search is disabled or not configured"

**Solution**: Verify `ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true` and restart GMS.

### Issue: AWS Credentials Error

```
Unable to load credentials from any provider in the chain
```

**Solutions**:

1. Verify AWS_PROFILE is set and profile exists in `~/.aws/credentials`
2. For EC2, verify instance role is attached: `curl http://169.254.169.254/latest/meta-data/iam/security-credentials/`
3. Check environment variables are set correctly

### Issue: Bedrock Access Denied

```
User: arn:aws:iam::123456789:user/datahub is not authorized to perform: bedrock:InvokeModel
```

**Solution**: Add IAM permissions (see IAM Permissions section above).

### Issue: Model Not Found

```
Could not find model: cohere.embed-english-v3
```

**Solutions**:

1. Verify model access is enabled in AWS Console → Bedrock → Model access
2. Check the region supports the model (use `us-west-2` for broadest support)
3. Ensure `EMBEDDING_PROVIDER_AWS_REGION` matches where you enabled access

### Issue: k-NN Index Creation Failed

```
Codec [zstd_no_dict] cannot be used with k-NN indices
```

**Solution**: This is a known issue with older DataHub versions. The semantic search port includes a fix. Ensure you have the latest code.

### Issue: Vector Dimension Mismatch

```
Dimension mismatch: expected 1024, got 1536
```

**Solution**: Your model's dimensions don't match the configuration. Update `ELASTICSEARCH_SEMANTIC_VECTOR_DIMENSION` to match your model.

## Performance Tuning

### OpenSearch k-NN Settings

For better performance, tune these parameters in `application.yaml`:

```yaml
semanticSearch:
  models:
    cohere_embed_v3:
      efConstruction: 128 # Higher = better recall, slower indexing (default: 128)
      m: 16 # Higher = better recall, more memory (default: 16)
      spaceType: cosinesimil # cosinesimil, l2, innerproduct
      knnEngine: faiss # faiss, nmslib, lucene
```

**Recommendations**:

- **Small datasets (<10K docs)**: `efConstruction: 128, m: 16`
- **Medium datasets (10K-100K docs)**: `efConstruction: 256, m: 32`
- **Large datasets (>100K docs)**: `efConstruction: 512, m: 48`

### AWS Bedrock Rate Limits

Cohere Embed v3 on Bedrock has default limits:

- **Requests per minute**: 1000
- **Tokens per minute**: 200,000

For higher limits, request a quota increase in AWS Service Quotas.

## Cost Estimation

### AWS Bedrock Pricing (Cohere Embed v3)

As of December 2024 in us-west-2:

- **$0.0001 per 1,000 input tokens** (~750 words)

**Example costs**:

- 10,000 datasets with 200 tokens each = 2M tokens = **$0.20**
- 100,000 datasets with 200 tokens each = 20M tokens = **$2.00**
- Query embeddings: ~50 tokens per query = 10,000 queries = **$0.05**

**Monthly estimates** (assuming daily re-indexing):

- 10K entities: ~$6/month
- 100K entities: ~$60/month

Check current pricing: https://aws.amazon.com/bedrock/pricing/

## Security Best Practices

1. **Use IAM Roles**: Prefer EC2 instance roles over static credentials
2. **Principle of Least Privilege**: Grant only `bedrock:InvokeModel` permission
3. **Enable CloudTrail**: Monitor Bedrock API calls
4. **Resource Tags**: Tag IAM roles for cost tracking
5. **Rotate Credentials**: If using access keys, rotate regularly

## References

- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/)
- [Cohere Embed Models](https://docs.cohere.com/docs/embed-2)
- [OpenSearch k-NN Plugin](https://opensearch.org/docs/latest/search-plugins/knn/index/)
