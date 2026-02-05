# Documentation Generation Evaluation

This directory contains tools for evaluating the quality and performance of AI-generated dataset documentation in DataHub.

## Overview

The [generate_docs_eval.py](generate_docs_eval.py) script evaluates DataHub's AI-powered documentation generation by:

1. Fetching dataset URNs from a DataHub instance
2. Generating table and column descriptions using the configured AI model
3. Measuring performance metrics (latency, success rate, description length)
4. Optionally comparing different AI models side-by-side
5. Optionally using Claude Opus 4.5 to evaluate description quality

## Features

- **Baseline Evaluation**: Run description generation with the default configured model
- **Model Comparison**: Compare two different models side-by-side (e.g., Sonnet vs Haiku)
- **Existing vs New Comparison**: Compare current production descriptions against a new model
- **Quality Analysis**: Use Claude Opus 4.5 via AWS Bedrock to judge which model produces better descriptions
- **Concurrent Processing**: Process multiple datasets in parallel for improved performance
- **CSV Export**: Export results with side-by-side comparison for analysis
- **Resume Capability**: Load existing CSV results and add quality analysis without regenerating descriptions

## Files

- **[generate_docs_eval.py](generate_docs_eval.py)**: Main evaluation script
- **[quality_comparison.py](quality_comparison.py)**: Quality comparison module using Claude Opus 4.5
- **README.md**: This documentation

## Requirements

- Python 3.x
- DataHub Python SDK
- boto3 (for AWS Bedrock quality comparisons)
- Access to a DataHub instance
- AWS credentials (only required for quality comparison)

## Setup

### DataHub Connection

Set environment variables to connect to your DataHub instance:

```bash
export DATAHUB_GMS_URL=http://localhost:8080
export DATAHUB_GMS_TOKEN=your_token_here
```

Alternatively, configure `~/.datahubenv` for automatic connection.

### AWS Credentials (Optional)

Only required when using `--compare-quality`:

```bash
export AWS_PROFILE=your-profile
# OR
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

## Usage Modes

The script supports three main modes of operation:

### Mode 1: Generate New Results (Default)

Runs baseline and optional eval model to generate new descriptions.

```bash
python generate_docs_eval.py --limit 100 --output results.csv
```

### Mode 2: Use Existing Descriptions as Baseline

Fetches current descriptions from DataHub entities and compares them against a new model. Useful for evaluating whether a new model improves upon existing production descriptions.

```bash
python generate_docs_eval.py \
  --limit 50 \
  --use-existing-as-baseline \
  --eval-model bedrock/anthropic.claude-sonnet-4-5-v1:0 \
  --output new_vs_existing.csv
```

### Mode 3: Quality Comparison Only

Loads existing CSV results and adds quality analysis without regenerating descriptions.

```bash
python generate_docs_eval.py \
  --input-csv existing_comparison.csv \
  --compare-quality \
  --output comparison_with_quality.csv
```

## Usage Examples

### Basic Usage: Baseline Evaluation

Generate descriptions for 100 datasets using the default model:

```bash
python generate_docs_eval.py --limit 100 --output results.csv
```

### Compare Two AI Models

Compare two different models (e.g., Sonnet vs Haiku):

```bash
python generate_docs_eval.py \
  --limit 50 \
  --eval-model bedrock/anthropic.claude-haiku-3-5-v2:0 \
  --output comparison.csv \
  --concurrency 8
```

This runs generation twice:

1. **Baseline**: Uses the default model from configuration
2. **Eval**: Uses the specified `--eval-model`

### Compare New Model vs Existing Production Descriptions

Test whether a new model produces better descriptions than what's currently in DataHub:

```bash
python generate_docs_eval.py \
  --limit 50 \
  --use-existing-as-baseline \
  --eval-model bedrock/anthropic.claude-sonnet-4-5-v1:0 \
  --output new_vs_existing.csv
```

This fetches existing descriptions from DataHub as baseline and generates new descriptions with the eval model.

### Full Comparison with Quality Analysis

Compare models and use Claude Opus 4.5 to judge quality:

```bash
export AWS_PROFILE=your-profile
python generate_docs_eval.py \
  --limit 50 \
  --eval-model bedrock/anthropic.claude-haiku-3-5-v2:0 \
  --compare-quality \
  --bedrock-region us-west-2 \
  --output full_comparison.csv
```

### Quality Analysis on Existing Results

Run quality comparison without regenerating descriptions:

```bash
export AWS_PROFILE=your-profile
python generate_docs_eval.py \
  --input-csv existing_comparison.csv \
  --compare-quality \
  --output comparison_with_quality.csv
```

## Command-Line Options

| Option                       | Description                                                 | Default            |
| ---------------------------- | ----------------------------------------------------------- | ------------------ |
| `--limit`                    | Maximum number of datasets to fetch                         | 100                |
| `--output`                   | Output CSV file path                                        | None (stdout only) |
| `--eval-model`               | Run second evaluation with this model                       | None               |
| `--use-existing-as-baseline` | Fetch existing entity descriptions from DataHub as baseline | False              |
| `--concurrency`              | Concurrent description generation requests                  | 8                  |
| `--compare-quality`          | Run quality comparison using Opus 4.5                       | False              |
| `--bedrock-region`           | AWS region for Bedrock API                                  | us-west-2          |
| `--quality-concurrency`      | Concurrent quality comparison requests                      | 5                  |
| `--input-csv`                | Load existing results for quality analysis                  | None               |

### Option Constraints

- `--compare-quality` requires either `--eval-model` or `--input-csv`
- `--input-csv` requires `--compare-quality` to be set
- `--input-csv` and `--eval-model` are mutually exclusive (CSV already has both runs)
- `--use-existing-as-baseline` requires `--eval-model` to be set

## Model Format

When specifying `--eval-model`, use the format expected by your LLM provider:

- **AWS Bedrock**: `bedrock/anthropic.claude-sonnet-4-0-v1:0`
- **OpenAI**: `openai/gpt-4-turbo`
- Other providers as supported by the DataHub integrations service

## Output

### Console Output

The script prints:

- Progress updates for each dataset processed
- Success/failure status with metrics
- Summary statistics (success rate, average latency, description length)
- ASCII table comparing baseline vs eval (if applicable)
- Quality comparison summary (if applicable)

Example ASCII table output:

```
========================================================================================================================
RESULTS SUMMARY COMPARISON
========================================================================================================================

Metric                              Baseline                                 Eval
------------------------------------------------------------------------------------------------------------------------
Model                               existing_entity_data                     bedrock/anthropic.claude-sonnet-4-5-v1:0

Total Requests                      50                                       50
Successful                          45                                       48
Failed                              5                                        2
Success Rate                        90.0%                                    96.0%

Avg Latency (ms)                    0.00                                     2150.34
P50 Latency (ms)                    0.00                                     2089.56
P95 Latency (ms)                    0.00                                     2567.89

Avg Description Length              156.3 chars                              289.7 chars

Latency Change                      ↑ 2150.34ms (+inf%)
Description Length Change           ↑ 133.4 chars (+85.4%)
------------------------------------------------------------------------------------------------------------------------
QUALITY COMPARISON (Opus 4.5)
------------------------------------------------------------------------------------------------------------------------
Total Comparisons                   45
Baseline Wins                       8 (17.8%)
Eval Wins                           32 (71.1%)
Ties                                5 (11.1%)

Avg Quality Score (Baseline)        6.23/10
Avg Quality Score (Eval)            8.14/10
Quality Score Change                ↑ 1.91 points

Overall Quality Winner              Eval (bedrock/anthropic.claude-sonnet-4-5-v1:0)
========================================================================================================================
```

### CSV Output

Two formats depending on whether you run a comparison:

#### Single Run Format

Columns:

- `run_label`: "baseline"
- `model`: Model identifier (e.g., "bedrock/anthropic.claude-sonnet-4-0-v1:0" or "existing_entity_data")
- `urn`: Dataset URN
- `index`: Processing order
- `success`: True/False
- `latency_ms`: Total time to generate (0 for existing descriptions)
- `metadata_extraction_time_ms`: Time to fetch metadata
- `has_table_description`: Boolean
- `table_description_length`: Character count
- `num_columns`: Total columns
- `num_columns_with_description`: Columns with generated descriptions
- `failure_reason`: Error message if failed
- `table_description`: Generated description text
- `column_descriptions`: JSON map of column names to descriptions

#### Side-by-Side Comparison Format

All columns prefixed with `baseline_*` and `eval_*`, plus optional quality fields:

- `quality_winner`: "A" (baseline), "B" (eval), or "tie"
- `quality_reasoning`: Opus 4.5's explanation
- `quality_score_baseline`: Score 1-10
- `quality_score_eval`: Score 1-10
- `quality_comparison_status`: "completed" or "error"

When using `--use-existing-as-baseline`, the baseline_model will be "existing_entity_data".

## Performance Tuning

Adjust concurrency based on your infrastructure:

```bash
# Higher concurrency for faster processing (may hit rate limits)
python generate_docs_eval.py --concurrency 16

# Lower concurrency for quality comparison (Bedrock rate limits)
python generate_docs_eval.py --compare-quality --quality-concurrency 3
```

Note: When using `--use-existing-as-baseline`, the baseline fetch is sequential (not concurrent) to avoid overwhelming the DataHub instance.

## Quality Comparison Details

When using `--compare-quality`, Claude Opus 4.5 evaluates each pair of descriptions based on:

1. Accuracy and relevance to the dataset URN
2. Clarity and readability
3. Completeness and informativeness (including column-level descriptions)
4. Professional tone and grammar

The quality judge returns:

- Winner selection (A, B, or tie)
- Reasoning for the decision
- Numeric quality scores (1-10) for each description

The quality comparison logic is implemented in [quality_comparison.py](quality_comparison.py).

## Real-World Examples

### Evaluate New Model Against Production

Before deploying a new model to production, compare it against existing descriptions:

```bash
export DATAHUB_GMS_URL=https://my-datahub.company.com
export DATAHUB_GMS_TOKEN=my_token
export AWS_PROFILE=production

python generate_docs_eval.py \
  --limit 100 \
  --use-existing-as-baseline \
  --eval-model bedrock/anthropic.claude-sonnet-4-5-v1:0 \
  --compare-quality \
  --output production_vs_sonnet45.csv \
  --concurrency 8 \
  --quality-concurrency 5
```

This will:

1. Fetch existing descriptions from 100 datasets
2. Generate new descriptions using Sonnet 4.5
3. Compare quality using Opus 4.5
4. Export detailed results to CSV

### Compare Sonnet 4 vs Haiku 3.5 with Quality Analysis

```bash
export DATAHUB_GMS_URL=https://my-datahub.company.com
export DATAHUB_GMS_TOKEN=my_token
export AWS_PROFILE=production

python generate_docs_eval.py \
  --limit 100 \
  --eval-model bedrock/anthropic.claude-haiku-3-5-v2:0 \
  --compare-quality \
  --output sonnet_vs_haiku.csv \
  --concurrency 8 \
  --quality-concurrency 5
```

### Test New Model Configuration

```bash
# Test with just 10 datasets to validate configuration
python generate_docs_eval.py \
  --limit 10 \
  --eval-model bedrock/anthropic.claude-sonnet-4-0-v1:0 \
  --output quick_test.csv
```

### Add Quality Analysis to Existing Results

```bash
# You already ran generation, now add quality scores
export AWS_PROFILE=production

python generate_docs_eval.py \
  --input-csv sonnet_vs_haiku.csv \
  --compare-quality \
  --output sonnet_vs_haiku_with_quality.csv
```

## Troubleshooting

### Connection Issues

If you see "Unable to connect to DataHub":

- Verify `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` are set correctly
- Check `~/.datahubenv` file if using default credentials
- Test connection: `datahub get --urn "urn:li:dataset:..."`

### AWS Bedrock Issues

If quality comparison fails:

- Verify AWS credentials: `aws sts get-caller-identity`
- Check Bedrock model access in the specified region
- Confirm you have access to `us.anthropic.claude-opus-4-5-20251101-v1:0`
- Try a different `--bedrock-region` if needed

### Rate Limiting

If you encounter rate limits:

- Reduce `--concurrency` (description generation)
- Reduce `--quality-concurrency` (quality comparison)
- Add delays between batches

### Memory Issues

For large datasets (1000+):

- Process in smaller batches using `--limit`
- Monitor memory usage during concurrent processing
- Reduce concurrency settings

### Existing Descriptions Not Found

When using `--use-existing-as-baseline`, some datasets may not have existing descriptions:

- These will be included in the baseline with `success=False`
- The failure reason will be "No existing descriptions found"
- Quality comparison will skip these URNs (can't compare missing descriptions)
- This is expected behavior for newly ingested datasets

## Interpreting Results

### Success Metrics

- **Success Rate**: Should be >95% for production workloads
- **Failures**: Review `failure_reason` column for patterns
- When using `--use-existing-as-baseline`, lower success rate is expected if many datasets lack descriptions

### Performance Metrics

- **Latency**: Includes metadata extraction + AI generation
- **P50/P95**: Understand typical vs worst-case performance
- Higher latency with Opus models vs Haiku is expected
- Baseline latency is 0ms when using `--use-existing-as-baseline` (no generation)

### Quality Metrics

- **Winner Counts**: Which model wins more head-to-head comparisons
- **Quality Scores**: Absolute quality ratings (aim for 7+ average)
- **Description Length**: Longer isn't always better, but very short may indicate incomplete descriptions
- When comparing against existing descriptions, focus on quality scores more than length

### Making Deployment Decisions

When evaluating a new model for production:

1. **Quality Winner**: New model should win >60% of comparisons
2. **Quality Score**: New model should average >7.5/10
3. **Success Rate**: Should be >95% for production readiness
4. **Latency**: Acceptable latency depends on your use case (batch vs real-time)

## Integration with CI/CD

Use this script in automated workflows:

```bash
#!/bin/bash
# regression_test.sh - Run before deploying new model config

# Run evaluation
python generate_docs_eval.py \
  --limit 50 \
  --use-existing-as-baseline \
  --eval-model "${NEW_MODEL}" \
  --output regression_test.csv

# Parse CSV to verify success rate meets threshold
success_rate=$(python -c "
import csv
with open('regression_test.csv') as f:
    reader = csv.DictReader(f)
    total = 0
    successful = 0
    for row in reader:
        if row['eval_success'] == 'True':
            successful += 1
        total += 1
    print(successful / total * 100 if total > 0 else 0)
")

echo "Success rate: ${success_rate}%"

if (( $(echo "$success_rate < 95" | bc -l) )); then
  echo "ERROR: Success rate below 95%"
  exit 1
fi

echo "SUCCESS: Model passes regression tests"
```

## Development

To modify the quality comparison logic, edit [quality_comparison.py](quality_comparison.py). The module is separated for easier testing and maintenance.

Key functions:

- `compare_descriptions_quality()`: Compares two descriptions using Opus 4.5
- `run_quality_comparisons()`: Orchestrates concurrent quality comparisons for all URN pairs
