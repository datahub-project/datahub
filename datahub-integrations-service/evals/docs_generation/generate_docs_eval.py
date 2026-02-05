#!/usr/bin/env python3
"""
Script to fetch top 100 dataset URNs from DataHub search API and generate descriptions.

Usage:
    python generate_docs_eval.py [--limit 100] [--output results.csv] [--concurrency 8]
                                  [--eval-model MODEL] [--compare-quality]
                                  [--bedrock-region REGION] [--quality-concurrency 5]
                                  [--input-csv existing_results.csv]

The script will use DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN environment variables
to connect to DataHub. If not set, it will fall back to DataHubClient.from_env()
which reads from ~/.datahubenv.

By default, processes 8 URNs concurrently for improved performance.

Features:
- Runs baseline evaluation with default model
- Optionally runs eval with a different model for comparison
- Generates ASCII table summary comparing latency, success rate, and description length
- Optionally runs quality comparison using Bedrock Opus 4.5 to evaluate description quality
- Exports results to CSV with side-by-side comparison
- Can load existing CSV results and run only quality comparison

Modes:
1. Generate new results (default): Runs baseline and optional eval model
2. Use existing entity data as baseline: Fetch current descriptions from DataHub as baseline
3. Quality comparison only: Use --input-csv to load existing results and add quality scores

Example:
    # Basic usage - generate baseline results only
    export DATAHUB_GMS_URL=http://localhost:8080
    export DATAHUB_GMS_TOKEN=your_token_here
    python generate_docs_eval.py --limit 100 --output results.csv --concurrency 8

    # Compare two models with full generation
    python generate_docs_eval.py --limit 50 \\
        --eval-model bedrock/anthropic.claude-sonnet-4-0-v1:0 \\
        --output comparison.csv

    # Full comparison with quality analysis (requires AWS credentials)
    export AWS_PROFILE=your-profile
    python generate_docs_eval.py --limit 50 \\
        --eval-model bedrock/anthropic.claude-haiku-3-5-v2:0 \\
        --compare-quality \\
        --bedrock-region us-west-2 \\
        --output full_comparison.csv

    # Run quality comparison on existing results (no generation, just evaluation)
    export AWS_PROFILE=your-profile
    python generate_docs_eval.py \\
        --input-csv existing_comparison.csv \\
        --compare-quality \\
        --output comparison_with_quality.csv

    # Compare new model against existing production descriptions
    python generate_docs_eval.py --limit 50 \\
        --use-existing-as-baseline \\
        --eval-model bedrock/anthropic.claude-sonnet-4-5-v1:0 \\
        --output new_vs_existing.csv
"""

import argparse
import asyncio
import csv
import json
import logging
import os
from typing import Dict, List, Optional

from datahub.ingestion.graph.client import get_default_graph
from datahub.sdk import DataHubClient
from datahub.sdk.search_filters import FilterDsl
from datahub.utilities.perf_timer import PerfTimer

from datahub_integrations.gen_ai import model_config as model_config_module
from datahub_integrations.gen_ai.description_v3 import (
    generate_entity_descriptions_for_urn,
)
from quality_comparison import run_quality_comparisons

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generate_ascii_table(
    baseline_results: List[Dict],
    eval_results: List[Dict],
    quality_comparisons: Optional[List[Dict]] = None,
) -> str:
    """
    Generate an ASCII table comparing baseline and eval results.

    Args:
        baseline_results: Results from baseline run
        eval_results: Results from eval run
        quality_comparisons: Optional quality comparison results from Opus

    Returns:
        ASCII table as string
    """

    # Calculate summary statistics
    def calc_stats(results: List[Dict], label: str) -> Dict:
        successful = [r for r in results if r.get("success")]
        failed = [r for r in results if not r.get("success")]

        return {
            "label": label,
            "total": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(results) * 100 if results else 0,
            "avg_latency": (
                sum(r["latency_ms"] for r in successful) / len(successful)
                if successful
                else 0
            ),
            "p50_latency": (
                sorted([r["latency_ms"] for r in successful])[len(successful) // 2]
                if successful
                else 0
            ),
            "p95_latency": (
                sorted([r["latency_ms"] for r in successful])[
                    int(len(successful) * 0.95)
                ]
                if successful
                else 0
            ),
            "avg_desc_length": (
                sum(r.get("table_description_length", 0) for r in successful)
                / len(successful)
                if successful
                else 0
            ),
            "model": results[0].get("model", "unknown") if results else "unknown",
        }

    baseline_stats = calc_stats(baseline_results, "Baseline")
    eval_stats = calc_stats(eval_results, "Eval")

    # Build ASCII table
    lines = []
    lines.append("=" * 120)
    lines.append("RESULTS SUMMARY COMPARISON")
    lines.append("=" * 120)
    lines.append("")

    # Header
    lines.append(f"{'Metric':<35} {'Baseline':<40} {'Eval':<40}")
    lines.append("-" * 120)

    # Model
    lines.append(
        f"{'Model':<35} {baseline_stats['model']:<40} {eval_stats['model']:<40}"
    )
    lines.append("")

    # Success metrics
    lines.append(
        f"{'Total Requests':<35} {baseline_stats['total']:<40} {eval_stats['total']:<40}"
    )
    lines.append(
        f"{'Successful':<35} {baseline_stats['successful']:<40} {eval_stats['successful']:<40}"
    )
    lines.append(
        f"{'Failed':<35} {baseline_stats['failed']:<40} {eval_stats['failed']:<40}"
    )
    lines.append(
        f"{'Success Rate':<35} {baseline_stats['success_rate']:.1f}%{'':<34} {eval_stats['success_rate']:.1f}%"
    )
    lines.append("")

    # Latency metrics
    lines.append(
        f"{'Avg Latency (ms)':<35} {baseline_stats['avg_latency']:.2f}{'':<34} {eval_stats['avg_latency']:.2f}"
    )
    lines.append(
        f"{'P50 Latency (ms)':<35} {baseline_stats['p50_latency']:.2f}{'':<34} {eval_stats['p50_latency']:.2f}"
    )
    lines.append(
        f"{'P95 Latency (ms)':<35} {baseline_stats['p95_latency']:.2f}{'':<34} {eval_stats['p95_latency']:.2f}"
    )
    lines.append("")

    # Description length
    lines.append(
        f"{'Avg Description Length':<35} {baseline_stats['avg_desc_length']:.1f} chars{'':<27} {eval_stats['avg_desc_length']:.1f} chars"
    )
    lines.append("")

    # Latency comparison
    latency_diff = eval_stats["avg_latency"] - baseline_stats["avg_latency"]
    latency_pct = (
        (latency_diff / baseline_stats["avg_latency"] * 100)
        if baseline_stats["avg_latency"] > 0
        else 0
    )
    latency_symbol = "↑" if latency_diff > 0 else "↓"
    lines.append(
        f"{'Latency Change':<35} {latency_symbol} {abs(latency_diff):.2f}ms ({latency_pct:+.1f}%)"
    )

    # Description length comparison
    length_diff = eval_stats["avg_desc_length"] - baseline_stats["avg_desc_length"]
    length_pct = (
        (length_diff / baseline_stats["avg_desc_length"] * 100)
        if baseline_stats["avg_desc_length"] > 0
        else 0
    )
    length_symbol = "↑" if length_diff > 0 else "↓"
    lines.append(
        f"{'Description Length Change':<35} {length_symbol} {abs(length_diff):.1f} chars ({length_pct:+.1f}%)"
    )

    # Add quality comparison section if available
    if quality_comparisons:
        lines.append("")
        lines.append("-" * 120)
        lines.append("QUALITY COMPARISON (Opus 4.5)")
        lines.append("-" * 120)

        winners = [c["winner"] for c in quality_comparisons if c.get("winner")]
        baseline_wins = winners.count("A")
        eval_wins = winners.count("B")
        ties = winners.count("tie")
        total_comparisons = len(quality_comparisons)

        lines.append(f"{'Total Comparisons':<35} {total_comparisons}")
        lines.append(
            f"{'Baseline Wins':<35} {baseline_wins} ({baseline_wins / total_comparisons * 100:.1f}%)"
        )
        lines.append(
            f"{'Eval Wins':<35} {eval_wins} ({eval_wins / total_comparisons * 100:.1f}%)"
        )
        lines.append(f"{'Ties':<35} {ties} ({ties / total_comparisons * 100:.1f}%)")
        lines.append("")

        if winners:
            avg_baseline_score = sum(
                c.get("quality_score_baseline", 0) for c in quality_comparisons
            ) / len(quality_comparisons)
            avg_eval_score = sum(
                c.get("quality_score_eval", 0) for c in quality_comparisons
            ) / len(quality_comparisons)

            lines.append(
                f"{'Avg Quality Score (Baseline)':<35} {avg_baseline_score:.2f}/10"
            )
            lines.append(f"{'Avg Quality Score (Eval)':<35} {avg_eval_score:.2f}/10")

            score_diff = avg_eval_score - avg_baseline_score
            score_symbol = "↑" if score_diff > 0 else "↓"
            lines.append(
                f"{'Quality Score Change':<35} {score_symbol} {abs(score_diff):.2f} points"
            )

            # Determine overall winner
            if eval_wins > baseline_wins:
                overall_winner = f"Eval ({eval_stats['model']})"
            elif baseline_wins > eval_wins:
                overall_winner = f"Baseline ({baseline_stats['model']})"
            else:
                overall_winner = "Tie"

            lines.append("")
            lines.append(f"{'Overall Quality Winner':<35} {overall_winner}")

    lines.append("=" * 120)

    return "\n".join(lines)




def load_results_from_csv(csv_path: str) -> tuple[List[Dict], List[Dict]]:
    """
    Load baseline and eval results from an existing CSV file.

    Args:
        csv_path: Path to CSV file with side-by-side comparison results

    Returns:
        Tuple of (baseline_results, eval_results)
    """
    baseline_results = []
    eval_results = []

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            urn = row["urn"]
            index = int(row["index"]) if row.get("index") else None

            # Parse baseline result
            baseline_result = {
                "urn": urn,
                "index": index,
                "run_label": "baseline",
                "model": row.get("baseline_model"),
                "success": row.get("baseline_success") == "True",
                "latency_ms": float(row["baseline_latency_ms"])
                if row.get("baseline_latency_ms")
                else 0,
                "metadata_extraction_time_ms": float(
                    row["baseline_metadata_extraction_time_ms"]
                )
                if row.get("baseline_metadata_extraction_time_ms")
                else 0,
                "has_table_description": row.get("baseline_has_table_description")
                == "True",
                "table_description_length": int(
                    row["baseline_table_description_length"]
                )
                if row.get("baseline_table_description_length")
                else 0,
                "num_columns": int(row["baseline_num_columns"])
                if row.get("baseline_num_columns")
                else 0,
                "num_columns_with_description": int(
                    row["baseline_num_columns_with_description"]
                )
                if row.get("baseline_num_columns_with_description")
                else 0,
                "failure_reason": row.get("baseline_failure_reason"),
                "error_type": row.get("baseline_error_type"),
                "error": row.get("baseline_error"),
                "table_description": row.get("baseline_table_description"),
                "column_descriptions": json.loads(row["baseline_column_descriptions"])
                if row.get("baseline_column_descriptions")
                else None,
            }

            # Parse eval result
            eval_result = {
                "urn": urn,
                "index": index,
                "run_label": "eval",
                "model": row.get("eval_model"),
                "success": row.get("eval_success") == "True",
                "latency_ms": float(row["eval_latency_ms"])
                if row.get("eval_latency_ms")
                else 0,
                "metadata_extraction_time_ms": float(
                    row["eval_metadata_extraction_time_ms"]
                )
                if row.get("eval_metadata_extraction_time_ms")
                else 0,
                "has_table_description": row.get("eval_has_table_description")
                == "True",
                "table_description_length": int(row["eval_table_description_length"])
                if row.get("eval_table_description_length")
                else 0,
                "num_columns": int(row["eval_num_columns"])
                if row.get("eval_num_columns")
                else 0,
                "num_columns_with_description": int(
                    row["eval_num_columns_with_description"]
                )
                if row.get("eval_num_columns_with_description")
                else 0,
                "failure_reason": row.get("eval_failure_reason"),
                "error_type": row.get("eval_error_type"),
                "error": row.get("eval_error"),
                "table_description": row.get("eval_table_description"),
                "column_descriptions": json.loads(row["eval_column_descriptions"])
                if row.get("eval_column_descriptions")
                else None,
            }

            baseline_results.append(baseline_result)
            eval_results.append(eval_result)

    print(
        f"Loaded {len(baseline_results)} baseline and {len(eval_results)} eval results from {csv_path}"
    )

    return baseline_results, eval_results


def fetch_existing_descriptions_as_baseline(urns: List[str]) -> List[Dict]:
    """
    Fetch existing descriptions from DataHub entities to use as baseline.

    Args:
        urns: List of dataset URNs to fetch

    Returns:
        List of result dictionaries with existing descriptions as baseline
    """
    graph = get_default_graph()
    baseline_results = []

    print(f"\nFetching existing descriptions for {len(urns)} URNs as baseline...")
    print("=" * 80)

    for idx, urn in enumerate(urns, 1):
        print(f"\n[{idx}/{len(urns)}] Fetching: {urn}")

        result_dict = {
            "urn": urn,
            "index": idx,
            "run_label": "baseline",
            "model": "existing_entity_data",
            "success": False,
            "latency_ms": 0,  # No generation, just fetch
            "metadata_extraction_time_ms": 0,
        }

        try:
            # Fetch entity data
            entity = graph.get_entity_semityped(urn)

            # Extract existing documentation
            table_description = None
            if entity and hasattr(entity, "editableSchemaMetadata"):
                editable_metadata = entity.editableSchemaMetadata
                if editable_metadata and hasattr(
                    editable_metadata, "editableSchemaFieldInfo"
                ):
                    # Get table-level description
                    if hasattr(entity, "editableDatasetProperties"):
                        dataset_props = entity.editableDatasetProperties
                        if dataset_props and hasattr(dataset_props, "description"):
                            table_description = dataset_props.description

            # Extract column descriptions
            column_descriptions = {}
            if entity and hasattr(entity, "schemaMetadata"):
                schema_metadata = entity.schemaMetadata
                if schema_metadata and hasattr(schema_metadata, "fields"):
                    for field in schema_metadata.fields:
                        field_path = field.fieldPath
                        description = None

                        # Try to get description from editableSchemaMetadata
                        if (
                            hasattr(entity, "editableSchemaMetadata")
                            and entity.editableSchemaMetadata
                        ):
                            editable = entity.editableSchemaMetadata
                            if hasattr(
                                editable, "editableSchemaFieldInfo"
                            ) and editable.editableSchemaFieldInfo:
                                for field_info in editable.editableSchemaFieldInfo:
                                    if field_info.fieldPath == field_path:
                                        if hasattr(field_info, "description"):
                                            description = field_info.description
                                        break

                        # Fallback to schema field description
                        if not description and hasattr(field, "description"):
                            description = field.description

                        if description:
                            column_descriptions[field_path] = description

            # Determine success based on whether we found any descriptions
            has_table_desc = table_description is not None and len(table_description) > 0
            has_col_desc = len(column_descriptions) > 0

            result_dict.update(
                {
                    "success": has_table_desc
                    or has_col_desc,  # Success if we found any descriptions
                    "has_table_description": has_table_desc,
                    "table_description": table_description,
                    "table_description_length": (
                        len(table_description) if table_description else 0
                    ),
                    "column_descriptions": (
                        column_descriptions if column_descriptions else None
                    ),
                    "num_columns": (
                        len(schema_metadata.fields)
                        if entity
                        and hasattr(entity, "schemaMetadata")
                        and entity.schemaMetadata
                        and hasattr(entity.schemaMetadata, "fields")
                        else 0
                    ),
                    "num_columns_with_description": len(column_descriptions),
                    "failure_reason": None
                    if (has_table_desc or has_col_desc)
                    else "No existing descriptions found",
                }
            )

            print(
                f"  ✓ Found: table_desc={has_table_desc}, "
                f"columns={len(column_descriptions)}/{result_dict['num_columns']}"
            )

        except Exception as e:
            result_dict.update(
                {
                    "success": False,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "has_table_description": False,
                    "table_description": None,
                    "table_description_length": 0,
                    "column_descriptions": None,
                    "num_columns": 0,
                    "num_columns_with_description": 0,
                    "failure_reason": f"Error fetching entity: {str(e)}",
                }
            )
            print(f"  ✗ Failed: {type(e).__name__}: {str(e)[:100]}")

        baseline_results.append(result_dict)

    print("\n" + "=" * 80)
    print(f"Completed fetching {len(urns)} URNs")

    # Print summary
    successful = sum(1 for r in baseline_results if r["success"])
    print("\nSummary:")
    print(f"  Found descriptions: {successful}/{len(urns)}")
    print(
        f"  Missing descriptions: {len(urns) - successful}/{len(urns)} (will be used as baseline anyway)"
    )

    return baseline_results


def fetch_top_datasets(limit: int = 100) -> List[str]:
    """
    Fetch top dataset URNs from DataHub search API.

    Args:
        limit: Maximum number of datasets to fetch (default: 100)

    Returns:
        List of dataset URNs
    """
    # Initialize the DataHub client from environment variables
    gms_url = os.environ.get("DATAHUB_GMS_URL")
    gms_token = os.environ.get("DATAHUB_GMS_TOKEN")

    if gms_url and gms_token:
        print(f"Connecting to DataHub at {gms_url} using environment variables...")
        client = DataHubClient(server=gms_url, token=gms_token)
    else:
        print("Using DataHubClient.from_env() to connect...")
        client = DataHubClient.from_env()

    # Create filter for datasets only
    dataset_filter = FilterDsl.entity_type("dataset")

    # Use the search client to get dataset URNs
    search_client = client.search
    urns = []

    print(f"Fetching top {limit} dataset URNs...")

    # Get URNs using the search client
    for idx, urn in enumerate(search_client.get_urns(filter=dataset_filter)):
        if idx >= limit:
            break
        urns.append(str(urn))
        if (idx + 1) % 10 == 0:
            print(f"Fetched {idx + 1} datasets...")

    print(f"Successfully fetched {len(urns)} dataset URNs")
    return urns


async def process_single_urn(
    graph, urn: str, idx: int, total_urns: int, run_label: str, actual_model: str
) -> Dict:
    """
    Process a single URN and generate descriptions.

    Args:
        graph: DataHub graph client
        urn: Dataset URN to process
        idx: Index of this URN in the batch
        total_urns: Total number of URNs being processed
        run_label: Label for this run
        actual_model: The actual model being used

    Returns:
        Result dictionary with metrics
    """
    print(f"\n[{idx}/{total_urns}] Processing: {urn}")

    result_dict = {
        "urn": urn,
        "index": idx,
        "run_label": run_label,
        "model": actual_model,
    }

    # Measure total latency for this URN
    with PerfTimer() as total_timer:
        try:
            # Run in thread pool since generate_entity_descriptions_for_urn is synchronous
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, generate_entity_descriptions_for_urn, graph, urn
            )

            # Record success metrics
            result_dict.update(
                {
                    "success": True,
                    "latency_ms": total_timer.elapsed_seconds() * 1000,
                    "metadata_extraction_time_ms": result.metadata_extraction_time_ms,
                    "has_table_description": result.table_description is not None,
                    "table_description_length": (
                        len(result.table_description) if result.table_description else 0
                    ),
                    "num_columns": result.num_columns,
                    "num_columns_with_description": result.num_columns_with_description,
                    "failure_reason": result.failure_reason,
                    "table_description": result.table_description,
                    "column_descriptions": result.column_descriptions,
                }
            )

            print(
                f"  ✓ Success in {result_dict['latency_ms']:.2f}ms "
                f"(table_desc: {result_dict['has_table_description']}, "
                f"columns: {result_dict['num_columns_with_description']}/{result_dict['num_columns']})"
            )

        except Exception as e:
            # Record failure
            result_dict.update(
                {
                    "success": False,
                    "latency_ms": total_timer.elapsed_seconds() * 1000,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
            )

            print(f"  ✗ Failed: {type(e).__name__}: {str(e)[:100]}")

    # Output result as JSON to console
    print(f"  Result: {json.dumps(result_dict)}")
    return result_dict


async def generate_descriptions_for_urns_async(
    urns: List[str], run_label: str = "default", concurrency: int = 8
) -> List[Dict]:
    """
    Generate descriptions for each URN with concurrency control.

    Args:
        urns: List of dataset URNs
        run_label: Label for this run (e.g., "baseline" or "eval")
        concurrency: Number of concurrent requests (default: 8)

    Returns:
        List of result dictionaries with URN, latency, and generation results
    """
    # Get graph client for description generation
    graph = get_default_graph()

    total_urns = len(urns)

    # Get the actual model name from config
    actual_model = model_config_module.model_config.documentation_ai.model

    print(
        f"\nGenerating descriptions for {total_urns} URNs using model: {actual_model}"
    )
    print(f"Run label: {run_label}")
    print(f"Concurrency: {concurrency}")
    print("=" * 80)

    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def process_with_semaphore(urn: str, idx: int) -> Dict:
        async with semaphore:
            return await process_single_urn(
                graph, urn, idx, total_urns, run_label, actual_model
            )

    # Process all URNs concurrently with limit
    tasks = [process_with_semaphore(urn, idx) for idx, urn in enumerate(urns, 1)]
    results = await asyncio.gather(*tasks)

    print("\n" + "=" * 80)
    print(f"Completed processing {total_urns} URNs")

    # Print summary statistics
    successful = sum(1 for r in results if r["success"])
    failed = total_urns - successful
    avg_latency = (
        sum(r["latency_ms"] for r in results if r["success"]) / successful
        if successful > 0
        else 0
    )

    print("\nSummary:")
    print(f"  Successful: {successful}/{total_urns}")
    print(f"  Failed: {failed}/{total_urns}")
    if successful > 0:
        print(f"  Avg latency: {avg_latency:.2f}ms")

    return results


def generate_descriptions_for_urns(
    urns: List[str], run_label: str = "default", concurrency: int = 8
) -> List[Dict]:
    """
    Synchronous wrapper for async description generation.

    Args:
        urns: List of dataset URNs
        run_label: Label for this run (e.g., "baseline" or "eval")
        concurrency: Number of concurrent requests (default: 8)

    Returns:
        List of result dictionaries with URN, latency, and generation results
    """
    return asyncio.run(
        generate_descriptions_for_urns_async(urns, run_label, concurrency)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch dataset URNs from DataHub and generate descriptions with latency measurements. "
        "Uses DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN environment variables, "
        "or falls back to ~/.datahubenv configuration."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of datasets to fetch (default: 100)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file to write results as CSV. "
        "If not specified, only prints to stdout",
    )
    parser.add_argument(
        "--eval-model",
        type=str,
        help="Optional: Run a second evaluation with this model. "
        "This will temporarily set model_config.documentation_ai.model to this value "
        "and run generation on all URNs again. Format: bedrock/model-id or openai/model-id",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Number of concurrent requests to process (default: 8)",
    )
    parser.add_argument(
        "--compare-quality",
        action="store_true",
        help="Run quality comparison using Bedrock Opus 4.5 for baseline vs eval descriptions. "
        "Requires --eval-model to be set. Uses AWS credentials from environment.",
    )
    parser.add_argument(
        "--bedrock-region",
        type=str,
        default="us-west-2",
        help="AWS region for Bedrock API (default: us-west-2)",
    )
    parser.add_argument(
        "--quality-concurrency",
        type=int,
        default=5,
        help="Number of concurrent quality comparison requests (default: 5)",
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        help="Path to existing results CSV file. If provided, only quality comparison will be run "
        "on the baseline/eval results from the file. Requires --compare-quality flag.",
    )
    parser.add_argument(
        "--use-existing-as-baseline",
        action="store_true",
        help="Fetch existing entity descriptions from DataHub as baseline instead of generating new baseline descriptions. "
        "Useful for comparing a new model against current production descriptions. "
        "Requires --eval-model to be set.",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.compare_quality and not args.eval_model and not args.input_csv:
        parser.error(
            "--compare-quality requires either --eval-model or --input-csv to be set"
        )

    if args.input_csv and not args.compare_quality:
        parser.error("--input-csv requires --compare-quality to be set")

    if args.input_csv and args.eval_model:
        parser.error(
            "--input-csv and --eval-model are mutually exclusive (CSV already has both runs)"
        )

    if args.use_existing_as_baseline and not args.eval_model:
        parser.error("--use-existing-as-baseline requires --eval-model to be set")

    all_results = []
    quality_comparisons = []
    baseline_results = []
    eval_results = []

    # Mode 1: Load results from existing CSV
    if args.input_csv:
        print("\n" + "=" * 80)
        print(f"LOADING RESULTS FROM CSV: {args.input_csv}")
        print("=" * 80)

        baseline_results, eval_results = load_results_from_csv(args.input_csv)
        all_results.extend(baseline_results)
        all_results.extend(eval_results)

        # Run quality comparison
        if args.compare_quality:
            quality_comparisons = asyncio.run(
                run_quality_comparisons(
                    baseline_results,
                    eval_results,
                    bedrock_region=args.bedrock_region,
                    concurrency=args.quality_concurrency,
                )
            )

        # Generate ASCII table comparison
        print("\n")
        ascii_table = generate_ascii_table(
            baseline_results,
            eval_results,
            quality_comparisons if quality_comparisons else None,
        )
        print(ascii_table)

    # Mode 2: Generate new results
    else:
        # Fetch the dataset URNs
        urns = fetch_top_datasets(args.limit)

        # Store original model for restoration
        original_model = model_config_module.model_config.documentation_ai.model

        # First run: Generate baseline (either from model or existing entity data)
        if args.use_existing_as_baseline:
            print("\n" + "=" * 80)
            print("BASELINE: Fetching existing entity descriptions from DataHub")
            print("=" * 80)
            baseline_results = fetch_existing_descriptions_as_baseline(urns)
            all_results.extend(baseline_results)
        else:
            print("\n" + "=" * 80)
            print("BASELINE RUN: Using default model configuration")
            print("=" * 80)
            baseline_results = generate_descriptions_for_urns(
                urns, run_label="baseline", concurrency=args.concurrency
            )
            all_results.extend(baseline_results)

        # Second run: Generate descriptions with eval model if specified
        if args.eval_model:
            print("\n" + "=" * 80)
            print(f"EVAL RUN: Using eval model: {args.eval_model}")
            print("=" * 80)

            # Temporarily override the model configuration
            print(f"Overriding model from {original_model} to {args.eval_model}")
            model_config_module.model_config.documentation_ai.model = args.eval_model

            # Clear any LRU caches that might have cached the old model
            # This ensures the new model is actually used
            try:
                from datahub_integrations.gen_ai.llm.factory import get_llm_client

                if hasattr(get_llm_client, "cache_clear"):
                    get_llm_client.cache_clear()
            except Exception as e:
                print(f"Note: Could not clear LLM client cache: {e}")

            eval_results = generate_descriptions_for_urns(
                urns, run_label="eval", concurrency=args.concurrency
            )
            all_results.extend(eval_results)

            # Restore original model
            model_config_module.model_config.documentation_ai.model = original_model
            print(f"\nRestored model to: {original_model}")

            # Run quality comparison if requested
            if args.compare_quality:
                quality_comparisons = asyncio.run(
                    run_quality_comparisons(
                        baseline_results,
                        eval_results,
                        bedrock_region=args.bedrock_region,
                        concurrency=args.quality_concurrency,
                    )
                )

            # Generate ASCII table comparison (after quality comparison if enabled)
            print("\n")
            ascii_table = generate_ascii_table(
                baseline_results,
                eval_results,
                quality_comparisons if quality_comparisons else None,
            )
            print(ascii_table)

    # Write results to file if requested
    if args.output:
        with open(args.output, "w", newline="") as f:
            if all_results:
                # Check if we have both baseline and eval runs
                baseline_results = [
                    r for r in all_results if r.get("run_label") == "baseline"
                ]
                eval_results = [r for r in all_results if r.get("run_label") == "eval"]
                has_eval = len(eval_results) > 0

                if has_eval:
                    # Side-by-side comparison format
                    fieldnames = [
                        "urn",
                        "index",
                        "baseline_model",
                        "baseline_success",
                        "baseline_latency_ms",
                        "baseline_metadata_extraction_time_ms",
                        "baseline_has_table_description",
                        "baseline_table_description_length",
                        "baseline_num_columns",
                        "baseline_num_columns_with_description",
                        "baseline_failure_reason",
                        "baseline_error_type",
                        "baseline_error",
                        "baseline_table_description",
                        "baseline_column_descriptions",
                        "eval_model",
                        "eval_success",
                        "eval_latency_ms",
                        "eval_metadata_extraction_time_ms",
                        "eval_has_table_description",
                        "eval_table_description_length",
                        "eval_num_columns",
                        "eval_num_columns_with_description",
                        "eval_failure_reason",
                        "eval_error_type",
                        "eval_error",
                        "eval_table_description",
                        "eval_column_descriptions",
                    ]

                    # Add quality comparison fields if available
                    if quality_comparisons:
                        fieldnames.extend(
                            [
                                "quality_winner",
                                "quality_reasoning",
                                "quality_score_baseline",
                                "quality_score_eval",
                                "quality_comparison_status",
                            ]
                        )

                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    # Create a mapping of URN to results
                    baseline_by_urn = {r["urn"]: r for r in baseline_results}
                    eval_by_urn = {r["urn"]: r for r in eval_results}
                    quality_by_urn = (
                        {c["urn"]: c for c in quality_comparisons}
                        if quality_comparisons
                        else {}
                    )

                    # Get all unique URNs (should be the same for both runs)
                    all_urns = sorted(
                        set(baseline_by_urn.keys()) | set(eval_by_urn.keys())
                    )

                    for urn in all_urns:
                        baseline = baseline_by_urn.get(urn, {})
                        eval_result = eval_by_urn.get(urn, {})
                        quality = quality_by_urn.get(urn, {})

                        # Build side-by-side row
                        row = {
                            "urn": urn,
                            "index": baseline.get("index") or eval_result.get("index"),
                        }

                        # Add baseline columns
                        for key in [
                            "model",
                            "success",
                            "latency_ms",
                            "metadata_extraction_time_ms",
                            "has_table_description",
                            "table_description_length",
                            "num_columns",
                            "num_columns_with_description",
                            "failure_reason",
                            "error_type",
                            "error",
                            "table_description",
                            "column_descriptions",
                        ]:
                            value = baseline.get(key)
                            if key == "column_descriptions" and value:
                                value = json.dumps(value)
                            row[f"baseline_{key}"] = value

                        # Add eval columns
                        for key in [
                            "model",
                            "success",
                            "latency_ms",
                            "metadata_extraction_time_ms",
                            "has_table_description",
                            "table_description_length",
                            "num_columns",
                            "num_columns_with_description",
                            "failure_reason",
                            "error_type",
                            "error",
                            "table_description",
                            "column_descriptions",
                        ]:
                            value = eval_result.get(key)
                            if key == "column_descriptions" and value:
                                value = json.dumps(value)
                            row[f"eval_{key}"] = value

                        # Add quality comparison columns if available
                        if quality_comparisons:
                            row["quality_winner"] = quality.get("winner")
                            row["quality_reasoning"] = quality.get("reasoning")
                            row["quality_score_baseline"] = quality.get(
                                "quality_score_baseline"
                            )
                            row["quality_score_eval"] = quality.get(
                                "quality_score_eval"
                            )
                            row["quality_comparison_status"] = quality.get("comparison")

                        writer.writerow(row)
                else:
                    # Single run format (baseline only)
                    fieldnames = [
                        "run_label",
                        "model",
                        "urn",
                        "index",
                        "success",
                        "latency_ms",
                        "metadata_extraction_time_ms",
                        "has_table_description",
                        "table_description_length",
                        "num_columns",
                        "num_columns_with_description",
                        "failure_reason",
                        "error_type",
                        "error",
                        "table_description",
                        "column_descriptions",
                    ]

                    writer = csv.DictWriter(
                        f, fieldnames=fieldnames, extrasaction="ignore"
                    )
                    writer.writeheader()

                    for result in all_results:
                        # Convert column_descriptions dict to JSON string for CSV
                        csv_result = result.copy()
                        if (
                            "column_descriptions" in csv_result
                            and csv_result["column_descriptions"]
                        ):
                            csv_result["column_descriptions"] = json.dumps(
                                csv_result["column_descriptions"]
                            )

                        writer.writerow(csv_result)

        print(f"\nResults written to {args.output}")

        # Print summary statistics per run
        print("\nSummary statistics:")
        for run_label in ["baseline", "eval"]:
            run_results = [r for r in all_results if r.get("run_label") == run_label]
            if not run_results:
                continue

            successful = sum(1 for r in run_results if r["success"])
            failed = len(run_results) - successful
            avg_latency = (
                sum(r["latency_ms"] for r in run_results if r["success"]) / successful
                if successful > 0
                else 0
            )

            # Get the model name from first result
            model_name = (
                run_results[0].get("model", "unknown") if run_results else "unknown"
            )

            print(f"\n  Run: {run_label} (model: {model_name})")
            print(f"    Total: {len(run_results)}")
            print(f"    Successful: {successful}")
            print(f"    Failed: {failed}")
            if successful > 0:
                print(f"    Avg latency: {avg_latency:.2f}ms")


if __name__ == "__main__":
    main()
