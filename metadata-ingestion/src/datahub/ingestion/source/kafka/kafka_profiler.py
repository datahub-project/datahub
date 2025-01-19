import logging
import math
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import avro.io
import avro.schema

from datahub.ingestion.source.kafka.kafka_config import ProfilerConfig
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    KafkaSchemaClass,
    PartitionSpecClass,
    PartitionTypeClass,
    QuantileClass,
    SchemaMetadataClass,
    TimeWindowSizeClass,
    ValueFrequencyClass,
)

logger = logging.getLogger(__name__)


@dataclass
class KafkaFieldStatistics:
    field_path: str
    sample_values: List[str]
    unique_count: int = 0
    unique_proportion: float = 0.0
    null_count: int = 0
    null_proportion: float = 0.0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[Any] = None
    stdev: Optional[float] = None
    quantiles: Optional[List[QuantileClass]] = None
    distinct_value_frequencies: Optional[Dict[str, int]] = None
    data_type: Optional[str] = None

    def __post_init__(self):
        if self.distinct_value_frequencies is None:
            self.distinct_value_frequencies = {}
        if self.quantiles is None:
            self.quantiles = []
        if self.sample_values is None:
            self.sample_values = []


def clean_field_path(field_path: str, preserve_types: bool = True) -> str:
    """Clean field path by optionally preserving or removing version, type and other metadata.

    Args:
        field_path: The full field path string
        preserve_types: If True, preserves version and type information in the path

    Returns:
        The cleaned field path string
    """
    # Don't modify key fields - we want to keep the full path
    if "[key=True]" in field_path:
        return field_path

    if preserve_types:
        # When preserving types, return the full path as-is
        return field_path

    # If not preserving types, use the original stripping logic
    parts = field_path.split(".")
    # Return last non-empty part that isn't a type or version declaration
    for part in reversed(parts):
        if part and not (part.startswith("[version=") or part.startswith("[type=")):
            return part

    return field_path


def flatten_json(
    nested_json: Dict[str, Any],
    parent_key: str = "",
    flattened_dict: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Flatten a nested JSON object into a single level dictionary.
    """
    if flattened_dict is None:
        flattened_dict = {}

    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(value, (dict, list)):
                flatten_json(
                    {"value": value} if isinstance(value, list) else value,
                    new_key,
                    flattened_dict,
                )
            else:
                flattened_dict[new_key] = value
    elif isinstance(nested_json, list):
        for i, item in enumerate(nested_json):
            new_key = f"{parent_key}[{i}]"
            if isinstance(item, (dict, list)):
                flatten_json({"item": item}, new_key, flattened_dict)
            else:
                flattened_dict[new_key] = item
    else:
        flattened_dict[parent_key] = nested_json

    return flattened_dict


class KafkaProfiler:
    """Handles advanced profiling of Kafka message samples"""

    def __init__(self, profiler_config: ProfilerConfig):
        self.profiler_config = profiler_config

    def profile_samples(
        self,
        samples: List[Dict[str, Any]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> DatasetProfileClass:
        """Analyze samples and create a dataset profile incorporating schema information."""
        # Initialize data structures
        field_values: Dict[str, List[Any]] = {}
        field_paths: Dict[str, str] = {}  # Maps clean names to full paths

        # Build field mappings from schema if available
        if schema_metadata and isinstance(
            schema_metadata.platformSchema, KafkaSchemaClass
        ):
            # Handle key schema if present
            if schema_metadata.platformSchema.keySchema:
                try:
                    key_schema = avro.schema.parse(
                        schema_metadata.platformSchema.keySchema
                    )
                    # Map key schema fields if they exist
                    if hasattr(key_schema, "fields"):
                        for schema_field in key_schema.fields:
                            clean_name = clean_field_path(
                                schema_field.name, preserve_types=False
                            )
                            field_paths[clean_name] = schema_field.name
                            field_values[schema_field.name] = []
                except Exception as e:
                    logger.warning(f"Failed to parse key schema: {e}")

            # Map all schema fields, maintaining full paths
            for schema_field in schema_metadata.fields or []:
                clean_name = clean_field_path(
                    schema_field.fieldPath, preserve_types=False
                )
                field_paths[clean_name] = schema_field.fieldPath
                field_values[schema_field.fieldPath] = []

        # Process each sample, mapping fields correctly
        for sample in samples:
            for field_name, value in sample.items():
                if field_name not in ("offset", "timestamp"):
                    field_path = None

                    # Handle key fields specially using schema's key field path
                    key_field = (
                        next(
                            (
                                schema_field
                                for schema_field in (schema_metadata.fields or [])
                                if schema_field.fieldPath.endswith("[key=True]")
                            ),
                            None,
                        )
                        if schema_metadata
                        else None
                    )

                    if field_name == "key" and key_field:
                        field_path = key_field.fieldPath
                    else:
                        # Try to find matching schema field
                        clean_sample = clean_field_path(
                            field_name, preserve_types=False
                        )

                        if schema_metadata and schema_metadata.fields:
                            for schema_field in schema_metadata.fields:
                                if (
                                    clean_field_path(
                                        schema_field.fieldPath, preserve_types=False
                                    )
                                    == clean_sample
                                ):
                                    field_path = schema_field.fieldPath
                                    break

                        if not field_path:
                            field_path = field_paths.get(clean_sample, field_name)

                    # Initialize field if needed and store value with original type
                    if field_path not in field_values:
                        field_values[field_path] = []
                    # Keep original type, don't convert to string
                    field_values[field_path].append(value)

        # Process statistics with original types
        field_stats = {}
        for field_path, values in field_values.items():
            if values:  # Only process fields that have values
                field_stats[field_path] = self._process_field_statistics(
                    field_path=field_path,
                    values=values,
                )

        return self.create_profile_data(field_stats, len(samples))

    def _process_field_statistics(
        self,
        field_path: str,
        values: List[Any],
    ) -> KafkaFieldStatistics:
        """Calculate statistics for a single field based on profiling config"""
        total_count = len(values)
        non_null_values = [v for v in values if v is not None and v != ""]

        # Detect type first
        data_type = "STRING"
        if non_null_values:
            sample_value = non_null_values[0]
            if isinstance(sample_value, (int, float)):
                data_type = "NUMERIC"
            elif isinstance(sample_value, bool):
                data_type = "BOOLEAN"
            elif isinstance(sample_value, (dict, list)):
                data_type = "COMPLEX"

        stats = KafkaFieldStatistics(
            field_path=field_path,
            sample_values=random.sample(
                [str(v) for v in non_null_values] if non_null_values else [""],
                min(3, len(non_null_values)) if non_null_values else 1,
            )
            if self.profiler_config.include_field_sample_values
            else [],
            data_type=data_type,
        )

        # Calculate null statistics
        stats.null_count = total_count - len(non_null_values)
        stats.null_proportion = stats.null_count / total_count if total_count > 0 else 0

        # Calculate distinct value stats
        if self.profiler_config.include_field_distinct_count:
            # Convert to strings only for counting distinct values
            value_counts: Dict[str, int] = {}
            for value in values:
                str_value = str(value)
                value_counts[str_value] = value_counts.get(str_value, 0) + 1

            stats.unique_count = len(value_counts)
            stats.unique_proportion = (
                stats.unique_count / total_count if total_count > 0 else 0
            )
            stats.distinct_value_frequencies = value_counts

        # Calculate numeric statistics only for numeric fields
        if data_type == "NUMERIC":
            numeric_values = [
                float(v) for v in non_null_values if isinstance(v, (int, float))
            ]
            if numeric_values:
                if self.profiler_config.include_field_min_value:
                    stats.min_value = min(numeric_values)
                if self.profiler_config.include_field_max_value:
                    stats.max_value = max(numeric_values)
                if self.profiler_config.include_field_mean_value:
                    stats.mean_value = sum(numeric_values) / len(numeric_values)
                if self.profiler_config.include_field_median_value:
                    stats.median_value = sorted(numeric_values)[
                        len(numeric_values) // 2
                    ]

                # Calculate standard deviation
                if (
                    self.profiler_config.include_field_stddev_value
                    and len(numeric_values) > 1
                ):
                    mean = stats.mean_value or 0
                    variance = sum((x - mean) ** 2 for x in numeric_values) / (
                        len(numeric_values) - 1
                    )
                    stats.stdev = math.sqrt(variance)

                # Calculate quantiles
                if self.profiler_config.include_field_quantiles:
                    sorted_values = sorted(numeric_values)
                    stats.quantiles = [
                        QuantileClass(
                            quantile=str(0.25),
                            value=str(sorted_values[int(len(sorted_values) * 0.25)]),
                        ),
                        QuantileClass(
                            quantile=str(0.5),
                            value=str(sorted_values[int(len(sorted_values) * 0.5)]),
                        ),
                        QuantileClass(
                            quantile=str(0.75),
                            value=str(sorted_values[int(len(sorted_values) * 0.75)]),
                        ),
                    ]

        return stats

    def create_profile_data(
        self, field_stats: Dict[str, KafkaFieldStatistics], sample_count: int
    ) -> DatasetProfileClass:
        """Create DataHub profile class from field statistics"""
        timestamp_millis = int(datetime.now().timestamp() * 1000)
        field_profiles = []

        for field_path, stats in field_stats.items():
            histogram = None
            if (
                self.profiler_config.include_field_histogram
                and stats.distinct_value_frequencies
            ):
                sorted_frequencies = sorted(
                    stats.distinct_value_frequencies.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:10]

                boundaries = [str(value) for value, _ in sorted_frequencies]
                heights = [float(freq) for _, freq in sorted_frequencies]

                histogram = HistogramClass(boundaries=boundaries, heights=heights)

            field_profile = DatasetFieldProfileClass(
                fieldPath=field_path,
                sampleValues=stats.sample_values
                if self.profiler_config.include_field_sample_values
                else None,
                uniqueCount=stats.unique_count
                if self.profiler_config.include_field_distinct_count
                else None,
                uniqueProportion=stats.unique_proportion
                if self.profiler_config.include_field_distinct_count
                else None,
                nullCount=stats.null_count
                if self.profiler_config.include_field_null_count
                else None,
                nullProportion=stats.null_proportion
                if self.profiler_config.include_field_null_count
                else None,
                min=str(stats.min_value)
                if self.profiler_config.include_field_min_value
                and stats.min_value is not None
                else None,
                max=str(stats.max_value)
                if self.profiler_config.include_field_max_value
                and stats.max_value is not None
                else None,
                mean=str(stats.mean_value)
                if self.profiler_config.include_field_mean_value
                and stats.mean_value is not None
                else None,
                median=str(stats.median_value)
                if self.profiler_config.include_field_median_value
                and stats.median_value is not None
                else None,
                stdev=str(stats.stdev)
                if self.profiler_config.include_field_stddev_value
                and hasattr(stats, "stdev")
                else None,
                quantiles=stats.quantiles
                if self.profiler_config.include_field_quantiles
                and hasattr(stats, "quantiles")
                else None,
                distinctValueFrequencies=[
                    ValueFrequencyClass(value=str(value), frequency=freq)
                    for value, freq in sorted(
                        stats.distinct_value_frequencies.items(),
                        key=lambda x: x[1],
                        reverse=True,
                    )[:10]
                ]
                if self.profiler_config.include_field_distinct_value_frequencies
                and stats.distinct_value_frequencies
                else None,
                histogram=histogram
                if self.profiler_config.include_field_histogram
                else None,
            )
            field_profiles.append(field_profile)

        return DatasetProfileClass(
            timestampMillis=timestamp_millis,
            columnCount=len(field_profiles),
            eventGranularity=TimeWindowSizeClass(
                unit=CalendarIntervalClass.SECOND,
                multiple=self.profiler_config.max_sample_time_seconds,
            ),
            # Add partition specification
            partitionSpec=PartitionSpecClass(
                partition=f"SAMPLE ({str(self.profiler_config.sample_size)} samples / {str(self.profiler_config.max_sample_time_seconds)} seconds)",
                type=PartitionTypeClass.QUERY,
            ),
            fieldProfiles=field_profiles,
        )
