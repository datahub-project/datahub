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
        # Ensure default values are properly initialized
        if self.distinct_value_frequencies is None:
            self.distinct_value_frequencies = {}
        if self.quantiles is None:
            self.quantiles = []
        if self.sample_values is None:
            self.sample_values = []
        # Ensure numeric values are properly typed
        if isinstance(self.mean_value, str):
            try:
                self.mean_value = float(self.mean_value)
            except (ValueError, TypeError):
                self.mean_value = None


def is_special_value(v: Any) -> bool:
    """Check if value is a special case that should be treated as null"""
    if isinstance(v, (int, float)):
        # Handle Java Long MIN/MAX values
        if abs(v) > 9.223372036854775e18:
            return True
        # Handle other special cases like -1 or Integer.MAX_VALUE
        if v in (-1, 2147483647):
            return True
    return False


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

    def _calculate_numeric_stats(
        self, numeric_values: List[float]
    ) -> Dict[str, Optional[float]]:
        """Calculate numeric statistics with validation"""
        stats: Dict[str, Optional[float]] = {
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "stdev": None,
        }
        try:
            if not numeric_values:
                return stats

            min_val: float = min(numeric_values)
            max_val: float = max(numeric_values)
            stats["min"] = min_val
            stats["max"] = max_val

            if any(abs(x) > 1e200 for x in numeric_values):
                return stats

            mean_val: float = sum(numeric_values) / len(numeric_values)
            stats["mean"] = mean_val

            if len(numeric_values) > 1:
                sorted_values = sorted(numeric_values)
                stats["median"] = sorted_values[len(sorted_values) // 2]

                variance = sum((x - mean_val) ** 2 for x in numeric_values) / (
                    len(numeric_values) - 1
                )
                if variance >= 0:
                    stats["stdev"] = math.sqrt(variance)

        except Exception as e:
            logger.warning(f"Error calculating numeric stats: {e}")

        return stats

    def _get_sample_values(self, values: List[Any], max_samples: int = 20) -> List[str]:
        if not values:
            return []

        try:
            # Take a random sample up to max_samples size
            sample_size = min(max_samples, len(values))
            indices = sorted(random.sample(range(len(values)), sample_size))
            samples = [str(values[i]) for i in indices]
            samples = [s[:1000] for s in samples]  # Limit string length
            return samples
        except Exception:
            return [str(values[0])]

    def _validate_field_type(
        self,
        field_path: str,
        value: Any,
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> bool:
        """Validate value matches schema field type"""
        if not schema_metadata or not schema_metadata.fields:
            return True

        for field in schema_metadata.fields:
            if field.fieldPath == field_path:
                field_type = field.type.type
                # Add type validation based on schema type
                if "NumberType" in str(field_type):
                    return isinstance(value, (int, float))
                if "StringType" in str(field_type):
                    return isinstance(value, str)
                if "BooleanType" in str(field_type):
                    return isinstance(value, bool)
        return True

    def _create_histogram(
        self, values: List[Any], max_buckets: int = 10
    ) -> Optional[HistogramClass]:
        """Create histogram with validation"""
        try:
            if not values or len(values) < 2:
                return None

            # Only create histograms for numeric values
            numeric_values = [
                float(v)
                for v in values
                if isinstance(v, (int, float))
                and not math.isnan(v)
                and not math.isinf(v)
                and abs(v) < 1e200
            ]

            if len(numeric_values) < 2:
                return None

            # Create buckets
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            if min_val == max_val:
                return None

            bucket_size = (max_val - min_val) / max_buckets
            buckets = [min_val + i * bucket_size for i in range(max_buckets + 1)]
            counts = [0] * max_buckets

            # Count values in buckets
            for val in numeric_values:
                bucket = int((val - min_val) / bucket_size)
                if bucket >= max_buckets:
                    bucket = max_buckets - 1
                counts[bucket] += 1

            return HistogramClass(
                boundaries=[str(b) for b in buckets],
                heights=[float(c) / len(numeric_values) for c in counts],
            )
        except Exception as e:
            logger.warning(f"Error creating histogram: {e}")
            return None

    def _init_schema_fields(
        self, schema_metadata: Optional[SchemaMetadataClass]
    ) -> Dict[str, Dict[str, List[Any]]]:
        """Initialize field mappings from schema metadata."""
        field_mappings: Dict[str, Dict[str, Any]] = {
            "paths": {},
            "values": {},
        }  # Maps clean names to full paths and values

        if not schema_metadata or not isinstance(
            schema_metadata.platformSchema, KafkaSchemaClass
        ):
            return field_mappings

        # Handle key schema if present
        if schema_metadata.platformSchema.keySchema:
            try:
                key_schema = avro.schema.parse(schema_metadata.platformSchema.keySchema)
                if hasattr(key_schema, "fields"):
                    for schema_field in key_schema.fields:
                        clean_name = clean_field_path(
                            schema_field.name, preserve_types=False
                        )
                        field_mappings["paths"][clean_name] = schema_field.name
                        field_mappings["values"][schema_field.name] = []
            except Exception as e:
                logger.warning(f"Failed to parse key schema: {e}")

        # Map all schema fields
        for schema_field in schema_metadata.fields or []:
            clean_name = clean_field_path(schema_field.fieldPath, preserve_types=False)
            field_mappings["paths"][clean_name] = schema_field.fieldPath
            field_mappings["values"][schema_field.fieldPath] = []

        return field_mappings

    def _get_field_path(
        self,
        field_name: str,
        schema_metadata: Optional[SchemaMetadataClass],
        field_paths: Dict[str, str],
    ) -> Optional[str]:
        """Determine the field path for a given field name."""
        if field_name in ("offset", "timestamp"):
            return None

        # Handle key fields specially
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
            return key_field.fieldPath

        # Try to find matching schema field
        clean_sample = clean_field_path(field_name, preserve_types=False)
        if schema_metadata and schema_metadata.fields:
            for schema_field in schema_metadata.fields:
                if (
                    clean_field_path(schema_field.fieldPath, preserve_types=False)
                    == clean_sample
                ):
                    return schema_field.fieldPath

        return field_paths.get(clean_sample, field_name)

    def _process_sample(
        self,
        sample: Dict[str, Any],
        schema_metadata: Optional[SchemaMetadataClass],
        field_mappings: Dict[str, Dict[str, Any]],
    ) -> None:
        """Process a single sample and update field values."""
        for field_name, value in sample.items():
            field_path = self._get_field_path(
                field_name, schema_metadata, field_mappings["paths"]
            )
            if not field_path:
                continue

            # Validate value before adding
            if self._validate_field_type(
                field_path, value, schema_metadata
            ) and not is_special_value(value):
                if field_path not in field_mappings["values"]:
                    field_mappings["values"][field_path] = []
                field_mappings["values"][field_path].append(value)
            else:
                # Initialize field if needed but don't add invalid value
                if field_path not in field_mappings["values"]:
                    field_mappings["values"][field_path] = []

    def _calculate_field_stats(
        self, field_values: Dict[str, List[Any]]
    ) -> Dict[str, KafkaFieldStatistics]:
        """Calculate statistics for all fields."""
        field_stats = {}
        for field_path, values in field_values.items():
            if values:  # Only process fields that have values
                try:
                    field_stats[field_path] = self._process_field_statistics(
                        field_path=field_path,
                        values=values,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to process statistics for field {field_path}: {e}"
                    )
                    continue
        return field_stats

    def profile_samples(
        self,
        samples: List[Dict[str, Any]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> DatasetProfileClass:
        """Analyze samples and create a dataset profile incorporating schema information."""
        # Initialize field mappings from schema
        field_mappings = self._init_schema_fields(schema_metadata)

        # Process all samples
        for sample in samples:
            self._process_sample(sample, schema_metadata, field_mappings)

        # Calculate statistics for all fields
        field_stats = self._calculate_field_stats(field_mappings["values"])

        # Create and return profile
        return self.create_profile_data(field_stats, len(samples))

    def _process_field_statistics(
        self,
        field_path: str,
        values: List[Any],
    ) -> KafkaFieldStatistics:
        """Calculate statistics for a single field based on profiling config"""
        total_count = len(values)
        # Add NaN check in non_null_values filter
        non_null_values = [
            v
            for v in values
            if v is not None
            and v != ""
            and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
        ]

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
            sample_values=self._get_sample_values(
                [
                    v
                    for v in non_null_values
                    if not (isinstance(v, float) and math.isnan(v))
                    and not is_special_value(v)
                ],
            )
            if self.profiler_config.include_field_sample_values
            else [],
            data_type=data_type,
        )

        # Calculate null statistics - treat NaN as null
        stats.null_count = sum(
            1
            for v in values
            if v is None
            or v == ""
            or (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
        )
        stats.null_proportion = stats.null_count / total_count if total_count > 0 else 0

        # Calculate distinct value stats
        if self.profiler_config.include_field_distinct_count:
            # Convert to strings only for counting distinct values, skip NaN
            value_counts: Dict[str, int] = {}
            for value in values:
                if not (
                    isinstance(value, float)
                    and (math.isnan(value) or math.isinf(value))
                ):
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
                float(v)
                for v in non_null_values
                if isinstance(v, (int, float))
                and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
                and not is_special_value(v)  # Add special value check
            ]
            if numeric_values:
                numeric_stats = self._calculate_numeric_stats(numeric_values)
                stats.min_value = numeric_stats["min"]
                stats.max_value = numeric_stats["max"]
                stats.mean_value = numeric_stats["mean"]
                stats.median_value = numeric_stats["median"]
                stats.stdev = numeric_stats["stdev"]

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
                if (
                    self.profiler_config.include_field_quantiles
                    and len(numeric_values) >= 4
                ):
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
                and stats.data_type
                == "NUMERIC"  # Only create histograms for numeric fields
            ):
                # Get values from distinct_value_frequencies
                values = []
                if stats.distinct_value_frequencies:
                    for value_str, freq in stats.distinct_value_frequencies.items():
                        try:
                            value = float(value_str)
                            if (
                                not math.isnan(value)
                                and not math.isinf(value)
                                and not is_special_value(value)
                            ):
                                values.extend([value] * freq)
                        except ValueError:
                            continue

                histogram = self._create_histogram(values)

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
                partition=f"SAMPLE ({str(sample_count)} samples)",
                type=PartitionTypeClass.QUERY,
            ),
            fieldProfiles=field_profiles,
        )
