import logging
import math
import random
from datetime import datetime
from typing import Dict, List, Optional, Set, Union

import avro.io
import avro.schema
from pydantic import BaseModel, Field

from datahub.ingestion.source.kafka.kafka_config import ProfilerConfig
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    CalendarIntervalClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    HistogramClass,
    KafkaSchemaClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    PartitionSpecClass,
    PartitionTypeClass,
    QuantileClass,
    RecordTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    TimeWindowSizeClass,
    UnionTypeClass,
    ValueFrequencyClass,
)

logger = logging.getLogger(__name__)

# Type aliases for better type safety
MessageValue = Union[str, int, float, bool, dict, list, None]
FieldValue = Union[str, int, float, bool, None]
NumericValue = Union[int, float]

# Removed unused KafkaProfilerRequest and KafkaDataProfiler classes
# The Kafka source now directly uses KafkaProfiler for simplicity


class KafkaFieldStatistics(BaseModel):
    """Statistics for a single field in a Kafka message sample."""

    field_path: str
    data_type: Optional[str] = None
    sample_values: List[str] = Field(default_factory=list)
    unique_count: int = 0
    unique_proportion: float = 0.0
    null_count: int = 0
    null_proportion: float = 0.0
    min_value: Optional[FieldValue] = None
    max_value: Optional[FieldValue] = None
    mean_value: Optional[float] = None
    median_value: Optional[FieldValue] = None
    stdev: Optional[float] = None
    quantiles: List[QuantileClass] = Field(default_factory=list)
    distinct_value_frequencies: Dict[str, int] = Field(default_factory=dict)

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True  # Allow QuantileClass types

    def __init__(self, **data):
        """Initialize and ensure numeric values are properly typed."""
        super().__init__(**data)
        if isinstance(self.mean_value, str):
            try:
                self.mean_value = float(self.mean_value)
            except (ValueError, TypeError):
                self.mean_value = None


def is_special_value(v: FieldValue) -> bool:
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
    """Clean field path by optionally preserving or removing metadata."""
    if preserve_types:
        # Return full path when preserving types
        return field_path

    # If not preserving types, extract the actual field name
    parts = field_path.split(".")
    # Return last non-empty part that isn't metadata
    for part in reversed(parts):
        if part and not (part.startswith("[version=") or part.startswith("[type=")):
            # Remove [key=True] annotations and return the field name
            clean_part = part.split("[key=")[0]
            return clean_part

    # Fallback: if all parts are metadata, try to extract field name from the end
    # For paths like "[version=2.0].[type=NumericData].[type=long].id"
    if "." in field_path and not field_path.endswith("]"):
        last_part = field_path.split(".")[-1]
        if not last_part.startswith("["):
            return last_part.split("[key=")[0]

    return field_path


def flatten_json(
    nested_json: Union[Dict[str, MessageValue], List[MessageValue]],
    parent_key: str = "",
    flattened_dict: Optional[Dict[str, str]] = None,
    max_depth: int = 10,
    current_depth: int = 0,
    seen_objects: Optional[Set[int]] = None,
) -> Dict[str, str]:
    """Flatten nested JSON with recursion and circular reference protection."""
    if flattened_dict is None:
        flattened_dict = {}

    if seen_objects is None:
        seen_objects = set()

    # Check recursion depth limit
    if current_depth >= max_depth:
        flattened_dict[parent_key or "truncated"] = f"<truncated at depth {max_depth}>"
        return flattened_dict

    # Check for circular references
    obj_id = id(nested_json)
    if obj_id in seen_objects:
        flattened_dict[parent_key or "circular"] = "<circular reference>"
        return flattened_dict

    # Add current object to seen set for circular reference detection
    if isinstance(nested_json, (dict, list)):
        seen_objects.add(obj_id)

    try:
        if isinstance(nested_json, dict):
            # Limit the number of keys processed to prevent excessive expansion
            keys = list(nested_json.keys())[:100]  # Limit to first 100 keys
            for key in keys:
                value = nested_json[key]
                new_key = f"{parent_key}.{key}" if parent_key else key

                if isinstance(value, (dict, list)):
                    flatten_json(
                        {"value": value} if isinstance(value, list) else value,
                        new_key,
                        flattened_dict,
                        max_depth,
                        current_depth + 1,
                        seen_objects.copy(),  # Pass a copy to avoid side effects
                    )
                else:
                    # Convert value to string and limit length to prevent memory issues
                    str_value = str(value)
                    if len(str_value) > 1000:
                        str_value = str_value[:1000] + "..."
                    flattened_dict[new_key] = str_value

        elif isinstance(nested_json, list):
            # Limit array processing to prevent excessive expansion
            items = nested_json[:50]  # Limit to first 50 items
            for i, item in enumerate(items):
                new_key = f"{parent_key}[{i}]"
                if isinstance(item, (dict, list)):
                    flatten_json(
                        {"item": item},
                        new_key,
                        flattened_dict,
                        max_depth,
                        current_depth + 1,
                        seen_objects.copy(),  # Pass a copy to avoid side effects
                    )
                else:
                    # Convert value to string and limit length
                    str_value = str(item)
                    if len(str_value) > 1000:
                        str_value = str_value[:1000] + "..."
                    flattened_dict[new_key] = str_value
        else:
            # Convert value to string and limit length
            str_value = str(nested_json)
            if len(str_value) > 1000:
                str_value = str_value[:1000] + "..."
            flattened_dict[parent_key] = str_value

    finally:
        # Remove current object from seen set when done
        if isinstance(nested_json, (dict, list)) and obj_id in seen_objects:
            seen_objects.discard(obj_id)

    return flattened_dict


class KafkaProfiler:
    """Handles profiling of Kafka message samples using GE-compatible patterns."""

    def __init__(self, profiler_config: ProfilerConfig) -> None:
        """Initialize the Kafka profiler with configuration."""
        self.profiler_config = profiler_config
        self._expensive_profiling_disabled = (
            profiler_config.turn_off_expensive_profiling_metrics
        )
        self._processed_field_count = 0

    @staticmethod
    def profile_topic(
        topic_name: str,
        samples: List[Dict[str, MessageValue]],
        schema_metadata: Optional[SchemaMetadataClass],
        config: ProfilerConfig,
    ) -> Optional[DatasetProfileClass]:
        """Profile a single Kafka topic from message samples. Designed for parallel execution."""
        try:
            if not samples:
                logger.warning(f"No samples available for topic {topic_name}")
                return None

            profiler = KafkaProfiler(config)
            profile = profiler.generate_dataset_profile(samples, schema_metadata)

            if profile:
                # Set partition spec to indicate sample-based profiling
                profile.partitionSpec = PartitionSpecClass(
                    partition=f"SAMPLE ({len(samples)} messages)",
                    type=PartitionTypeClass.QUERY,
                )

            return profile

        except RecursionError as e:
            logger.error(
                f"Maximum recursion depth exceeded while profiling topic {topic_name}. "
                f"This is likely due to deeply nested or circular JSON structures. "
                f"Consider reducing the message complexity or adjusting flatten_json max_depth. Error: {e}"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to profile topic {topic_name}: {e}")
            return None

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

    def _get_sample_values(
        self, values: List[FieldValue], max_samples: int = 20
    ) -> List[str]:
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

    def _create_histogram(
        self, values: List[NumericValue], max_buckets: int = 10
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
    ) -> Dict[str, Union[Dict[str, str], Dict[str, List[FieldValue]]]]:
        """Initialize field mappings from schema metadata."""
        field_mappings: Dict[
            str, Union[Dict[str, str], Dict[str, List[FieldValue]]]
        ] = {
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
                        field_mappings["values"][schema_field.name] = []  # type: ignore[assignment]
            except Exception as e:
                logger.warning(f"Failed to parse key schema: {e}")

        # Map all schema fields
        for schema_field in schema_metadata.fields or []:
            clean_name = clean_field_path(schema_field.fieldPath, preserve_types=False)
            field_mappings["paths"][clean_name] = schema_field.fieldPath
            field_mappings["values"][schema_field.fieldPath] = []  # type: ignore[assignment]

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

        # Handle key prefix specially
        if field_name.startswith("key."):
            base_field_name = field_name[4:]  # Remove "key." prefix
            if schema_metadata and schema_metadata.fields:
                # Look for field with [key=True] annotation and the matching base name
                for schema_field in schema_metadata.fields:
                    if (
                        "[key=True]" in schema_field.fieldPath
                        and clean_field_path(
                            schema_field.fieldPath, preserve_types=False
                        )
                        == base_field_name
                    ):
                        return schema_field.fieldPath

            # If no match found in schema, keep the key prefix
            return field_name

        # Special case for simple key field (not a complex key)
        if field_name == "key" and schema_metadata and schema_metadata.fields:
            # Look for schema fields with [key=True] in fieldPath
            # We should match on the complete field path, not just the field name
            key_schema_fields = [
                schema_field
                for schema_field in schema_metadata.fields
                if "[key=True]" in schema_field.fieldPath
            ]

            # If we found exactly one key field, use its full fieldPath
            if len(key_schema_fields) == 1:
                return key_schema_fields[0].fieldPath
            # If we found multiple key fields, try to find the best match
            elif len(key_schema_fields) > 1:
                # Look for a simple type like long, string, etc.
                for schema_field in key_schema_fields:
                    if "[type=" in schema_field.fieldPath:
                        return schema_field.fieldPath
                # If no simple type found, just use the first one
                return key_schema_fields[0].fieldPath

            return field_name  # No match found, keep as-is

        # Try to find matching schema field
        clean_sample = clean_field_path(field_name, preserve_types=False)
        if schema_metadata and schema_metadata.fields:
            for schema_field in schema_metadata.fields:
                # Skip key fields when looking for regular field matches
                if "[key=True]" in schema_field.fieldPath:
                    continue

                if (
                    clean_field_path(schema_field.fieldPath, preserve_types=False)
                    == clean_sample
                ):
                    return schema_field.fieldPath

        return field_paths.get(clean_sample, field_name)

    def _process_sample(
        self,
        sample: Dict[str, MessageValue],
        schema_metadata: Optional[SchemaMetadataClass],
        field_mappings: Dict[str, Union[Dict[str, str], Dict[str, List[FieldValue]]]],
    ) -> None:
        """Process a single sample and update field values."""
        for field_name, value in sample.items():
            field_path = self._get_field_path(
                field_name,
                schema_metadata,
                field_mappings["paths"],  # type: ignore[arg-type]
            )
            if not field_path:
                continue

            # Add value if it's not a special value
            if not is_special_value(value):  # type: ignore[arg-type]
                if field_path not in field_mappings["values"]:
                    field_mappings["values"][field_path] = []  # type: ignore[assignment]
                field_mappings["values"][field_path].append(value)  # type: ignore[union-attr,arg-type]
            else:
                # Initialize field if needed but don't add special value
                if field_path not in field_mappings["values"]:
                    field_mappings["values"][field_path] = []  # type: ignore[assignment]

    def _calculate_field_stats(
        self,
        field_values: Dict[str, List[FieldValue]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> Dict[str, KafkaFieldStatistics]:
        """Calculate statistics for all fields."""
        field_stats = {}
        for field_path, values in field_values.items():
            if values:  # Only process fields that have values
                try:
                    field_stats[field_path] = self._process_field_statistics(
                        field_path=field_path,
                        values=values,
                        schema_metadata=schema_metadata,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to process statistics for field {field_path}: {e}"
                    )
                    continue
        return field_stats

    def profile_samples(
        self,
        samples: List[Dict[str, MessageValue]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> DatasetProfileClass:
        """Profile samples and return DatasetProfileClass - main entry point for profiling."""
        return self.generate_dataset_profile(samples, schema_metadata)

    def generate_dataset_profile(
        self,
        samples: List[Dict[str, MessageValue]],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> DatasetProfileClass:
        """Analyze samples and create a dataset profile incorporating schema information."""
        try:
            # Initialize field mappings from schema
            field_mappings = self._init_schema_fields(schema_metadata)

            # Process all samples with error handling for each sample
            for i, sample in enumerate(samples):
                try:
                    self._process_sample(sample, schema_metadata, field_mappings)
                except RecursionError as e:
                    logger.warning(
                        f"Skipping sample {i} due to recursion error (likely circular/deep JSON): {e}"
                    )
                    continue
                except Exception as e:
                    logger.warning(f"Error processing sample {i}: {e}")
                    continue

            # Calculate statistics for all fields
            # Cast to the expected type since we know "values" contains the field values
            from typing import cast

            values_dict = cast(Dict[str, List[FieldValue]], field_mappings["values"])
            field_stats = self._calculate_field_stats(values_dict, schema_metadata)

            # Create and return profile
            return self.create_profile_data(field_stats, len(samples))

        except RecursionError as e:
            logger.error(f"Maximum recursion depth exceeded in dataset profiling: {e}")
            # Return a minimal profile if recursion error occurs
            return DatasetProfileClass(
                timestampMillis=int(datetime.now().timestamp() * 1000),
                columnCount=0,
                fieldProfiles=[],
            )
        except Exception as e:
            logger.error(f"Unexpected error in dataset profiling: {e}")
            raise

    def _get_data_type_from_schema_field_type(
        self,
        type_class: Union[
            NumberTypeClass,
            BooleanTypeClass,
            DateTypeClass,
            TimeTypeClass,
            StringTypeClass,
            EnumTypeClass,
            ArrayTypeClass,
            MapTypeClass,
            RecordTypeClass,
            UnionTypeClass,
            BytesTypeClass,
            FixedTypeClass,
            NullTypeClass,
        ],
    ) -> str:
        """Determine data type from SchemaFieldDataType class using isinstance checks.

        Aligned with Great Expectations ProfilerDataType enum:
        NUMERIC, BOOLEAN, DATETIME, STRING, UNKNOWN
        """

        if isinstance(type_class, NumberTypeClass):
            return "NUMERIC"
        elif isinstance(type_class, BooleanTypeClass):
            return "BOOLEAN"
        elif isinstance(type_class, (DateTypeClass, TimeTypeClass)):
            return "DATETIME"  # Aligned with GE ProfilerDataType
        elif isinstance(type_class, (StringTypeClass, EnumTypeClass)):
            return "STRING"
        elif isinstance(
            type_class,
            (
                ArrayTypeClass,
                MapTypeClass,
                RecordTypeClass,
                UnionTypeClass,
                BytesTypeClass,
                FixedTypeClass,
                NullTypeClass,
            ),
        ):
            return "UNKNOWN"  # Complex/unsupported types -> UNKNOWN (aligned with GE)
        else:
            return "UNKNOWN"  # Default fallback aligned with GE

    def _get_data_type_from_native_type(self, native_type: str) -> str:
        """Determine data type from nativeDataType string using set membership.

        Aligned with Great Expectations ProfilerDataType enum:
        NUMERIC, BOOLEAN, DATETIME, STRING, UNKNOWN
        """
        # Using sets for O(1) lookup instead of multiple 'in' checks
        numeric_types = {"long", "int", "double", "float", "decimal"}
        boolean_types = {"boolean", "bool"}
        datetime_types = {
            "date",
            "time-micros",
            "time-millis",
            "timestamp-micros",
            "timestamp-millis",
        }
        string_types = {"string", "enum"}  # Enum treated as string in GE
        # Complex/unsupported types mapped to UNKNOWN
        unknown_types = {"bytes", "fixed", "array", "map", "record", "union", "null"}

        if native_type in numeric_types:
            return "NUMERIC"
        elif native_type in boolean_types:
            return "BOOLEAN"
        elif native_type in datetime_types:
            return "DATETIME"  # Aligned with GE ProfilerDataType
        elif native_type in string_types:
            return "STRING"
        elif native_type in unknown_types:
            return "UNKNOWN"  # Complex/unsupported types -> UNKNOWN (aligned with GE)
        else:
            return "STRING"  # Default fallback for unknown string-like types

    def _process_field_statistics(
        self,
        field_path: str,
        values: List[FieldValue],
        schema_metadata: Optional[SchemaMetadataClass] = None,
    ) -> KafkaFieldStatistics:
        """Calculate statistics for a single field based on profiling config."""
        # Determine data type and filter values
        data_type = self._determine_field_data_type(field_path, values, schema_metadata)
        non_null_values = self._filter_non_null_values(values)

        # Check field limits for expensive profiling
        if self._should_skip_field_processing():
            return self._create_minimal_stats(field_path, data_type)

        # Create base statistics
        stats = self._create_base_field_statistics(
            field_path, data_type, values, non_null_values
        )

        # Add distinct value statistics
        self._add_distinct_value_statistics(stats, values)

        # Add numeric statistics if applicable
        if data_type == "NUMERIC":
            self._add_numeric_field_statistics(stats, non_null_values)

        return stats

    def _determine_field_data_type(
        self,
        field_path: str,
        values: List[FieldValue],
        schema_metadata: Optional[SchemaMetadataClass],
    ) -> str:
        """Determine the data type of a field from schema and sample values."""
        data_type = "STRING"  # Default fallback

        # Try to determine type from schema metadata first
        if schema_metadata and schema_metadata.fields:
            data_type = self._get_data_type_from_schema(field_path, schema_metadata)
            if data_type != "STRING":
                return data_type

        # Fallback to detecting from sample values
        return self._detect_data_type_from_values(values, data_type)

    def _filter_non_null_values(self, values: List[FieldValue]) -> List[FieldValue]:
        """Filter out null, empty, NaN, and infinite values."""
        return [
            v
            for v in values
            if v is not None
            and v != ""
            and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
        ]

    def _get_data_type_from_schema(
        self, field_path: str, schema_metadata: SchemaMetadataClass
    ) -> str:
        """Extract data type from schema metadata."""
        for schema_field in schema_metadata.fields:
            if schema_field.fieldPath == field_path:
                # Check the actual schema field type (more reliable than nativeDataType)
                if schema_field.type and schema_field.type.type:
                    data_type = self._get_data_type_from_schema_field_type(
                        schema_field.type.type
                    )
                    if data_type != "STRING":  # Found a specific type
                        return data_type

                # Fallback to native data type if type class is not available
                native_type = getattr(schema_field, "nativeDataType", "").lower()
                data_type = self._get_data_type_from_native_type(native_type)
                if data_type != "STRING":  # Found a specific type
                    return data_type
        return "STRING"

    def _detect_data_type_from_values(
        self, values: List[FieldValue], default_type: str
    ) -> str:
        """Detect data type from sample values."""
        non_null_values = self._filter_non_null_values(values)
        if not non_null_values:
            return default_type

        sample_value = non_null_values[0]
        if isinstance(sample_value, (int, float)):
            return "NUMERIC"
        elif isinstance(sample_value, bool):
            return "BOOLEAN"
        elif isinstance(sample_value, (dict, list)):
            return "UNKNOWN"  # Complex types -> UNKNOWN (aligned with GE)
        return default_type

    def _should_skip_field_processing(self) -> bool:
        """Check if field processing should be skipped due to limits."""
        if not self._expensive_profiling_disabled:
            return False

        max_fields = (
            getattr(self.profiler_config, "max_number_of_fields_to_profile", 10) or 10
        )
        if hasattr(self, "_processed_field_count"):
            self._processed_field_count += 1
            return self._processed_field_count > max_fields
        else:
            self._processed_field_count = 1
            return False

    def _create_minimal_stats(
        self, field_path: str, data_type: str
    ) -> KafkaFieldStatistics:
        """Create minimal statistics when field limit is exceeded."""
        return KafkaFieldStatistics(
            field_path=field_path,
            sample_values=[],
            data_type=data_type,
        )

    def _create_base_field_statistics(
        self,
        field_path: str,
        data_type: str,
        values: List[FieldValue],
        non_null_values: List[FieldValue],
    ) -> KafkaFieldStatistics:
        """Create base field statistics."""
        total_count = len(values)

        # Get sample values
        sample_values = (
            self._get_sample_values(
                [
                    v
                    for v in non_null_values
                    if not (isinstance(v, float) and math.isnan(v))
                    and not is_special_value(v)
                ],
                max_samples=getattr(
                    self.profiler_config, "field_sample_values_limit", 20
                ),
            )
            if self.profiler_config.include_field_sample_values
            and not self._expensive_profiling_disabled
            else []
        )

        stats = KafkaFieldStatistics(
            field_path=field_path,
            sample_values=sample_values,
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

        return stats

    def _add_distinct_value_statistics(
        self, stats: KafkaFieldStatistics, values: List[FieldValue]
    ) -> None:
        """Add distinct value statistics to the stats object."""
        if not (
            self.profiler_config.include_field_distinct_count
            and not self._expensive_profiling_disabled
        ):
            return

        # Convert to strings only for counting distinct values, skip NaN
        value_counts: Dict[str, int] = {}
        for value in values:
            if not (
                isinstance(value, float) and (math.isnan(value) or math.isinf(value))
            ):
                str_value = str(value)
                value_counts[str_value] = value_counts.get(str_value, 0) + 1

        stats.unique_count = len(value_counts)
        stats.unique_proportion = stats.unique_count / len(values) if values else 0
        stats.distinct_value_frequencies = value_counts

    def _add_numeric_field_statistics(
        self, stats: KafkaFieldStatistics, non_null_values: List[FieldValue]
    ) -> None:
        """Add numeric statistics to the stats object."""
        numeric_values = self._extract_numeric_values(non_null_values)
        if not numeric_values:
            return

        # Calculate basic numeric statistics
        numeric_stats = self._calculate_numeric_stats(numeric_values)
        stats.min_value = numeric_stats["min"]
        stats.max_value = numeric_stats["max"]
        stats.mean_value = numeric_stats["mean"]
        stats.median_value = numeric_stats["median"]
        stats.stdev = numeric_stats["stdev"]

        # Calculate standard deviation (legacy calculation)
        if self.profiler_config.include_field_stddev_value and len(numeric_values) > 1:
            mean = stats.mean_value or 0
            variance = sum((x - mean) ** 2 for x in numeric_values) / (
                len(numeric_values) - 1
            )
            stats.stdev = math.sqrt(variance)

        # Calculate quantiles (skip for expensive profiling)
        if (
            self.profiler_config.include_field_quantiles
            and len(numeric_values) >= 4
            and not self._expensive_profiling_disabled
        ):
            self._add_quantile_statistics(stats, numeric_values)

    def _extract_numeric_values(self, non_null_values: List[FieldValue]) -> List[float]:
        """Extract and convert values to numeric format."""
        numeric_values = []
        for v in non_null_values:
            try:
                # Handle both numeric types and string representations of numbers
                if isinstance(v, (int, float)):
                    num_val = float(v)
                elif isinstance(v, str):
                    num_val = float(v)  # Convert string to float
                else:
                    continue  # Skip non-numeric types

                # Skip invalid float values and special values
                if (
                    math.isnan(num_val)
                    or math.isinf(num_val)
                    or is_special_value(num_val)
                ):
                    continue

                numeric_values.append(num_val)
            except (ValueError, TypeError):
                # Skip values that cannot be converted to numbers
                continue
        return numeric_values

    def _add_quantile_statistics(
        self, stats: KafkaFieldStatistics, numeric_values: List[float]
    ) -> None:
        """Add quantile statistics to the stats object."""
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
                and not self._expensive_profiling_disabled  # Skip for expensive profiling disabled
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
                and not self._expensive_profiling_disabled
                else None,
                histogram=histogram
                if self.profiler_config.include_field_histogram
                and not self._expensive_profiling_disabled
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
