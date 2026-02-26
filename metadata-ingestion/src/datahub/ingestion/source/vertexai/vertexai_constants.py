import re
from typing import Literal, Set

# Platform identifier
PLATFORM: Literal["vertexai"] = "vertexai"

# Progress logging configuration
PROGRESS_LOG_INTERVAL = 100

# Kubeflow Pipelines timestamp suffix pattern (appended to display_name for each run)
# Example: "my-pipeline-20241107083959" -> extract "my-pipeline"
KUBEFLOW_TIMESTAMP_SUFFIX_PATTERN = re.compile(r"-\d{14}$")

# Field names for object attributes
UPDATE_TIME_FIELD = "update_time"
CREATE_TIME_FIELD = "create_time"
VERSION_ID_FIELD = "version_id"

# Model version sorting configuration
# Padding for version IDs to ensure correct string-based sorting (supports up to 9,999,999,999)
VERSION_SORT_ID_PADDING = 10

# API ordering for incremental ingestion
ORDER_BY_UPDATE_TIME_DESC = f"{UPDATE_TIME_FIELD} desc"
ORDER_BY_CREATE_TIME_DESC = f"{CREATE_TIME_FIELD} desc"


# Resource type names for state tracking and logging
class ResourceTypes:
    MODEL = "Model"
    MODEL_EVALUATION = "ModelEvaluation"
    PIPELINE_JOB = "PipelineJob"
    EXPERIMENT = "Experiment"
    EXPERIMENT_RUN = "ExperimentRun"
    CUSTOM_JOB = "CustomJob"
    BATCH_PREDICTION_JOB = "BatchPredictionJob"
    HYPERPARAMETER_TUNING_JOB = "HyperparameterTuningJob"


# Type for external data platforms referenced in Vertex AI lineage
ExternalPlatform = Literal["gcs", "bigquery", "s3", "abs", "snowflake"]


class ExternalPlatforms:
    """
    Constants for external data platforms that may be referenced in Vertex AI lineage.
    Used for platform_instance_map configuration.

    Supported Platforms:
    - GCS: Google Cloud Storage (gs:// URIs)
    - BIGQUERY: BigQuery tables (bq:// or projects/.../datasets/.../tables/... URIs)
    - S3: Amazon S3 (s3://, s3a:// URIs)
    - AZURE_BLOB_STORAGE: Azure Blob Storage (wasbs://, abfss:// URIs)
    - SNOWFLAKE: Snowflake tables (snowflake:// URIs or table references)
    """

    GCS: ExternalPlatform = "gcs"
    BIGQUERY: ExternalPlatform = "bigquery"
    S3: ExternalPlatform = "s3"
    AZURE_BLOB_STORAGE: ExternalPlatform = "abs"
    SNOWFLAKE: ExternalPlatform = "snowflake"


# Type for resource category folder names
ResourceCategoryType = Literal[
    "Models",
    "Training Jobs",
    "Datasets",
    "Endpoints",
    "Pipelines",
    "Experiments",
    "Evaluations",
]


class ResourceCategory:
    """Container categories for organizing Vertex AI resources."""

    MODELS: ResourceCategoryType = "Models"
    TRAINING_JOBS: ResourceCategoryType = "Training Jobs"
    DATASETS: ResourceCategoryType = "Datasets"
    ENDPOINTS: ResourceCategoryType = "Endpoints"
    PIPELINES: ResourceCategoryType = "Pipelines"
    EXPERIMENTS: ResourceCategoryType = "Experiments"
    EVALUATIONS: ResourceCategoryType = "Evaluations"


class MLMetadataSchemas:
    # Artifact schemas
    DATASET = "system.Dataset"
    MODEL = "system.Model"
    ARTIFACT = "system.Artifact"
    METRICS = "system.Metrics"

    # Execution schemas
    CONTAINER_EXECUTION = "system.ContainerExecution"
    RUN = "system.Run"
    CUSTOM_JOB = "system.CustomJob"

    # Context schemas
    EXPERIMENT = "system.Experiment"
    PIPELINE_RUN = "system.PipelineRun"


class MLMetadataEventTypes:
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    DECLARED_INPUT = "DECLARED_INPUT"
    DECLARED_OUTPUT = "DECLARED_OUTPUT"


class HyperparameterPatterns:
    # Heuristic lists derived from common ML framework naming conventions
    # (scikit-learn, Keras/TensorFlow, PyTorch, XGBoost). No single authoritative
    # source; extend as needed for frameworks not covered here.
    EXACT_MATCHES: Set[str] = {
        "learning_rate",
        "lr",
        "batch_size",
        "epochs",
        "num_epochs",
        "optimizer",
        "dropout",
        "dropout_rate",
        "num_layers",
        "hidden_units",
        "hidden_size",
        "num_hidden_layers",
        "max_depth",
        "min_samples_split",
        "min_samples_leaf",
        "n_estimators",
        "alpha",
        "beta",
        "gamma",
        "momentum",
        "weight_decay",
        "l1_ratio",
        "l2_ratio",
        "regularization",
        "max_iter",
        "max_iterations",
        "tolerance",
        "tol",
    }

    PREFIXES: Set[str] = {
        "param_",
        "hp_",
    }

    @classmethod
    def is_hyperparam(cls, key: str) -> bool:
        key_lower = key.lower()
        if key_lower in cls.EXACT_MATCHES:
            return True
        return any(key_lower.startswith(prefix) for prefix in cls.PREFIXES)


class MetricPatterns:
    # Heuristic lists derived from common ML framework naming conventions
    # (scikit-learn, Keras/TensorFlow, PyTorch, XGBoost). No single authoritative
    # source; extend as needed for frameworks not covered here.
    EXACT_MATCHES: Set[str] = {
        # Classification
        "accuracy",
        "precision",
        "recall",
        "f1",
        "f1_score",
        "auc",
        "roc_auc",
        "pr_auc",
        "log_loss",
        "binary_crossentropy",
        "categorical_crossentropy",
        # Regression
        "mse",
        "rmse",
        "mae",
        "mape",
        "r2",
        "r2_score",
        "mean_squared_error",
        "mean_absolute_error",
        "root_mean_squared_error",
        # Training/validation
        "loss",
        "val_loss",
        "train_loss",
        "val_accuracy",
        "train_accuracy",
        "validation_accuracy",
        "training_accuracy",
        "val_precision",
        "train_precision",
        "val_recall",
        "train_recall",
        "val_f1",
        "train_f1",
        "val_auc",
        "train_auc",
    }

    SUFFIXES: Set[str] = {
        "_score",
        "_metric",
        "_loss",
        "_accuracy",
        "_error",
        "_auc",
        "_precision",
        "_recall",
        "_f1",
    }

    PREFIXES: Set[str] = {
        "metric_",
        "eval_",
        "evaluation_",
        "test_",
        "valid_",
        "val_",
        "train_",
        "training_",
    }

    @classmethod
    def is_metric(cls, key: str) -> bool:
        key_lower = key.lower()
        if key_lower in cls.EXACT_MATCHES:
            return True
        if any(key_lower.endswith(suffix) for suffix in cls.SUFFIXES):
            return True
        return any(key_lower.startswith(prefix) for prefix in cls.PREFIXES)


class URIPatterns:
    """
    URI patterns for data sources referenced in Vertex AI.

    Supported URI Formats:
    - GCS: gs://bucket/path
    - BigQuery: bq://project.dataset.table or projects/.../datasets/.../tables/...
    - S3: s3://bucket/key or s3a://bucket/key
    - Azure Blob: wasbs://container@account.blob.core.windows.net/path
    - Azure Data Lake: abfss://container@account.dfs.core.windows.net/path
    - Snowflake: snowflake://account/database/schema/table
    """

    # Cloud storage prefixes
    GCS_PREFIX = "gs://"
    S3_PREFIX = "s3://"
    S3A_PREFIX = "s3a://"
    S3N_PREFIX = "s3n://"
    AZURE_WASB_PREFIX = "wasbs://"
    AZURE_ABFS_PREFIX = "abfss://"

    # Grouped prefixes for multi-scheme platforms
    S3_PREFIXES = (S3_PREFIX, S3A_PREFIX, S3N_PREFIX)
    ABS_PREFIXES = (AZURE_WASB_PREFIX, AZURE_ABFS_PREFIX)

    # Data warehouse prefixes
    BQ_PREFIX = "bq://"
    SNOWFLAKE_PREFIX = "snowflake://"

    # Path patterns for full URIs
    DATASET_PATH_PATTERN = "/datasets/"
    TABLE_PATH_PATTERN = "/tables/"
    MODEL_PATH_PATTERN = "/models/"

    # Resource path components (used in URI parsing)
    PROJECTS_PREFIX = "projects/"
    PROJECTS_COMPONENT = "projects"
    DATASETS_COMPONENT = "datasets"
    TABLES_COMPONENT = "tables"
    MODELS_COMPONENT = "models"

    # URI direction classifications
    CLASSIFICATION_INPUT = "input"
    CLASSIFICATION_OUTPUT = "output"

    @classmethod
    def looks_like_uri(cls, s: str) -> bool:
        """Check if string looks like a supported URI pattern."""
        return (
            s.startswith(cls.GCS_PREFIX)
            or s.startswith(cls.BQ_PREFIX)
            or s.startswith(cls.S3_PREFIX)
            or s.startswith(cls.S3A_PREFIX)
            or s.startswith(cls.AZURE_WASB_PREFIX)
            or s.startswith(cls.AZURE_ABFS_PREFIX)
            or s.startswith(cls.SNOWFLAKE_PREFIX)
            or cls.DATASET_PATH_PATTERN in s
        )

    @classmethod
    def is_input_like(cls, key_path: str) -> bool:
        key_lower = key_path.lower()
        return any(
            tok in key_lower
            for tok in ["input", "source", "training_data", "train_data"]
        )

    @classmethod
    def is_output_like(cls, key_path: str) -> bool:
        key_lower = key_path.lower()
        return any(
            tok in key_lower
            for tok in ["output", "destination", "sink", "model", "artifact"]
        )


class MLMetadataDefaults:
    DEFAULT_METADATA_STORE = "default"
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 100  # API limitation
    MAX_EXECUTION_SEARCH_RESULTS = 500
    METADATA_STORE_PATH_TEMPLATE = (
        "projects/{project_id}/locations/{region}/metadataStores/{metadata_store}"
    )

    RETRY_INITIAL_WAIT_SECS = 10.0
    RETRY_MAXIMUM_WAIT_SECS = 60.0
    RETRY_MULTIPLIER = 2.0
    RETRY_DEADLINE_SECS = 120.0


class IngestionLimits:
    """Safety limits for ingestion to prevent resource exhaustion."""

    # Per-resource-type limits
    DEFAULT_MAX_MODELS = 10000
    DEFAULT_MAX_TRAINING_JOBS_PER_TYPE = 1000
    DEFAULT_MAX_EXPERIMENTS = 1000
    DEFAULT_MAX_RUNS_PER_EXPERIMENT = 100
    DEFAULT_MAX_EVALUATIONS_PER_MODEL = 10

    # Absolute maximum values (hard caps)
    ABSOLUTE_MAX_MODELS = 50000
    ABSOLUTE_MAX_TRAINING_JOBS_PER_TYPE = 10000
    ABSOLUTE_MAX_EXPERIMENTS = 10000
    ABSOLUTE_MAX_RUNS_PER_EXPERIMENT = 1000
    ABSOLUTE_MAX_EVALUATIONS_PER_MODEL = 100


# Duration formatting constants
class DurationUnit:
    """Time duration unit suffixes for formatted output."""

    DAYS = "d"
    HOURS = "h"
    MINUTES = "m"
    SECONDS = "s"
    MILLISECONDS = "ms"


# Date/time format strings
class DateTimeFormat:
    """Standard date/time format strings used across the connector."""

    TIMESTAMP = "%Y-%m-%d %H:%M:%S"


# Label formatting constants
class LabelFormat:
    """Constants for formatting labels and metadata."""

    KEY_VALUE_SEPARATOR = ":"
    ITEM_SEPARATOR = ","


# External URL constants
class ExternalURLs:
    """URL templates for Vertex AI Console links."""

    # Base URL for all Vertex AI console pages
    BASE_URL = "https://console.cloud.google.com/vertex-ai"

    # URL path templates (use .format() to populate placeholders)
    MODEL = "/models/locations/{region}/models/{model_name}?project={project_id}"
    MODEL_VERSION = "/models/locations/{region}/models/{model_name}/versions/{version_id}?project={project_id}"
    TRAINING_JOB = "/training/training-pipelines?trainingPipelineId={job_name}?project={project_id}"
    EXPERIMENT = "/experiments/locations/{region}/experiments/{experiment_name}/runs?project={project_id}"
    EXPERIMENT_RUN = "/experiments/locations/{region}/experiments/{experiment_name}/runs/{run_name}/charts?project={project_id}"
    EXPERIMENT_ARTIFACTS = "/experiments/locations/{region}/experiments/{experiment_name}/runs/{run_name}/artifacts?project={project_id}"
    PIPELINE = "/pipelines/locations/{region}/runs/{pipeline_name}?project={project_id}"


# ML Model type constants
class MLModelType:
    """Constants for ML model type classifications."""

    ML_MODEL = "ML Model"


class TrainingJobTypes:
    """Vertex AI training job class names."""

    CUSTOM_JOB = "CustomJob"
    CUSTOM_TRAINING_JOB = "CustomTrainingJob"
    CUSTOM_CONTAINER_TRAINING_JOB = "CustomContainerTrainingJob"
    CUSTOM_PYTHON_PACKAGE_TRAINING_JOB = "CustomPythonPackageTrainingJob"
    AUTOML_TABULAR_TRAINING_JOB = "AutoMLTabularTrainingJob"
    AUTOML_TEXT_TRAINING_JOB = "AutoMLTextTrainingJob"
    AUTOML_IMAGE_TRAINING_JOB = "AutoMLImageTrainingJob"
    AUTOML_VIDEO_TRAINING_JOB = "AutoMLVideoTrainingJob"
    AUTOML_FORECASTING_TRAINING_JOB = "AutoMLForecastingTrainingJob"

    @classmethod
    def all(cls) -> list[str]:
        """Return all training job type names."""
        return [
            cls.CUSTOM_JOB,
            cls.CUSTOM_TRAINING_JOB,
            cls.CUSTOM_CONTAINER_TRAINING_JOB,
            cls.CUSTOM_PYTHON_PACKAGE_TRAINING_JOB,
            cls.AUTOML_TABULAR_TRAINING_JOB,
            cls.AUTOML_TEXT_TRAINING_JOB,
            cls.AUTOML_IMAGE_TRAINING_JOB,
            cls.AUTOML_VIDEO_TRAINING_JOB,
            cls.AUTOML_FORECASTING_TRAINING_JOB,
        ]


class DatasetTypes:
    """Vertex AI dataset class names."""

    TEXT_DATASET = "TextDataset"
    TABULAR_DATASET = "TabularDataset"
    IMAGE_DATASET = "ImageDataset"
    TIME_SERIES_DATASET = "TimeSeriesDataset"
    VIDEO_DATASET = "VideoDataset"

    @classmethod
    def all(cls) -> list[str]:
        """Return all dataset type names."""
        return [
            cls.TEXT_DATASET,
            cls.TABULAR_DATASET,
            cls.IMAGE_DATASET,
            cls.TIME_SERIES_DATASET,
            cls.VIDEO_DATASET,
        ]
