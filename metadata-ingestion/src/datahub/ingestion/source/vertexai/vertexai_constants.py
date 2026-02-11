from typing import Set

from datahub.ingestion.source.common.subtypes import MLAssetSubTypes

# Default actor for metadata operations when no user information is available
DATAHUB_ACTOR = "urn:li:corpuser:datahub"


class ResourceCategory:
    """Container categories for organizing Vertex AI resources."""

    MODELS = "Models"
    TRAINING_JOBS = "Training Jobs"
    DATASETS = "Datasets"
    ENDPOINTS = "Endpoints"
    PIPELINES = "Pipelines"
    EVALUATIONS = "Evaluations"


class VertexAISubTypes:
    """
    Vertex AI resource subtypes used for entity classification in DataHub.

    These are imported from the common subtypes enum but organized here
    for easy reference and maintenance of Vertex AI-specific subtypes.
    """

    # ML Resources
    MODEL = MLAssetSubTypes.VERTEX_MODEL
    MODEL_GROUP = MLAssetSubTypes.VERTEX_MODEL_GROUP
    MODEL_EVALUATION = MLAssetSubTypes.VERTEX_MODEL_EVALUATION
    ENDPOINT = MLAssetSubTypes.VERTEX_ENDPOINT

    # Training & Experiments
    TRAINING_JOB = MLAssetSubTypes.VERTEX_TRAINING_JOB
    EXPERIMENT = MLAssetSubTypes.VERTEX_EXPERIMENT
    EXPERIMENT_RUN = MLAssetSubTypes.VERTEX_EXPERIMENT_RUN
    EXECUTION = MLAssetSubTypes.VERTEX_EXECUTION

    # Pipelines
    PIPELINE = MLAssetSubTypes.VERTEX_PIPELINE
    PIPELINE_TASK = MLAssetSubTypes.VERTEX_PIPELINE_TASK
    PIPELINE_TASK_RUN = MLAssetSubTypes.VERTEX_PIPELINE_TASK_RUN

    # Data & Infra
    DATASET = MLAssetSubTypes.VERTEX_DATASET
    PROJECT = MLAssetSubTypes.VERTEX_PROJECT


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
        "random_state",
        "seed",
        "n_jobs",
        "validation_split",
        "test_split",
        "train_size",
        "test_size",
    }

    PREFIXES: Set[str] = {
        "param_",
        "hp_",
        "hyperparameter_",
        "hyper_param_",
        "hyperparam_",
    }

    @classmethod
    def is_hyperparam(cls, key: str) -> bool:
        key_lower = key.lower()
        if key_lower in cls.EXACT_MATCHES:
            return True
        return any(key_lower.startswith(prefix) for prefix in cls.PREFIXES)


class MetricPatterns:
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
    GCS_PREFIX = "gs://"
    BQ_PREFIX = "bq://"
    DATASET_PATH_PATTERN = "/datasets/"
    TABLE_PATH_PATTERN = "/tables/"

    @classmethod
    def looks_like_uri(cls, s: str) -> bool:
        return (
            s.startswith(cls.GCS_PREFIX)
            or s.startswith(cls.BQ_PREFIX)
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
    MAX_EXECUTION_SEARCH_RESULTS = 100
    METADATA_STORE_PATH_TEMPLATE = (
        "projects/{project_id}/locations/{region}/metadataStores/{metadata_store}"
    )


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
