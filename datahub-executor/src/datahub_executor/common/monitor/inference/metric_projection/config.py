from typing import ClassVar, Dict, List, TypeAlias, Union

from datahub.configuration.common import ConfigModel
from pydantic import Field, StrictBool, StrictFloat, StrictInt

# Tricky: we need these to be the strict variants to ensure that pydantic
# doesn't coerce them incorrectly.
_Param: TypeAlias = Union[StrictFloat, StrictInt, StrictBool]


class MetricProjectorConfig(ConfigModel):
    COMPUTE_CONFIDENCE_SCORE: ClassVar[bool] = False
    SPLIT_RATIO: ClassVar[float] = (
        0.7  # Fraction of total data used for training during hyperparameter tuning. (0.7 - 0.8)
    )
    TUNE_HYPERPARAMETERS: ClassVar[bool] = True
    MIN_DATA_LENGTH: ClassVar[int] = (
        4  # Minimum number of samples required to get the predictions. (>4)
    )
    MIN_SAMPLES_FOR_TUNING: ClassVar[int] = (
        10  # Minimum number of samples required to consider hyperparameter tuning. (>10)
    )
    DEFAULT_PARAMS: Dict[str, _Param] = Field(
        default_factory=lambda: {
            "daily_seasonality": False,
            "weekly_seasonality": False,
            "interval_width": 1,
            "changepoint_range": 1,
            "changepoint_prior_scale": 0.01,
            "seasonality_prior_scale": 0.01,
        }
    )
    PARAM_GRID: Dict[str, List[_Param]] = Field(
        default_factory=lambda: {
            "daily_seasonality": [False],
            "weekly_seasonality": [True],
            "interval_width": [1],
            "changepoint_range": [1],
            "changepoint_prior_scale": [0.001, 0.01, 0.1],
            # "seasonality_prior_scale": [0.01, 1.0, 10.0],
        }
    )
    VALID_INTERVALS: ClassVar[List[str]] = ["H", "D", "W", "M"]
    INTERVAL_THRESHOLDS: ClassVar[Dict[str, float]] = {
        "30T": 0.2,
        "H": 0.2,
        "D": 0.5,
        "W": 0.5,
        "M": 0.6,
        "6M": 0.5,
    }
    DAYS_OF_WEEK: ClassVar[List[str]] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    POST_PROCESS_INTERVAL_PAIRS: ClassVar[List[str]] = [
        "HH",
        "DH",
        "WH",
        "MH",
        "DD",
        "WD",
        "MD",
        "WW",
        "MW",
        "MM",
    ]
    POST_PROCESSING: ClassVar[bool] = True
    SCORING_METRIC: ClassVar[str] = "mape"
    OUTLIER_COEFFICIENT: ClassVar[float] = 2.0  # (1.5 - 2.5)
    POST_PROCESSING_SAMPLE_COUNT_THRESHOLD: ClassVar[int] = 20
    WEEKLY_SEASONALITY_SAMPLE_COUNT_THRESHOLD: ClassVar[int] = 21
    USE_PARALLELIZATION: ClassVar[bool] = True
    CPU_COUNT: ClassVar[int] = -1
    BUFFER_FACTOR: ClassVar[float] = (
        0.25  # Fraction of the standard deviation to be added/subtracted as buffer in the predictions. (0.2-0.3)
    )
    REMOVE_ANOMALIES: ClassVar[bool] = True
    OUTLIER_COEFFICIENT_FOR_SPIKES_DETECTION: ClassVar[int] = (
        20  # To detect the extreme anomalies in data preprocessing step. (15-35)
    )
    DELTA_DIFFERENCE_FOR_SPIKES: ClassVar[float] = (
        0.2  # Allowed difference in the deltas corresponding to the spike anomalies. (0.1-0.5)
    )
