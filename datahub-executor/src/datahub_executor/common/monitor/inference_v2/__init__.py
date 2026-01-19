"""
Inference V2 Module - Observe-Models Integration

This module provides V2 trainers that leverage the observe-models
package for preprocessing, forecasting, and anomaly detection.

Enable via environment variable: DATAHUB_USE_OBSERVE_MODELS=true

Note: The datahub_observe package is an optional dependency. If not installed,
the V2 trainers will not be available and USE_OBSERVE_MODELS will be False.
"""

from datahub_executor.common.monitor.inference_v2.config import (
    OBSERVE_MODELS_AVAILABLE,
    should_use_observe_models,
)

__all__ = [
    "should_use_observe_models",
    "OBSERVE_MODELS_AVAILABLE",
]

# Only export V2 trainers if datahub_observe is available.
# This prevents import errors in slim builds without datahub_observe.
if OBSERVE_MODELS_AVAILABLE:
    from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import (
        BaseTrainerV2,  # noqa: F401
    )
    from datahub_executor.common.monitor.inference_v2.field_trainer_v2 import (
        FieldTrainerV2,  # noqa: F401
    )
    from datahub_executor.common.monitor.inference_v2.sql_trainer_v2 import (
        SqlTrainerV2,  # noqa: F401
    )
    from datahub_executor.common.monitor.inference_v2.volume_trainer_v2 import (
        VolumeTrainerV2,  # noqa: F401
    )

    __all__.extend(
        [
            "BaseTrainerV2",
            "FieldTrainerV2",
            "SqlTrainerV2",
            "VolumeTrainerV2",
        ]
    )
