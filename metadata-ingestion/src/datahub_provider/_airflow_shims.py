from datahub_provider._airflow_compat import AIRFLOW_PATCHED

from airflow.models.baseoperator import BaseOperator

try:
    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.operator import Operator
except ModuleNotFoundError:
    Operator = BaseOperator  # type: ignore
    MappedOperator = None  # type: ignore

try:
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor  # type: ignore

assert AIRFLOW_PATCHED

__all__ = [
    "Operator",
    "MappedOperator",
    "ExternalTaskSensor",
]
