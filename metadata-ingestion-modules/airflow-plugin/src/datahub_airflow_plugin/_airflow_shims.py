from airflow.models.baseoperator import BaseOperator

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED

try:
    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.operator import Operator
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    # Operator isn't a real class, but rather a type alias defined
    # as the union of BaseOperator and MappedOperator.
    # Since older versions of Airflow don't have MappedOperator, we can just use BaseOperator.
    Operator = BaseOperator  # type: ignore
    MappedOperator = None  # type: ignore
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

try:
    from airflow.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor  # type: ignore

assert AIRFLOW_PATCHED

__all__ = [
    "Operator",
    "MappedOperator",
    "EmptyOperator",
    "ExternalTaskSensor",
]
