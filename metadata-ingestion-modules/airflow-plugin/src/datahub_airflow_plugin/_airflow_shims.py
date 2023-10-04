from typing import List

import airflow.version
import packaging.version
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

# Approach suggested by https://stackoverflow.com/a/11887885/5004662.
AIRFLOW_VERSION = packaging.version.parse(airflow.version.version)
HAS_AIRFLOW_STANDALONE_CMD = AIRFLOW_VERSION >= packaging.version.parse("2.2.0.dev0")
HAS_AIRFLOW_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.3.0.dev0")
HAS_AIRFLOW_DAG_LISTENER_API = AIRFLOW_VERSION >= packaging.version.parse("2.5.0.dev0")


def get_task_inlets(operator: "Operator") -> List:
    # From Airflow 2.4 _inlets is dropped and inlets used consistently. Earlier it was not the case, so we have to stick there to _inlets
    if hasattr(operator, "_inlets"):
        return operator._inlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_inlet_defs"):
        return operator.get_inlet_defs()  # type: ignore[attr-defined]
    return operator.inlets


def get_task_outlets(operator: "Operator") -> List:
    # From Airflow 2.4 _outlets is dropped and inlets used consistently. Earlier it was not the case, so we have to stick there to _outlets
    # We have to use _outlets because outlets is empty in Airflow < 2.4.0
    if hasattr(operator, "_outlets"):
        return operator._outlets  # type: ignore[attr-defined, union-attr]
    if hasattr(operator, "get_outlet_defs"):
        return operator.get_outlet_defs()
    return operator.outlets


__all__ = [
    "AIRFLOW_VERSION",
    "HAS_AIRFLOW_LISTENER_API",
    "Operator",
    "MappedOperator",
    "EmptyOperator",
    "ExternalTaskSensor",
]
