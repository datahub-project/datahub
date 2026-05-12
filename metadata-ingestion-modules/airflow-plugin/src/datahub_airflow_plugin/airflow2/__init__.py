"""
DataHub Airflow Plugin for Airflow 2.x.

Supports two OpenLineage backends depending on the installed Airflow version:

* Airflow 2.5–2.6: uses the standalone ``openlineage-airflow`` package
  (install with ``pip install 'acryl-datahub-airflow-plugin[airflow2]'``).
* Airflow 2.7+: uses the native ``apache-airflow-providers-openlineage`` package,
  which avoids the deprecation warning emitted by the legacy package
  (install with ``pip install 'acryl-datahub-airflow-plugin[airflow27]'``).

The correct backend is selected automatically at import time based on the
installed Airflow version and which OpenLineage package is present.
"""
