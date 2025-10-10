# DataHub Airflow Plugin Example DAGs

This directory contains example DAGs demonstrating various features of the DataHub Airflow plugin.

## Directory Structure

```
example_dags/
├── airflow2/          # Airflow 2.x examples (with compatibility layers)
│   ├── lineage_backend_demo.py
│   ├── lineage_backend_taskflow_demo.py
│   ├── snowflake_sample_dag.py
│   ├── mysql_sample_dag.py
│   ├── generic_recipe_sample_dag.py
│   ├── lineage_emission_dag.py
│   └── graph_usage_sample_dag.py
│
├── airflow3/          # Airflow 3.0+ examples (native syntax, no compatibility)
│   ├── lineage_backend_demo.py
│   ├── lineage_backend_taskflow_demo.py
│   └── snowflake_sample_dag.py
│
└── *.py               # Legacy examples (deprecated - use airflow2/ or airflow3/)
```

## Choosing the Right Examples

### For Airflow 3.0+ Users

Use the examples in `airflow3/`. These DAGs:

- Use native Airflow 3.0 syntax (`schedule` instead of `schedule_interval`)
- Don't include compatibility layers
- Are simpler and easier to read
- Demonstrate best practices for Airflow 3.0+

### For Airflow 2.x Users

Use the examples in `airflow2/`. These DAGs:

- Work across Airflow 2.3 through 2.9
- Include compatibility helpers from `_airflow_version_specific.py`
- Handle parameter name changes between versions

### For Cross-Version Support

If you need DAGs that work on both Airflow 2.x and 3.x:

- See `airflow2/` examples for patterns using `get_airflow_compatible_dag_kwargs()`
- Use `days_ago()` helper for start_date compatibility
- Import hooks conditionally based on Airflow version

## Example Categories

### 1. Lineage Collection

Examples showing how to automatically collect lineage from your Airflow DAGs:

- **lineage_backend_demo.py**: Basic lineage collection using `inlets` and `outlets`
- **lineage_backend_taskflow_demo.py**: Lineage with TaskFlow API (`@task` decorator)

### 2. Metadata Ingestion

Examples showing how to ingest metadata from data sources into DataHub:

- **snowflake_sample_dag.py**: Ingest Snowflake metadata
- **mysql_sample_dag.py**: Ingest MySQL metadata
- **generic_recipe_sample_dag.py**: Run any DataHub ingestion recipe

### 3. Advanced Features

- **lineage_emission_dag.py**: Directly emit custom lineage using `DatahubEmitterOperator`
- **graph_usage_sample_dag.py**: Complex DAG graphs with multiple task dependencies

## Key Differences: Airflow 2 vs 3

### DAG Parameters

```python
# Airflow 2.x
DAG(
    schedule_interval=timedelta(days=1),
    default_view="tree",  # Removed in Airflow 3
)

# Airflow 3.0+
DAG(
    schedule=timedelta(days=1),
    # default_view parameter removed
)
```

### Hooks Import

```python
# Airflow 2.x
from airflow.hooks.base import BaseHook

# Airflow 3.0+
from airflow.hooks.base_hook import BaseHook
```

### Start Date

```python
# Airflow 2.x (compatibility helper)
from datahub_airflow_plugin._airflow_version_specific import days_ago
start_date=days_ago(2)

# Airflow 3.0+ (direct)
from datetime import datetime
start_date=datetime(2023, 1, 1)
```

## Running the Examples

1. Copy the appropriate example to your Airflow DAGs folder:

   ```bash
   # For Airflow 3.0+
   cp airflow3/lineage_backend_demo.py $AIRFLOW_HOME/dags/

   # For Airflow 2.x
   cp airflow2/lineage_backend_demo.py $AIRFLOW_HOME/dags/
   ```

2. Configure the DataHub connection in Airflow:

   ```bash
   airflow connections add datahub_rest_default \
     --conn-type datahub-rest \
     --conn-host http://localhost:8080
   ```

3. Enable the DAG in the Airflow UI or trigger it manually:
   ```bash
   airflow dags trigger datahub_lineage_backend_demo
   ```

## Additional Resources

- [DataHub Airflow Plugin Documentation](https://datahubproject.io/docs/lineage/airflow/)
- [Airflow 3.0 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/migration-guide.html)
- [DataHub Metadata Ingestion](https://datahubproject.io/docs/metadata-ingestion/)
