# Assertion

Assertion entity represents a data quality rule applied on dataset.  
In future, it can evolve to span across Datasets, Flows (Pipelines), Models, Features etc.

## Identity

An **Assertion** is identified by globally unique identifier which remains constant between runs of the assertion. For each source of assertion information, it is expected that the logic required to generate the stable guid will differ. For example, a unique GUID is generated from each assertion  from Great Expectations based on a combination of the assertion name along with its parameters. 

## Important Capabilities

### Assertion Info

Type and Details of assertions set on a Dataset (Table). 

**Scope**: Column, Rows, Schema 
**Inputs**: Column(s) 
**Aggregation**: Max, Min, etc 
**Operator**: Greater Than, Not null, etc 
**Parameters**: Value, Min Value, Max Value 

### Assertion Run Events 

Evaluation status and results for an assertion tracked over time.

<details>
<summary>Python SDK: Emit assertion info and results for dataset </summary>

```python
{{ inline /metadata-ingestion/examples/library/data_quality_mcpw_rest.py show_path_as_comment }}
```
</details>




