# Assertion

Assertion entity represents a data quality rule applied on dataset.  
In future, it can evolve to span across Datasets, Flows (Pipelines), Models, Features etc.

## Identity

Assertion is identified by globally unique identifier, which is function of details that uniquely identify the assertion in an assertion platform.

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
# inlined from examples/library/data_quality_mcpw_rest.py
{{ inline examples/library/data_quality_mcpw_rest.py }}
```
</details>




