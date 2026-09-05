## Overview

Pentaho Data Integration (PDI, formerly Kettle) stores its work as XML files. Transformations use the `.ktr` extension, jobs use `.kjb`.

The DataHub integration scans a folder of these files and emits each one as a DataJob with table-level lineage from its database steps.

## Concept Mapping

| Source Concept          | DataHub Concept                 | Notes                                                         |
| ----------------------- | ------------------------------- | ------------------------------------------------------------- |
| Transformation (`.ktr`) | DataJob (type `TRANSFORMATION`) | One DataJob per transformation file.                          |
| Job (`.kjb`)            | DataJob (type `JOB`)            | Referenced transformations and jobs become custom properties. |
| `TableInput` step       | Dataset (upstream)              | Contributes an input edge to the transformation.              |
| `TableOutput` step      | Dataset (downstream)            | Contributes an output edge to the transformation.             |
| Database connection     | Data Platform                   | Resolved through `platform_mappings`.                         |
| Pentaho instance        | Platform Instance               | Set with `platform_instance`.                                 |
