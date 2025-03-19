# DataHub HDF5 Source

The DataHub HDF5 Source provides functionality for ingesting datasets from HDF5 files into DataHub as structured tables. This source supports various types of datasets present within HDF5 files, translating hierarchical and structured data into tabular schemas suitable for DataHub.

---

## Overview

The HDF5 Source is designed to read datasets from HDF5 files and represent these datasets as tables within the DataHub metadata ecosystem. It processes nested groups, multi-dimensional datasets, and compound datasets conforming to specific conventions described below.

---

## Key Ingestion Concepts

### Nested Groups
Nested groups within an HDF5 file are represented in DataHub as hierarchical paths, which reflect their location from the file root.
- Example: `/filename/group1/subgroup2/datasetA`

### Dataset Dimensionality Handling
The ingestion behavior varies based on the dimensionality (`shape`) of the datasets:
- **One-dimensional (1D) Compound Datasets**: Ingested as a table schema using the fields present in the compound data type directly.
- **Other 1D Datasets**: Ingested as a table consisting of a single column named `col0`.
- **Two-dimensional (2D) Datasets**: Each row of the dataset is translated into a distinct table column. Column names reflect the row index and the number of elements within the row (e.g., `row_1_with_10000_values`), with column values populated by corresponding row data.
- **Three-dimensional (3D) and Higher Datasets**: Only the first two dimensions are ingested. Additional dimensions beyond the second are excluded to conform to a manageable schema within DataHub.

---

## Schema Generation Logic
The HDF5 source constructs schema metadata and fields based on data types and dimensionality present in the datasets:

- **Simple (scalar or numeric)**: Fields are directly translated based on dataset types and dimensions into `SchemaField` entries.
- **Compound Datatype Datasets**: Each named field within the compound dataset generates a distinct table schema column corresponding to its type and dimensionality.
- **String and Text Datasets**: Represented accurately within data type mappings and schema fields to ensure correct ingestion into DataHub's metadata tables.

---

## Example Representation

Consider the given file hierarchy:

```
datafile.h5
│
├── /group1
│   ├── 1D_compound_dataset
│   └── 3D_numeric_dataset
│
└── /group2
    ├── 1D_numeric_dataset
    └── 2D_numeric_dataset
```

The ingested metadata representation on DataHub would look as follows:
- `/datafile/group1/1D_compound_dataset`: Compound fields become distinct schema columns.
- `/datafile/group1/3D_numeric_dataset`: Only first two dimensions ingested.
- `/datafile/group2/1D_numeric_dataset`: Single-column table (`col0`).
- `/datafile/group2/2D_numeric_dataset`: Multiple columns derived from rows.

---
