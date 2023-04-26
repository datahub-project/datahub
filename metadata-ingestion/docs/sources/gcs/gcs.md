
### Path Specs

**Example - Dataset per file**

Bucket structure:

```
test-gs-bucket
├── employees.csv
└── food_items.csv
```

Path specs config
```
path_specs:
    - include: gs://test-gs-bucket/*.csv

```

**Example - Datasets with partitions**

Bucket structure:
```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── returns
    └── year=2021
        └── month=2
            └── 1.parquet

```

Path specs config:
```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Example - Datasets with partition and exclude**

Bucket structure:
```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── tmp_orders
    └── year=2021
        └── month=2
            └── 1.parquet


```

Path specs config:
```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
      exclude: 
        - **/tmp_orders/**
```
**Example - Datasets of mixed nature**

Bucket structure:
```
test-gs-bucket
├── customers
│   ├── part1.json
│   ├── part2.json
│   ├── part3.json
│   └── part4.json
├── employees.csv
├── food_items.csv
├── tmp_10101000.csv
└──  orders
    └── year=2022
        └── month=2
            ├── 1.parquet
            ├── 2.parquet
            └── 3.parquet

```

Path specs config:
```
path_specs:
    - include: gs://test-gs-bucket/*.csv
      exclude:
        - **/tmp_10101000.csv
    - include: gs://test-gs-bucket/{table}/*.json
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Valid path_specs.include**

```python
gs://my-bucket/foo/tests/bar.avro # single file table   
gs://my-bucket/foo/tests/*.* # mulitple file level tables
gs://my-bucket/foo/tests/{table}/*.avro #table without partition
gs://my-bucket/foo/tests/{table}/*/*.avro #table where partitions are not specified
gs://my-bucket/foo/tests/{table}/*.* # table where no partitions as well as data type specified
gs://my-bucket/{dept}/tests/{table}/*.avro # specifying keywords to be used in display name
gs://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
gs://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in bucket
gs://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in bucket
```

**Valid path_specs.exclude**
- \**/tests/**
- gs://my-bucket/hr/**
- **/tests/*.csv
- gs://my-bucket/foo/*/my_table/**

**Notes**

- {table} represents folder for which dataset will be created.
- include path must end with (*.* or *.[ext]) to represent leaf level.
- if *.[ext] is provided then only files with specified type will be scanned.
- /*/ represents single folder.
- {partition[i]} represents value of partition.
- {partition_key[i]} represents name of the partition.
- While extracting, “i” will be used to match partition_key to partition.
- all folder levels need to be specified in include. Only exclude path can have ** like matching.
- exclude path cannot have named variables ( {} ).
- Folder names should not contain {, }, *, / in their names.
- {folder} is reserved for internal working. please do not use in named variables.



If you would like to write a more complicated function for resolving file names, then a {transformer} would be a good fit.

:::caution

Specify as long fixed prefix ( with out /*/ ) as possible in `path_specs.include`. This will reduce the scanning time and cost, specifically on Google Cloud Storage.

:::


:::caution

If you are ingesting datasets from Google Cloud Storage, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::
