
### Path Specs

Path Specs (`path_specs`) is a list of Path Spec (`path_spec`) objects, where each individual `path_spec` represents one or more datasets. The include path (`path_spec.include`) represents a formatted path to the dataset. This path must end with `*.*` or `*.[ext]` to represent the leaf level. If `*.[ext]` is provided, then only files with the specified extension type will be scanned. "`.[ext]`" can be any of the [supported file types](#supported-file-types). Refer to [example 1](#example-1---individual-file-as-dataset) below for more details.

All folder levels need to be specified in the include path. You can use `/*/` to represent a folder level and avoid specifying the exact folder name. To map a folder as a dataset, use the `{table}` placeholder to represent the folder level for which the dataset is to be created. For a partitioned dataset, you can use the placeholder `{partition_key[i]}` to represent the name of the `i`th partition and `{partition[i]}` to represent the value of the `i`th partition. During ingestion, `i` will be used to match the partition_key to the partition. Refer to [examples 2 and 3](#example-2---folder-of-files-as-dataset-without-partitions) below for more details.

Exclude paths (`path_spec.exclude`) can be used to ignore paths that are not relevant to the current `path_spec`. This path cannot have named variables (`{}`). The exclude path can have `**` to represent multiple folder levels. Refer to [example 4](#example-4---folder-of-files-as-dataset-with-partitions-and-exclude-filter) below for more details.

Refer to [example 5](#example-5---advanced---either-individual-file-or-folder-of-files-as-dataset) if your container has a more complex dataset representation.

**Additional points to note**
- Folder names should not contain {, }, *, / in their names.
- Named variable {folder} is reserved for internal working. please do not use in named variables.


### Path Specs -  Examples
#### Example 1 - Individual file as Dataset

Container structure:

```
test-container
├── employees.csv
├── departments.json
└── food_items.csv
```

Path specs config to ingest `employees.csv` and `food_items.csv` as datasets:
```
path_specs:
    - include: https://storageaccountname.blob.core.windows.net/test-container/*.csv

```
This will automatically ignore `departments.json` file. To include it, use `*.*` instead of `*.csv`.

#### Example 2 - Folder of files as Dataset (without Partitions)

Container structure:
```
test-container
└──  offers
     ├── 1.avro
     └── 2.avro

```

Path specs config to ingest folder `offers` as dataset:
```
path_specs:
    - include: https://storageaccountname.blob.core.windows.net/test-container/{table}/*.avro
```

`{table}` represents folder for which dataset will be created.
 
#### Example 3 - Folder of files as Dataset (with Partitions)

Container structure:
```
test-container
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── returns
    └── year=2021
        └── month=2
            └── 1.parquet

```

Path specs config to ingest folders `orders` and `returns` as datasets:
```
path_specs:
    - include: https://storageaccountname.blob.core.windows.net/test-container/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

One can also use `include: https://storageaccountname.blob.core.windows.net/test-container/{table}/*/*/*.parquet` here however above format is preferred as it allows declaring partitions explicitly.

#### Example 4 - Folder of files as Dataset (with Partitions), and Exclude Filter

Container structure:
```
test-container
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── tmp_orders
    └── year=2021
        └── month=2
            └── 1.parquet


```

Path specs config to ingest folder `orders` as dataset but not folder `tmp_orders`:
```
path_specs:
    - include: https://storageaccountname.blob.core.windows.net/test-container/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
      exclude: 
        - **/tmp_orders/**
```


#### Example 5 - Advanced - Either Individual file OR Folder of files as Dataset

Container structure:
```
test-container
├── customers
│   ├── part1.json
│   ├── part2.json
│   ├── part3.json
│   └── part4.json
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
    - include: https://storageaccountname.blob.core.windows.net/test-container/*.csv
      exclude:
        - **/tmp_10101000.csv
    - include: https://storageaccountname.blob.core.windows.net/test-container/{table}/*.json
    - include: https://storageaccountname.blob.core.windows.net/test-container/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

Above config has 3 path_specs and will ingest following datasets
- `employees.csv` - Single File as Dataset
- `food_items.csv` - Single File as Dataset
- `customers` - Folder as Dataset
- `orders` - Folder as Dataset
  and will ignore file `tmp_10101000.csv`

**Valid path_specs.include**

```python
https://storageaccountname.blob.core.windows.net/my-container/foo/tests/bar.avro # single file table   
https://storageaccountname.blob.core.windows.net/my-container/foo/tests/*.* # mulitple file level tables
https://storageaccountname.blob.core.windows.net/my-container/foo/tests/{table}/*.avro #table without partition
https://storageaccountname.blob.core.windows.net/my-container/foo/tests/{table}/*/*.avro #table where partitions are not specified
https://storageaccountname.blob.core.windows.net/my-container/foo/tests/{table}/*.* # table where no partitions as well as data type specified
https://storageaccountname.blob.core.windows.net/my-container/{dept}/tests/{table}/*.avro # specifying keywords to be used in display name
https://storageaccountname.blob.core.windows.net/my-container/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
https://storageaccountname.blob.core.windows.net/my-container/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
https://storageaccountname.blob.core.windows.net/my-container/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
https://storageaccountname.blob.core.windows.net/my-container/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in container
https://storageaccountname.blob.core.windows.net/my-container/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in container
```

**Valid path_specs.exclude**
- \**/tests/**
- https://storageaccountname.blob.core.windows.net/my-container/hr/**
- **/tests/*.csv
- https://storageaccountname.blob.core.windows.net/my-container/foo/*/my_table/**



If you would like to write a more complicated function for resolving file names, then a {transformer} would be a good fit.

:::caution

Specify as long fixed prefix ( with out /*/ ) as possible in `path_specs.include`. This will reduce the scanning time and cost, specifically on AWS S3

:::

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

:::caution

If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

### Compatibility

Profiles are computed with PyDeequ, which relies on PySpark. Therefore, for computing profiles, we currently require Spark 3.0.3 with Hadoop 3.2 to be installed and the `SPARK_HOME` and `SPARK_VERSION` environment variables to be set. The Spark+Hadoop binary can be downloaded [here](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz).

For an example guide on setting up PyDeequ on AWS, see [this guide](https://aws.amazon.com/blogs/big-data/testing-data-quality-at-scale-with-pydeequ/).

:::caution

From Spark 3.2.0+, Avro reader fails on column names that don't start with a letter and contains other character than letters, number, and underscore. [https://github.com/apache/spark/blob/72c62b6596d21e975c5597f8fff84b1a9d070a02/connector/avro/src/main/scala/org/apache/spark/sql/avro/AvroFileFormat.scala#L158] 
Avro files that contain such columns won't be profiled.
:::