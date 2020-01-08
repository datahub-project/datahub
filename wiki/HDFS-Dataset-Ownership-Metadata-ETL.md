> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

This is an optional feature.

The way to store dataset ownership varies by companies. This dataset ownership metadata ETL process gives an example of extracting owner information from a Hive table.

## Extract
Major related file: [DatasetOwnerEtl.java](../wherehows-etl/src/main/java/metadata/etl/ownership/DatasetOwnerEtl.java)

By defining the Hive query in the job property (which extracts dataset_urn, owner_id, sort_id, namespace, db_name, and source_time), the extract() function will ssh to the Hadoop gateway using a private key and run the Hive query. The query results will be stored in a CSV file that is then copied to the WhereHows application folder for the transform/load process.

## Transform
Major related file: [OwnerTransform.py](../wherehows-etl/src/main/resources/jython/OwnerTransform.py)

If the file that contains owner information is generated in the specified format (dataset_urn, owner_id, sort_id, namespace, db_name, source_time), the following process should be quite similar.

At this stage, all the dataset_urn is matched to the dict_dataset table to get a dataset ID, and owner type information is fetched from the LDAP information.

## Load
Major related file: [OwnerLoad.py](../wherehows-etl/src/main/resources/jython/OwnerLoad.py)

Loading the staging table into final table. Do not overwrite the information modified from the UI, including sort_id, owner_type, and owner_sub_type.

Also, the datasets that do not have an ID (not found in the dict_dataset table), are inserted into a overflow table for future reference.
