# S3 Crawling with Glue

:::note

Our [S3 data lake](./s3_data_lake.md) source allows you to ingest S3 files directly with the option to compute profiles as well. The following guide describes how to ingest S3 datasets through cataloging them in AWS Glue.

:::

To produce schema metadata for files on S3, we recommend using AWS Glue's built-in schema inference capabilities, as we already have a [Glue ingestion integration](./glue.md). Note: if you have nested data, perhaps in JSON format, then we recommend you hold tight since Glue's nested schema capabilities are fairly limited.

To ingest S3 files with Glue, there are two main steps: (1) setting up Glue to scan your S3 directories, and (2) adding the Glue ingestion configurations on DataHub.

## Setting up Glue on AWS

Glue scans files and infers their schemas using **crawlers**. Support exists for a wide variety of file formats (see [here](https://docs.aws.amazon.com/glue/latest/dg/add-classifier.html) for the official AWS list). Glue is also capable of crawling databases such as MySQL, PostgreSQL, and Redshift, though DataHub already has ingestion frameworks for these.

| Classifier type     | Classification string | Notes                                                                                                                                                                                                                                                                                                                                               |
| ------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Apache Avro         | `avro`                | Reads the schema at the beginning of the file to determine format.                                                                                                                                                                                                                                                                                  |
| Apache ORC          | `orc`                 | Reads the file metadata to determine format.                                                                                                                                                                                                                                                                                                        |
| Apache Parquet      | `parquet`             | Reads the schema at the end of the file to determine format.                                                                                                                                                                                                                                                                                        |
| JSON                | `json`                | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| Binary JSON         | `bson`                | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| XML                 | `xml`                 | Reads the beginning of the file to determine format. AWS Glue determines the table schema based on XML tags in the document. For information about creating a custom XML classifier to specify rows in the document, see [Writing XML Custom Classifiers](https://docs.aws.amazon.com/glue/latest/dg/custom-classifier.html#custom-classifier-xml). |
| Amazon Ion          | `ion`                 | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| Combined Apache log | `combined_apache`     | Determines log formats through a grok pattern.                                                                                                                                                                                                                                                                                                      |
| Apache log          | `apache`              | Determines log formats through a grok pattern.                                                                                                                                                                                                                                                                                                      |
| Linux kernel log    | `linux_kernel`        | Determines log formats through a grok pattern.                                                                                                                                                                                                                                                                                                      |
| Microsoft log       | `microsoft_log`       | Determines log formats through a grok pattern.                                                                                                                                                                                                                                                                                                      |
| Ruby log            | `ruby_logger`         | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| Squid 3.x log       | `squid`               | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| Redis monitor log   | `redismonlog`         | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| Redis log           | `redislog`            | Reads the beginning of the file to determine format.                                                                                                                                                                                                                                                                                                |
| CSV                 | `csv`                 | Checks for the following delimiters: comma (,), pipe (\|), tab (\t), semicolon (;), and Ctrl-A (\u0001). Ctrl-A is the Unicode control character for `Start Of Heading`.                                                                                                                                                                            |

### Test data

If you'd like to test Glue out on a smaller dataset before connecting it to any production records, AWS has public S3 buckets named `crawler-public-{REGION}` (for instance, `crawler-public-us-west-2`). These contain an example dataset on 2016 plane flights in CSV, Avro, ORC, and Parquet formats. In the following example, we'll demonstrate how to crawl the Avro files and ingest them into DataHub.

### Creating a crawler

To configure Glue, first navigate to the Glue console at https://console.aws.amazon.com/glue/home.

Before starting, consider the following:

1. The source files should all be of the same shape (i.e. they have the same columns). Glue's schema inference may have unexpected behavior if files with different schemas are scanned together.
2. The source files should not contain deeply nested fields (which is possible with JSON files). Glue will only capture the top-level keys in such files.
3. [Glue's pricing model](https://aws.amazon.com/glue/pricing/). Note that each crawler is charged based on how long and how many of AWS's Data Processing Units (DPUs) run. If the source dataset is relatively large, there may be high costs associated, so we recommend you run on a smaller subset first.

Now let's get started!

Under "Data Catalog" > "Crawlers", click "Add Crawler" to open up the creation dialog.

1. First, set the name and description of your crawler so you'll be able to come back to it later. Note that once you've created a crawler, you can still edit it later, so none of the settings here have to be final except for the crawler name.

   ![1_crawler-info](../../docs/imgs/s3-ingestion/1_crawler-info.png)

2. Next, set the type of data you'd like to crawl over. In this case, we'll traverse all folders in an existing data store (S3). Table's already in Glue's data catalog may also be reused.

   ![2_crawler-type](../../docs/imgs/s3-ingestion/2_crawler-type.png)

3. Now, we'll specify the data store source. In this case, we use the folder `s3://crawler-public-us-west-2/flight/avro/`.

   ![3_data-store](../../docs/imgs/s3-ingestion/3_data-store.png)

4. If there's another data source, we also have the option to add it here. Adding another source may be useful if there's another similar data store you want to crawl at the same time.

   ![4_data-store-2](../../docs/imgs/s3-ingestion/4_data-store-2.png)

5. To give your crawler permissions to read your data source, select or create an IAM role.

   ![5_iam](../../docs/imgs/s3-ingestion/5_iam.png)

6. Next, set how you want your crawler to be run. In this case, we'll use "Run on demand", which means that we trigger the crawler ourselves.

   ![6_schedule](../../docs/imgs/s3-ingestion/6_schedule.png)

7. Now, we'll configure how you want the crawler to output data. Glue outputs metadata to 'databases' where each detected group of data is mapped to a 'table.' If you'd like to distinguish between different crawlers/data sources, you can select a prefix to be added to the database name.

   ![7_output](../../docs/imgs/s3-ingestion/7_output.png)

8. Next, review your crawler settings and save the crawler.

   ![8_review](../../docs/imgs/s3-ingestion/8_review.png)

9. Finally, let's trigger the crawler run as we previously selected 'On demand'. From the list of crawlers, click the one we just created, and hit 'Run crawler'. (Otherwise, if you selected a scheduled frequency, just wait for your crawler to run automatically).

   ![9_run](../../docs/imgs/s3-ingestion/9_run.png)

Once complete, you should see an `avro` table in the data catalog along with the following metadata:

![10_outputs](../../docs/imgs/s3-ingestion/10_outputs.png)

## Adding a Glue ingestion source

Having crawled our data in Glue, the next step is to ingest our Glue metadata into DataHub.

To add a Glue ingestion source, first make sure you have DataHub running already and have already installed the CLI (see [here](../README.md) for installation instructions).

Next, create your YML config file (see example config file [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/recipes/glue_to_datahub.yml)) and run `datahub ingest -c CONFIG_FILE.yml` to get your metadata!

For the above example with the Avro flights data, one might use the following config for the source:

```yaml
source:
  type: glue
  config:
    aws_region: "us-west-2"
    env: "PROD"

    # Filtering patterns for databases and tables to scan
    database_pattern:
      allow:
        - "flights-database"
    table_pattern:
    	allow:
        - "avro"

    # Credentials. If not specified here, these are picked up according to boto3 rules.
    # (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    aws_access_key_id: # Optional.
    aws_secret_access_key: # Optional.
    aws_session_token: # Optional.
    aws_role: # Optional (Role chaining supported by using a sorted list).
```

This will extract the following:

- List of tables
- Column types associated with each table
- Table metadata, such as owner, description and parameters

After ingesting, you should be able to see your datasets on the DataHub frontend. For more details, check out the [full metadata ingestion docs](../README.md).
