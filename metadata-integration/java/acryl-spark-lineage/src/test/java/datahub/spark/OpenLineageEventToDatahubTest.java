package datahub.spark;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataprocess.RunResultType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import datahub.spark.conf.SparkLineageConf;
import datahub.spark.converter.SparkStreamingEventToDatahub;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.datahubproject.openlineage.dataset.PathSpec;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;

public class OpenLineageEventToDatahubTest extends TestCase {
  public void testGenerateUrnFromStreamingDescriptionFile() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "FileSink[/tmp/streaming_output/]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    assertEquals("hdfs", urn.get().getPlatformEntity().getPlatformNameEntity());
    assertEquals("/tmp/streaming_output", urn.get().getDatasetNameEntity());
  }

  public void testGenerateUrnFromStreamingDescriptionS3File() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });
    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "FileSink[s3://bucket/streaming_output/]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    assertEquals("s3", urn.get().getPlatformEntity().getPlatformNameEntity());
    assertEquals("bucket/streaming_output", urn.get().getDatasetNameEntity());
  }

  public void testGenerateUrnFromStreamingDescriptionS3AFile() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "FileSink[s3a://bucket/streaming_output/]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    assertEquals("s3", urn.get().getPlatformEntity().getPlatformNameEntity());
    assertEquals("bucket/streaming_output", urn.get().getDatasetNameEntity());
  }

  public void testGenerateUrnFromStreamingDescriptionGCSFile() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "FileSink[gcs://bucket/streaming_output/]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    assertEquals("gcs", urn.get().getPlatformEntity().getPlatformNameEntity());
    assertEquals("bucket/streaming_output", urn.get().getDatasetNameEntity());
  }

  public void testGenerateUrnFromStreamingDescriptionDeltaFile() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "DeltaSink[/tmp/streaming_output/]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    assertEquals("hdfs", urn.get().getPlatformEntity().getPlatformNameEntity());
    assertEquals("/tmp/streaming_output", urn.get().getDatasetNameEntity());
  }

  public void testGenerateUrnFromStreamingDescriptionGCSWithPathSpec()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(
                    SparkConfigParser.PLATFORM_KEY
                        + "."
                        + "gcs"
                        + "."
                        + SparkConfigParser.PATH_SPEC_LIST_KEY,
                    String.join(",", "gcs://my-bucket/foo/{table}/*/*/*"));
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "DeltaSink[gcs://my-bucket/foo/tests/year=2023/month=03/day=11/myfile.parquet]",
            sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests,PROD)", urn.get().toString());
  }

  public void testGcsDataset() throws URISyntaxException {
    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("gs://spark-integration-tests")
            .name("/spark-integration-test/test_gcs_delta_lake")
            .build();

    Config datahubConfig = ConfigFactory.empty();

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
            outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
    assert (urn.isPresent());
    assertEquals(
        "spark-integration-tests/spark-integration-test/test_gcs_delta_lake",
        urn.get().getDatasetNameEntity());
  }

  public void testGcsDatasetWithoutSlashInName() throws URISyntaxException {
    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("gs://spark-integration-tests")
            .name("spark-integration-test/test_gcs_delta_lake")
            .build();

    Config datahubConfig = ConfigFactory.empty();

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
            outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
    assert (urn.isPresent());
    assertEquals(
        "spark-integration-tests/spark-integration-test/test_gcs_delta_lake",
        urn.get().getDatasetNameEntity());
  }

  public void testRemoveFilePrefixFromPath() throws URISyntaxException {
    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("file")
            .name("/tmp/streaming_output/file.txt")
            .build();

    Config datahubConfig = ConfigFactory.empty();
    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
            outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
    assert (urn.isPresent());
    assertEquals("/tmp/streaming_output/file.txt", urn.get().getDatasetNameEntity());
  }

  public void testRemoveFilePrefixFromPathWithPlatformInstance() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(SparkConfigParser.DATASET_PLATFORM_INSTANCE_KEY, "my-platfrom-instance");
              }
            });

    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("file")
            .name("/tmp/streaming_output/file.txt")
            .build();

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
            outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
    assert (urn.isPresent());
    assertEquals(
        "my-platfrom-instance./tmp/streaming_output/file.txt", urn.get().getDatasetNameEntity());
  }

  public void testOpenlineageDatasetWithPathSpec() throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(
                    SparkConfigParser.PLATFORM_KEY
                        + "."
                        + "s3"
                        + "."
                        + SparkConfigParser.PATH_SPEC_LIST_KEY,
                    String.join(
                        ",",
                        "s3a://data-482ajm7100-longtailcompanions-demo-795586375822-usw2/kafka_backup/{table}/year=*/month=*/day=*/*"));
              }
            });

    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("s3a://data-482ajm7100-longtailcompanions-demo-795586375822-usw2")
            .name(
                "/kafka_backup/482ajm7100-longtailcompanions_MCL_Timeseries/year=2023/month=03/day=23")
            .build();

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
            outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
    assert (urn.isPresent());
    assertEquals(
        "data-482ajm7100-longtailcompanions-demo-795586375822-usw2/kafka_backup/482ajm7100-longtailcompanions_MCL_Timeseries",
        urn.get().getDatasetNameEntity());
  }

  public void testOpenlineageTableDataset() throws URISyntaxException {
    // https://openlineage.io/docs/spec/naming#dataset-naming
    Stream<Triple<String, String, String>> testCases =
        Stream.of(
            Triple.of(
                "postgres://db.foo.com:6543",
                "metrics.sales.orders",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,metrics.sales.orders,PROD)"),
            Triple.of(
                "mysql://db.foo.com:6543",
                "metrics.orders",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,metrics.orders,PROD)"),
            Triple.of(
                "s3://sales-metrics",
                "orders.csv",
                "urn:li:dataset:(urn:li:dataPlatform:s3,sales-metrics/orders.csv,PROD)"),
            Triple.of(
                "gcs://sales-metrics",
                "orders.csv",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,sales-metrics/orders.csv,PROD)"),
            Triple.of(
                "hdfs://stg.foo.com:3000",
                "salesorders.csv",
                "urn:li:dataset:(urn:li:dataPlatform:hdfs,salesorders.csv,PROD)"),
            Triple.of(
                "bigquery",
                "metrics.sales.orders",
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,metrics.sales.orders,PROD)"),
            Triple.of(
                "redshift://examplecluster.XXXXXXXXXXXX.us-west-2.redshift.amazonaws.com:5439",
                "metrics.sales.orders",
                "urn:li:dataset:(urn:li:dataPlatform:redshift,metrics.sales.orders,PROD)"),
            Triple.of(
                "awsathena://athena.us-west-2.amazonaws.com",
                "metrics.sales.orders",
                "urn:li:dataset:(urn:li:dataPlatform:athena,metrics.sales.orders,PROD)"),
            Triple.of(
                "sqlserver://XXXXXXXXXXXX.sql.azuresynapse.net:1433",
                "SQLPool1/sales.orders",
                "urn:li:dataset:(urn:li:dataPlatform:mssql,SQLPool1/sales.orders,PROD)"),
            Triple.of(
                "azurecosmos://XXXXXXXXXXXX.documents.azure.com/dbs",
                "metrics.colls.orders",
                "urn:li:dataset:(urn:li:dataPlatform:azurecosmos,metrics.colls.orders,PROD)"));
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });

    testCases.forEach(
        args -> {
          String namespace = args.getLeft();
          String datasetName = args.getMiddle();
          String expectedUrn = args.getRight();

          OpenLineage.OutputDataset outputDataset =
              new OpenLineage.OutputDatasetBuilder().namespace(namespace).name(datasetName).build();

          SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder =
              SparkLineageConf.builder();
          sparkLineageConfBuilder.openLineageConf(
              SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
                  datahubConfig, new SparkAppContext()));

          Optional<DatasetUrn> urn =
              OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
                  outputDataset, sparkLineageConfBuilder.build().getOpenLineageConf());
          assert (urn.isPresent());
          assertEquals(expectedUrn, urn.get().toString());
        });
  }

  public void testProcessOlEvent() throws URISyntaxException, IOException {
    OpenLineage.OutputDataset outputDataset =
        new OpenLineage.OutputDatasetBuilder()
            .namespace("file")
            .name("/tmp/streaming_output/file.txt")
            .build();

    Config datahubConfig = ConfigFactory.empty();
    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));
    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob =
        OpenLineageToDataHub.convertRunEventToJob(
            runEvent, sparkLineageConfBuilder.build().getOpenLineageConf());
    assertNotNull(datahubJob);
  }

  public void testProcessOlFailedEvent() throws URISyntaxException, IOException {

    Config datahubConfig = ConfigFactory.empty();
    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));
    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_failed_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob =
        OpenLineageToDataHub.convertRunEventToJob(
            runEvent, sparkLineageConfBuilder.build().getOpenLineageConf());
    assertNotNull(datahubJob);
    assertEquals("cloud_trail_log_statistics", datahubJob.getDataFlowInfo().getName());
    assertEquals(
        RunResultType.FAILURE, datahubJob.getDataProcessInstanceRunEvent().getResult().getType());
  }

  public void testProcessOlEventWithSetFlowname() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.pipelineName("my_flow_name");

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_failed_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());
    assertNotNull(datahubJob);
    assertEquals("my_flow_name", datahubJob.getDataFlowInfo().getName());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,aws-cloudtrail-logs-795586375822-837d93fd/AWSLogs/795586375822/CloudTrail/eu-west-1,PROD)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,acryl-datahub-offline/tmp/daily_stats2,PROD)",
          dataset.getUrn().toString());
    }

    assertEquals(
        RunResultType.FAILURE, datahubJob.getDataProcessInstanceRunEvent().getResult().getType());
  }

  public void testProcessOlEventWithSetDatasetFabricType() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_failed_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,aws-cloudtrail-logs-795586375822-837d93fd/AWSLogs/795586375822/CloudTrail/eu-west-1,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,acryl-datahub-offline/tmp/daily_stats2,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcessGlueOlEvent() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,my_glue_database.my_glue_table,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:hive,my_glue_database.my_output_glue_table,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcess_OL17_GlueOlEvent() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue_ol_0_17_changes.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,my_glue_database.my_glue_table,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,my_glue_database.my_output_glue_table,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcessGlueOlEventSymlinkDisabled() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.disableSymlinkResolution(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket-test/sample_data/input_data.parquet,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket-test/sample_data/output_data.parquet,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcessGlueOlEventWithHiveAlias() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,my_glue_database.my_glue_table,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,my_glue_database.my_output_glue_table,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcessRedshiftOutput() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/redshift_lineage_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.spark_redshift_load_test,DEV)",
          dataset.getUrn().toString());
      assertEquals(
          dataset.getSchemaMetadata().getPlatform().toString(), "urn:li:dataPlatform:redshift");
    }
  }

  public void testProcessRedshiftOutputWithPlatformInstance()
      throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.commonDatasetPlatformInstance("my-platform-instance");

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/redshift_lineage_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:mysql,my-platform-instance.datahub.metadata_aspect_v2,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:redshift,my-platform-instance.dev.public.spark_redshift_load_test,DEV)",
          dataset.getUrn().toString());
      assertEquals(
          dataset.getSchemaMetadata().getPlatform().toString(), "urn:li:dataPlatform:redshift");
    }
  }

  public void testProcessRedshiftOutputWithPlatformSpecificPlatformInstance()
      throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.pathSpecs(
        new HashMap<String, List<PathSpec>>() {
          {
            put(
                "redshift",
                List.of(
                    PathSpec.builder()
                        .platform("redshift")
                        .platformInstance(Optional.of("my-platform-instance"))
                        .build()));
          }
        });

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/redshift_lineage_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:redshift,my-platform-instance.dev.public.spark_redshift_load_test,DEV)",
          dataset.getUrn().toString());
      assertEquals(
          dataset.getSchemaMetadata().getPlatform().toString(), "urn:li:dataPlatform:redshift");
    }
  }

  public void testProcessRedshiftOutputWithPlatformSpecificEnv()
      throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.pathSpecs(
        new HashMap<String, List<PathSpec>>() {
          {
            put(
                "redshift",
                List.of(PathSpec.builder().platform("redshift").env(Optional.of("PROD")).build()));
          }
        });

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/redshift_lineage_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.spark_redshift_load_test,PROD)",
          dataset.getUrn().toString());
      assertEquals(
          dataset.getSchemaMetadata().getPlatform().toString(), "urn:li:dataPlatform:redshift");
    }
  }

  public void testProcessRedshiftOutputLowercasedUrns() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.hivePlatformAlias("glue");
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.lowerCaseDatasetUrns(true);

    String olEvent =
        IOUtils.toString(
            this.getClass()
                .getResourceAsStream("/ol_events/redshift_mixed_case_lineage_spark.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.spark_redshift_load_test,DEV)",
          dataset.getUrn().toString());
      assertEquals(
          dataset.getSchemaMetadata().getPlatform().toString(), "urn:li:dataPlatform:redshift");
    }
  }

  public void testProcessGCSInputsOutputs() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/gs_input_output.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:gcs,my-gs-input-bucket/path/to/my-input-file.csv,DEV)",
          dataset.getUrn().toString());
    }
    assertEquals(1, datahubJob.getOutSet().size());

    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:gcs,my-gs-output-bucket/path/to/my-output-file.csv,DEV)",
          dataset.getUrn().toString());
    }
  }

  public void testProcessMappartitionJob() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/map_partition_job.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/my_dir/my_file.csv,DEV)",
          dataset.getUrn().toString());
    }
    assertEquals(0, datahubJob.getOutSet().size());
  }
}
