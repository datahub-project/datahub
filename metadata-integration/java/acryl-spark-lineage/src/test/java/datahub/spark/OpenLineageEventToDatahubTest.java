package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataprocess.RunResultType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.mxe.MetadataChangeProposal;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import datahub.spark.conf.SparkLineageConf;
import datahub.spark.converter.SparkStreamingEventToDatahub;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.datahubproject.openlineage.dataset.PathSpec;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

public class OpenLineageEventToDatahubTest {
  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testGenerateUrnFromStreamingDescriptionKafkaWithPlatformInstance()
      throws URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(SparkConfigParser.DATASET_PLATFORM_INSTANCE_KEY, "my_instance");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            "KafkaV2[Subscribe[my-topic]]", sparkLineageConfBuilder.build());
    assert (urn.isPresent());

    // Streaming Kafka sinks must carry the configured platform_instance so their URNs match the
    // batch-emitted URNs for the same topic (cross-domain lineage stitching).
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:kafka,my_instance.my-topic,PROD)",
        urn.get().toString());
  }

  @Test
  public void testSparkConfigParsesConnectionInstanceMap() {
    Config datahubConfig =
        ConfigFactory.parseString(
            "metadata.dataset.connections {\n"
                + "  \"arn:aws:glue:us-east-1:111122223333\" { platformInstance = \"domain_a\", env = \"PROD\" }\n"
                + "  \"snowflake://acme-prod\" { platformInstance = \"snow_prod\" }\n"
                + "}");

    DatahubOpenlineageConfig conf =
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(datahubConfig, new SparkAppContext());

    ConnectionInstanceDetail glue =
        conf.getConnectionInstanceMap().get("arn:aws:glue:us-east-1:111122223333");
    assertNotNull(glue);
    assertEquals(Optional.of("domain_a"), glue.getPlatformInstance());
    assertEquals(Optional.of(FabricType.PROD), glue.getEnv());

    // Same map, a non-Glue connection keyed by its OpenLineage namespace.
    ConnectionInstanceDetail snowflake =
        conf.getConnectionInstanceMap().get("snowflake://acme-prod");
    assertNotNull(snowflake);
    assertEquals(Optional.of("snow_prod"), snowflake.getPlatformInstance());
    assertEquals(Optional.empty(), snowflake.getEnv());
  }

  @Test
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

    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests,PROD)", urn.get().toString());
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testToConnectionKeyDispatchesByProtocol() {
    // Glue arrives as an ARN (both pre- and post-0.17.1 forms) -> canonical ARN authority.
    assertEquals(
        "arn:aws:glue:us-west-2:123456789012",
        OpenLineageToDataHub.toConnectionKey("aws:glue:us-west-2:123456789012"));
    assertEquals(
        "arn:aws:glue:us-east-1:111122223333",
        OpenLineageToDataHub.toConnectionKey("arn:aws:glue:us-east-1:111122223333"));
    // URI namespaces carry the connection authority directly -> used verbatim.
    assertEquals(
        "snowflake://acme-prod", OpenLineageToDataHub.toConnectionKey("snowflake://acme-prod"));
    assertEquals(
        "postgres://pg.example.com:5432",
        OpenLineageToDataHub.toConnectionKey("postgres://pg.example.com:5432"));
    // Bare platform name / null -> no connection key.
    assertNull(OpenLineageToDataHub.toConnectionKey("hive"));
    assertNull(OpenLineageToDataHub.toConnectionKey(null));
  }

  @Test
  public void testProcessGlueOlEventWithConnectionInstanceMap()
      throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    // The Glue symlink in the fixture is aws:glue:us-west-2:123456789012; map that catalog (account
    // + region) to its owning instance via its canonical ARN authority. Account alone is not
    // unique — the same account can own a Glue catalog in another region.
    Map<String, ConnectionInstanceDetail> connections = new HashMap<>();
    connections.put(
        "arn:aws:glue:us-west-2:123456789012",
        ConnectionInstanceDetail.builder().platformInstance(Optional.of("domain_a")).build());
    builder.connectionInstanceMap(connections);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:glue,domain_a.my_glue_database.my_glue_table,DEV)",
          dataset.getUrn().toString());
    }
  }

  @Test
  public void testConnectionInstanceMapSymlinkKeyLowerCasedWhenLowerCaseUrns()
      throws URISyntaxException {
    // A symlinked (e.g. Hive) upstream whose connection namespace has a mixed-case host. With
    // lowerCaseUrns the dataset namespace is lowercased, so the connection-map lookup key must be
    // too — otherwise the mixed-case symlink-derived key misses a lowercase map entry and the
    // platform_instance is silently dropped.
    OpenLineage ol = new OpenLineage(URI.create("https://test"));
    OpenLineage.SymlinksDatasetFacet symlinks =
        ol.newSymlinksDatasetFacet(
            List.of(
                ol.newSymlinksDatasetFacetIdentifiers(
                    "hive://Metastore.Example.com:9083", "mydb.orders", "TABLE")));
    OpenLineage.InputDataset inputDataset =
        ol.newInputDatasetBuilder()
            .namespace("hive://Metastore.Example.com:9083")
            .name("mydb.orders")
            .facets(ol.newDatasetFacetsBuilder().symlinks(symlinks).build())
            .build();

    Map<String, ConnectionInstanceDetail> connections = new HashMap<>();
    connections.put(
        "hive://metastore.example.com:9083",
        ConnectionInstanceDetail.builder().platformInstance(Optional.of("hive_core")).build());
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .lowerCaseDatasetUrns(true)
            .connectionInstanceMap(connections)
            .build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(inputDataset, config);
    assertTrue(urn.isPresent());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:hive,hive_core.mydb.orders,PROD)",
        urn.get().toString());
  }

  @Test
  public void testFilePartitionRegexpStripsBareFileNamespace() throws URISyntaxException {
    // Local "file" datasets use a bare namespace (no scheme), so they bypass HdfsPathDataset where
    // file_partition_regexp is normally applied. The opt-in regexp must still strip the partition.
    OpenLineage.InputDataset inputDataset =
        new OpenLineage.InputDatasetBuilder()
            .namespace("file")
            .name("/data/events/dt=2024-01-01")
            .build();
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .filePartitionRegexpPattern("/dt=[^/]*")
            .build();
    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(inputDataset, config);
    assert (urn.isPresent());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:file,/data/events,PROD)", urn.get().toString());
  }

  @Test
  public void testBareFileNamespaceUnchangedWithoutPartitionRegexp() throws URISyntaxException {
    // Without the opt-in regexp the bare-namespace file dataset URN is unchanged (no regression).
    OpenLineage.InputDataset inputDataset =
        new OpenLineage.InputDatasetBuilder()
            .namespace("file")
            .name("/data/events/dt=2024-01-01")
            .build();
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(inputDataset, config);
    assert (urn.isPresent());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:file,/data/events/dt=2024-01-01,PROD)",
        urn.get().toString());
  }

  @Test
  public void testConnectionInstanceMapNonGlueUpstream() throws URISyntaxException {
    // A non-Glue upstream with no symlink: the OpenLineage namespace IS the connection key and its
    // scheme is the platform, so the same map works beyond Glue (e.g. a Spark job reading
    // Postgres).
    OpenLineage.InputDataset inputDataset =
        new OpenLineage.InputDatasetBuilder()
            .namespace("postgres://pg.example.com:5432")
            .name("mydb.public.orders")
            .build();

    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.PROD);
    Map<String, ConnectionInstanceDetail> connections = new HashMap<>();
    connections.put(
        "postgres://pg.example.com:5432",
        ConnectionInstanceDetail.builder().platformInstance(Optional.of("pg_core")).build());
    builder.connectionInstanceMap(connections);

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(inputDataset, builder.build());
    assert (urn.isPresent());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,pg_core.mydb.public.orders,PROD)",
        urn.get().toString());
  }

  @Test
  public void testConnectionInstanceEnvOverridesGlobalFabric() throws URISyntaxException {
    // A per-connection env must win over the job-global fabricType, otherwise the URN's fabric
    // won't match the one the upstream's own connector emits and the lineage edge dangles.
    OpenLineage.InputDataset inputDataset =
        new OpenLineage.InputDatasetBuilder()
            .namespace("postgres://pg.example.com:5432")
            .name("mydb.public.orders")
            .build();

    Map<String, ConnectionInstanceDetail> connections = new HashMap<>();
    connections.put(
        "postgres://pg.example.com:5432",
        ConnectionInstanceDetail.builder()
            .platformInstance(Optional.of("pg_core"))
            .env(Optional.of(FabricType.PROD))
            .build());
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.DEV) // global default deliberately differs from the connection
            .connectionInstanceMap(connections)
            .build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(inputDataset, config);
    assertTrue(urn.isPresent());
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,pg_core.mydb.public.orders,PROD)",
        urn.get().toString());
  }

  @Test
  public void testConnectionInstanceMapEnvNormalizedAndValidated() {
    // env is normalized (case-insensitive) and validated at parse time: a lowercase value is
    // accepted; an unparseable value is dropped (left empty) rather than silently corrupting URNs.
    Config datahubConfig =
        ConfigFactory.parseString(
            "metadata.dataset.connections {\n"
                + "  \"postgres://h1:5432\" { platformInstance = \"a\", env = \"prod\" }\n"
                + "  \"postgres://h2:5432\" { platformInstance = \"b\", env = \"NONSENSE\" }\n"
                + "}");

    DatahubOpenlineageConfig conf =
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(datahubConfig, new SparkAppContext());

    assertEquals(
        Optional.of(FabricType.PROD),
        conf.getConnectionInstanceMap().get("postgres://h1:5432").getEnv());
    assertEquals(
        Optional.empty(), conf.getConnectionInstanceMap().get("postgres://h2:5432").getEnv());
  }

  @Test
  public void testDataJobHasPlatformInstanceAspect() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.platformInstance("my_pipeline_instance");

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_glue.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());
    List<MetadataChangeProposal> mcps = datahubJob.toMcps(builder.build());

    // The DataJob must carry its own DataPlatformInstance aspect, not just inherit the instance via
    // the parent DataFlow URN.
    boolean dataJobHasInstance =
        mcps.stream()
            .anyMatch(
                m ->
                    "dataJob".equals(m.getEntityType())
                        && "dataPlatformInstance".equals(m.getAspectName()));
    assertTrue(dataJobHasInstance);
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testCaptureTransformOption() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(true);
    builder.captureColumnLevelLineage(true);
    builder.includeIndirectColumnLineage(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_spark_with_transformation.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:file,/spark-test/people.parquet,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:file,/spark-test/result_test,DEV)",
          dataset.getUrn().toString());

      // With include_indirect=true, DIRECT and INDIRECT transformations are merged into the
      // single FineGrainedLineage entry per downstream column (back-compat with the
      // pre-fix emission shape).
      assertEquals(
          "DIRECT:IDENTITY,INDIRECT:FILTER",
          Objects.requireNonNull(dataset.getLineage().getFineGrainedLineages())
              .get(0)
              .getTransformOperation());
    }
  }

  @Test
  public void testIncludeIndirectColumnLineageDisabled() throws URISyntaxException, IOException {
    // When include_indirect=false:
    //   - "name" output has one INDIRECT-only contributor (people.parquet.age, filter) and one
    //     DIRECT contributor (people.parquet.name). The INDIRECT-only contributor must drop.
    //   - "age" output has a single MIXED contributor (people.parquet.age with both
    //     DIRECT:IDENTITY and INDIRECT:FILTER). Mixed contributors must NOT be dropped.
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(true);
    builder.captureColumnLevelLineage(true);
    builder.includeIndirectColumnLineage(false);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_spark_with_transformation.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      List<FineGrainedLineage> fglines =
          Objects.requireNonNull(dataset.getLineage().getFineGrainedLineages());

      FineGrainedLineage nameEntry =
          fglines.stream()
              .filter(fgl -> fgl.getDownstreams().get(0).toString().endsWith(",name)"))
              .findFirst()
              .orElseThrow(AssertionError::new);
      assertEquals(
          1,
          nameEntry.getUpstreams().size(),
          "INDIRECT-only contributor (age) must be dropped from the name column's upstreams");
      // Transformation tags are preserved even when the contributing URN is dropped, so the
      // user can still tell the SQL involved a filter even though we don't list the filter
      // column as an upstream.
      assertEquals("DIRECT:IDENTITY,INDIRECT:FILTER", nameEntry.getTransformOperation());

      // Mixed DIRECT+INDIRECT contributor must be kept — guards against isIndirectOnly
      // incorrectly returning true when the field also has a DIRECT role.
      FineGrainedLineage ageEntry =
          fglines.stream()
              .filter(fgl -> fgl.getDownstreams().get(0).toString().endsWith(",age)"))
              .findFirst()
              .orElseThrow(AssertionError::new);
      assertEquals(
          1,
          ageEntry.getUpstreams().size(),
          "Mixed DIRECT+INDIRECT contributor (age) must NOT be dropped from the age column's upstreams");
      assertEquals("DIRECT:IDENTITY,INDIRECT:FILTER", ageEntry.getTransformOperation());
    }
  }

  @Test
  public void testCaptureSQLJobFacet() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(true);
    builder.captureColumnLevelLineage(true);
    builder.includeIndirectColumnLineage(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/sample_spark_with_sql_facet.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:file,/spark-test/people.parquet,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:file,/spark-test/result_test,DEV)",
          dataset.getUrn().toString());

      String transformOperation =
          Objects.requireNonNull(dataset.getLineage().getFineGrainedLineages())
              .get(0)
              .getTransformOperation();
      assertNotNull(transformOperation);
      // Format: "-- <transformations>\n<SQL>"
      assertTrue(
          transformOperation.contains("SELECT age, name FROM people WHERE age > 18"),
          "Transform operation should contain SQL query, got: " + transformOperation);
      assertTrue(
          transformOperation.contains("DIRECT:IDENTITY")
              && transformOperation.contains("INDIRECT:FILTER"),
          "Transform operation should contain both DIRECT and INDIRECT transformations, got: "
              + transformOperation);
      assertTrue(
          transformOperation.startsWith("-- ") && transformOperation.contains("\n"),
          "Transform operation should prefix transformations with '-- ' before SQL, got: "
              + transformOperation);
    }
  }

  @Test
  public void testFlinkJobEvent() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(false);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/flink_job_test.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:kafka,lineage-test-topic-json,DEV)",
          dataset.getUrn().toString());
    }
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:kafka,lineage-test-topic-json-flinkoutput,DEV)",
          dataset.getUrn().toString());
    }
  }

  @Test
  public void testDebeziumJobEvent() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.DEV);
    builder.lowerCaseDatasetUrns(true);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(false);
    builder.usePatch(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/debezium_event.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);

    assertEquals(0, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:kafka,debezium.public.product,DEV)",
          dataset.getUrn().toString());
    }
  }

  @Test
  public void testDatabricksMergeIntoStartEvent() throws URISyntaxException, IOException {
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder =
        DatahubOpenlineageConfig.builder();
    builder.fabricType(FabricType.PROD);
    builder.materializeDataset(true);
    builder.includeSchemaMetadata(true);
    builder.isSpark(true);

    String olEvent =
        IOUtils.toString(
            this.getClass().getResourceAsStream("/ol_events/databricks_mergeinto_start_event.json"),
            StandardCharsets.UTF_8);

    OpenLineage.RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(olEvent);
    DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, builder.build());

    assertNotNull(datahubJob);
    assertEquals("my-docuemnt-merge-job", datahubJob.getDataFlowInfo().getName());
    assertEquals("my-docuemnt-merge-job", datahubJob.getJobInfo().getName());

    assertEquals(1, datahubJob.getInSet().size());
    for (DatahubDataset dataset : datahubJob.getInSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:hive,documentraw.document,PROD)",
          dataset.getUrn().toString());
    }

    // This test verifies the bug: outputs should be present but the converter returns empty outSet
    // "Expected at least one output dataset but found none. This indicates the bug where outputs
    // are not being processed correctly for MERGE INTO START events."
    assertTrue(datahubJob.getOutSet().size() > 0);

    for (DatahubDataset dataset : datahubJob.getOutSet()) {
      assertEquals(
          "urn:li:dataset:(urn:li:dataPlatform:hive,documentraw.document,PROD)",
          dataset.getUrn().toString());
    }
  }
}
