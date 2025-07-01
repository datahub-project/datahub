package datahub.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import datahub.spark.conf.SparkLineageConf;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.datahubproject.openlineage.dataset.SparkDataset;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class HdfsPathDatasetTest {

  @Test
  public void testNoPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
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

    log.warn("Test log");
    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3://my-bucket/foo/tests/bar.avro"),
            sparkLineageConfBuilder.build().getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
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
                        ",", "s3a://wrong-my-bucket/foo/{table}", "s3a://my-bucket/foo/{table}"));
              }
            });
    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            datahubConfig, new SparkAppContext()));

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3a://my-bucket/foo/tests/bar.avro"),
            sparkLineageConfBuilder.build().getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testUrisWithPartitionRegexp()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    Config rawDatahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(SparkConfigParser.FILE_PARTITION_REGEXP_PATTERN, "year=.*/month=.*/day=.*");
              }
            });

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            rawDatahubConfig, new SparkAppContext()));
    DatahubOpenlineageConfig datahubConfig = sparkLineageConfBuilder.build().getOpenLineageConf();

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3://bucket-a/kafka_backup/my-table/year=2022/month=10/day=11/my-file.tx"),
            datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket-a/kafka_backup/my-table,PROD)",
        dataset.urn().toString());

    dataset =
        HdfsPathDataset.create(
            new URI("s3://bucket-b/kafka_backup/my-table/year=2023/month=11/day=23/my-file.tx"),
            datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket-b/kafka_backup/my-table,PROD)",
        dataset.urn().toString());

    dataset =
        HdfsPathDataset.create(
            new URI(
                "s3://bucket-c/my-backup/my-other-folder/my-table/year=2023/month=11/day=23/my-file.tx"),
            datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket-c/my-backup/my-other-folder/my-table,PROD)",
        dataset.urn().toString());

    dataset =
        HdfsPathDataset.create(
            new URI("s3://bucket-d/kafka_backup/my-table/non-partitioned/"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket-d/kafka_backup/my-table/non-partitioned,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testNoMatchPathSpecListWithFolder()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    Config rawDatahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });
    String gcsPath =
        "gcs://gcs-spike-standard-offerwall-dev-useast1/events_creation_timestamp_enhanced";
    String expectedUrn =
        "urn:li:dataset:(urn:li:dataPlatform:gcs,gcs-spike-standard-offerwall-dev-useast1/events_creation_timestamp_enhanced,PROD)";

    SparkLineageConf.SparkLineageConfBuilder sparkLineageConfBuilder = SparkLineageConf.builder();
    sparkLineageConfBuilder.openLineageConf(
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
            rawDatahubConfig, new SparkAppContext()));
    DatahubOpenlineageConfig datahubConfig = sparkLineageConfBuilder.build().getOpenLineageConf();

    SparkDataset dataset = HdfsPathDataset.create(new URI(gcsPath), datahubConfig);
    Assert.assertEquals(expectedUrn, dataset.urn().toString());
  }

  @Test
  public void testNoMatchPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
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
                    String.join(",", "s3a://wrong-my-bucket/foo/{table}"));
              }
            });
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);
    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3a://my-bucket/foo/tests/bar.avro"), sparkLineageConf.getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathSpecListPlatformInstance()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
                put(SparkConfigParser.DATASET_PLATFORM_INSTANCE_KEY, "instance");
                put(
                    SparkConfigParser.PLATFORM_KEY
                        + "."
                        + "s3"
                        + "."
                        + SparkConfigParser.PATH_SPEC_LIST_KEY,
                    String.join(
                        ",", "s3a://wrong-my-bucket/foo/{table}", "s3a://my-bucket/foo/{table}"));
              }
            });
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3a://my-bucket/foo/tests/bar.avro"), sparkLineageConf.getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathAliasList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
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
                    String.join(",", "s3a://my-bucket/{table}"));
              }
            });
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("s3a://my-bucket/foo/tests/bar.avro"), sparkLineageConf.getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo,PROD)", dataset.urn().toString());
  }

  // ====================================================================
  // GCS tests
  // ====================================================================
  @Test
  public void testGcsNoPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    Config datahubConfig =
        ConfigFactory.parseMap(
            new HashMap<String, Object>() {
              {
                put(SparkConfigParser.DATASET_ENV_KEY, "PROD");
              }
            });
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("gs://my-bucket/foo/tests/bar.avro"), sparkLineageConf.getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testGcsPathSpecList()
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
                    String.join(
                        ",", "s3a://wrong-my-bucket/foo/{table}", "gs://my-bucket/foo/{table}"));
              }
            });
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);

    SparkDataset dataset =
        HdfsPathDataset.create(
            new URI("gs://my-bucket/foo/tests/bar.avro"), sparkLineageConf.getOpenLineageConf());
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }
}
