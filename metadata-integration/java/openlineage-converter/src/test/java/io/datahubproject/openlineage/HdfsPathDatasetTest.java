package io.datahubproject.openlineage;

import com.linkedin.common.FabricType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.datahubproject.openlineage.dataset.PathSpec;
import io.datahubproject.openlineage.dataset.SparkDataset;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HdfsPathDatasetTest {

  @Test
  public void testNoPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3://my-bucket/foo/tests/bar.avro"), config);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("s3")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Arrays.asList(
                                            "s3a://wrong-my-bucket/foo/{table}",
                                            "s3a://my-bucket/foo/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3a://my-bucket/foo/tests/bar.avro"), config);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testNoMatchPathSpecListWithFolder()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();

    String gcsPath =
        "gcs://gcs-spike-standard-offerwall-dev-useast1/events_creation_timestamp_enhanced";
    String expectedUrn =
        "urn:li:dataset:(urn:li:dataPlatform:gcs,gcs-spike-standard-offerwall-dev-useast1/events_creation_timestamp_enhanced,PROD)";

    SparkDataset dataset = HdfsPathDataset.create(new URI(gcsPath), datahubConfig);
    Assert.assertEquals(expectedUrn, dataset.urn().toString());
  }

  @Test
  public void testNoMatchPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("s3")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Collections.singletonList(
                                            "s3a://wrong-my-bucket/foo/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3a://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathSpecListPlatformInstance()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .commonDatasetPlatformInstance("instance")
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("s3")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Arrays.asList(
                                            "s3a://wrong-my-bucket/foo/{table}",
                                            "s3a://my-bucket/foo/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3a://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathSpecListPathSpecPlatformInstance()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .commonDatasetPlatformInstance("instance")
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("s3")
                                .platformInstance(Optional.of("s3Instance"))
                                .pathSpecList(
                                    new LinkedList<>(
                                        Arrays.asList(
                                            "s3a://wrong-my-bucket/foo/{table}",
                                            "s3a://my-bucket/foo/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3a://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,s3Instance.my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testPathAliasList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("s3")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Collections.singletonList("s3a://my-bucket/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("s3a://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/foo,PROD)", dataset.urn().toString());
  }

  // ====================================================================
  // GCS tests
  // ====================================================================
  @Test
  public void testGcsNoPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("gcs")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Arrays.asList("s3a://wrong-my-bucket/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("gs://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)",
        dataset.urn().toString());
  }

  @Test
  public void testGcsPathSpecList()
      throws InstantiationException, IllegalArgumentException, URISyntaxException {
    DatahubOpenlineageConfig datahubConfig =
        DatahubOpenlineageConfig.builder()
            .pathSpecs(
                new HashMap<String, List<PathSpec>>() {
                  {
                    put(
                        "s3",
                        Collections.singletonList(
                            PathSpec.builder()
                                .env(Optional.of("PROD"))
                                .platform("gcs")
                                .pathSpecList(
                                    new LinkedList<>(
                                        Arrays.asList(
                                            "s3a://wrong-my-bucket/foo/{table}",
                                            "gs://my-bucket/foo/{table}")))
                                .build()));
                  }
                })
            .fabricType(FabricType.PROD)
            .build();

    SparkDataset dataset =
        HdfsPathDataset.create(new URI("gs://my-bucket/foo/tests/bar.avro"), datahubConfig);
    Assert.assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests,PROD)",
        dataset.urn().toString());
  }
}
