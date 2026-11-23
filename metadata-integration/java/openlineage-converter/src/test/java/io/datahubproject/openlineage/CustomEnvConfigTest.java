package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.common.FabricType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import java.net.URI;
import java.util.Optional;
import org.testng.annotations.Test;

/**
 * C7: the config's env is held as a string. The FabricType builder methods and getters behave as
 * before for built-in values; a custom environment can be set explicitly with env(String).
 */
public class CustomEnvConfigTest {

  @Test
  public void testFabricTypeApiBehavesAsBefore() {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.QA).build();
    assertSame(config.getFabricType(), FabricType.QA);
    assertEquals(config.getEnv(), "QA");

    DatahubOpenlineageConfig defaults = DatahubOpenlineageConfig.builder().build();
    assertSame(defaults.getFabricType(), FabricType.PROD);
    assertEquals(defaults.getEnv(), "PROD");

    ConnectionInstanceDetail detail =
        ConnectionInstanceDetail.builder().env(Optional.of(FabricType.DEV)).build();
    assertEquals(detail.getEnv(), Optional.of(FabricType.DEV));
    assertEquals(detail.getEnvName(), Optional.of("DEV"));
    assertEquals(ConnectionInstanceDetail.builder().build().getEnv(), Optional.empty());
  }

  @Test
  public void testExplicitCustomEnvReachesDatasetUrns() throws Exception {
    DatahubOpenlineageConfig config = DatahubOpenlineageConfig.builder().env("MY_ENV").build();
    assertSame(config.getFabricType(), FabricType.$UNKNOWN);

    HdfsPathDataset dataset = HdfsPathDataset.create(new URI("s3://bucket/path/table"), config);

    assertEquals(dataset.urn().getEnv(), "MY_ENV");
  }
}
