package io.datahubproject.openlineage.converter;

import static org.junit.jupiter.api.Assertions.assertFalse;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import org.junit.jupiter.api.Test;

/** Placeholder test for operation type functionality */
public class ExtractOperationTypeTest {

  @Test
  public void testLegacyJobNameConfig() {
    // Test default value
    DatahubOpenlineageConfig config = DatahubOpenlineageConfig.builder().build();
    assertFalse(config.isUseLegacyJobNames(), "useLegacyJobNames should be false by default");

    // Test explicitly setting value
    config = DatahubOpenlineageConfig.builder().useLegacyJobNames(true).build();

    assert (config.isUseLegacyJobNames() == true);
  }
}
