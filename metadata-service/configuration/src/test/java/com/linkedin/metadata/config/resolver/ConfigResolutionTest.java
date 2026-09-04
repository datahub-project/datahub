package com.linkedin.metadata.config.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import org.testng.annotations.Test;

public class ConfigResolutionTest {

  @Test
  public void resolveReturnsTheStaticallyBoundValue() {
    assertTrue(
        ConfigResolution.resolve(
            OperationFingerprint.EMPTY, ConfigKeyConstants.VIEWS_ENABLED, true));
    assertEquals(
        ConfigResolution.resolve(
            OperationFingerprint.EMPTY, ConfigKeyConstants.SEARCH_BAR_API_VARIANT, "DEFAULT"),
        "DEFAULT");
  }

  /** Validates the generated constants carry the exact authored yaml spelling. */
  @Test
  public void generatedConstantsCarryExactYamlKeySpelling() {
    assertEquals(ConfigKeyConstants.VIEWS_ENABLED, "views.enabled");
    assertEquals(ConfigKeyConstants.SEARCH_BAR_API_VARIANT, "searchBar.apiVariant");
  }
}
