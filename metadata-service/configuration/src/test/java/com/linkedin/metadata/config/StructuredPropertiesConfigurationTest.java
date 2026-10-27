package com.linkedin.metadata.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertiesConfigurationTest {

  @Test
  public void testKeywordMaxLength_Configurable() {
    StructuredPropertiesConfiguration fromBuilder =
        StructuredPropertiesConfiguration.builder().keywordMaxLength(1024).build();
    Assert.assertEquals(fromBuilder.getKeywordMaxLength(), 1024);

    StructuredPropertiesConfiguration fromSetter = new StructuredPropertiesConfiguration();
    fromSetter.setKeywordMaxLength(2048);
    Assert.assertEquals(fromSetter.getKeywordMaxLength(), 2048);
  }

  @Test
  public void testDropOversizedKeywordValuesFromIndex_DefaultFalse() {
    StructuredPropertiesConfiguration defaults = new StructuredPropertiesConfiguration();
    Assert.assertFalse(defaults.isDropOversizedKeywordValuesFromIndex());

    StructuredPropertiesConfiguration enabled =
        StructuredPropertiesConfiguration.builder()
            .dropOversizedKeywordValuesFromIndex(true)
            .build();
    Assert.assertTrue(enabled.isDropOversizedKeywordValuesFromIndex());
  }
}
