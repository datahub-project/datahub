package com.linkedin.metadata.structuredproperties.validators;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.structuredproperties.validation.HidePropertyValidator;
import com.linkedin.structured.StructuredPropertySettings;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HidePropertyValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn TEST_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

  @Test
  public void testValidUpsert() {

    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setIsHidden(false)
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    boolean isValid =
        HidePropertyValidator.validateSettingsUpserts(
                TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY))
            .findAny()
            .isEmpty();
    Assert.assertTrue(isValid);
  }

  @Test
  public void testInvalidUpsert() {

    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setIsHidden(true)
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    boolean isValid =
        HidePropertyValidator.validateSettingsUpserts(
                TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY))
            .findAny()
            .isEmpty();
    Assert.assertFalse(isValid);
  }
}
