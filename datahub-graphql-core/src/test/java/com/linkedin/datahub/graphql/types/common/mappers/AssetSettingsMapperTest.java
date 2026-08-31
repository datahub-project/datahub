package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.AssetSettings;
import com.linkedin.datahub.graphql.generated.AssetSummarySettings;
import com.linkedin.datahub.graphql.generated.AssetSummarySettingsTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.settings.asset.AssetSummarySettingsTemplateArray;
import org.testng.annotations.Test;

public class AssetSettingsMapperTest {

  private static final String TEST_TEMPLATE_URN = "urn:li:dataHubPageTemplate:testTemplate";
  private static final String TEST_TEMPLATE_URN_2 = "urn:li:dataHubPageTemplate:testTemplate2";

  @Test
  public void testMapWithNullAssetSummary() {
    // Create GMS AssetSettings with no assetSummary set (defaults to null)
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        new com.linkedin.settings.asset.AssetSettings();
    // Don't set assetSummary - it will be null by default

    // Test static map method
    AssetSettings result = AssetSettingsMapper.map(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNull(result.getAssetSummary());
  }

  @Test
  public void testMapWithEmptyAssetSummary() {
    // Create GMS AssetSettings with empty assetSummary (no templates set)
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        new com.linkedin.settings.asset.AssetSettings();
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        new com.linkedin.settings.asset.AssetSummarySettings();
    // Don't set templates - it will be null by default
    gmsSettings.setAssetSummary(gmsAssetSummary);

    // Test static map method
    AssetSettings result = AssetSettingsMapper.map(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getAssetSummary());
    assertNotNull(result.getAssetSummary().getTemplates());
    assertEquals(result.getAssetSummary().getTemplates().size(), 0);
  }

  @Test
  public void testMapWithSingleTemplate() {
    // Create GMS AssetSettings with single template
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        createGmsAssetSettingsWithSingleTemplate();

    // Test static map method
    AssetSettings result = AssetSettingsMapper.map(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getAssetSummary());
    assertNotNull(result.getAssetSummary().getTemplates());
    assertEquals(result.getAssetSummary().getTemplates().size(), 1);

    AssetSummarySettingsTemplate template = result.getAssetSummary().getTemplates().get(0);
    assertNotNull(template);
    assertNotNull(template.getTemplate());
    assertEquals(template.getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(template.getTemplate().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testMapWithMultipleTemplates() {
    // Create GMS AssetSettings with multiple templates
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        createGmsAssetSettingsWithMultipleTemplates();

    // Test static map method
    AssetSettings result = AssetSettingsMapper.map(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getAssetSummary());
    assertNotNull(result.getAssetSummary().getTemplates());
    assertEquals(result.getAssetSummary().getTemplates().size(), 2);

    // Verify first template
    AssetSummarySettingsTemplate template1 = result.getAssetSummary().getTemplates().get(0);
    assertNotNull(template1);
    assertNotNull(template1.getTemplate());
    assertEquals(template1.getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(template1.getTemplate().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);

    // Verify second template
    AssetSummarySettingsTemplate template2 = result.getAssetSummary().getTemplates().get(1);
    assertNotNull(template2);
    assertNotNull(template2.getTemplate());
    assertEquals(template2.getTemplate().getUrn(), TEST_TEMPLATE_URN_2);
    assertEquals(template2.getTemplate().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testMapWithEmptyTemplateArray() {
    // Create GMS AssetSettings with empty template array
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        new com.linkedin.settings.asset.AssetSettings();
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        new com.linkedin.settings.asset.AssetSummarySettings();
    AssetSummarySettingsTemplateArray emptyTemplates = new AssetSummarySettingsTemplateArray();
    gmsAssetSummary.setTemplates(emptyTemplates);
    gmsSettings.setAssetSummary(gmsAssetSummary);

    // Test static map method
    AssetSettings result = AssetSettingsMapper.map(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getAssetSummary());
    assertNotNull(result.getAssetSummary().getTemplates());
    assertEquals(result.getAssetSummary().getTemplates().size(), 0);
  }

  @Test
  public void testApplyMethod() {
    // Create GMS AssetSettings
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        createGmsAssetSettingsWithSingleTemplate();

    // Test instance apply method
    AssetSettings result = AssetSettingsMapper.INSTANCE.apply(gmsSettings);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getAssetSummary());
    assertNotNull(result.getAssetSummary().getTemplates());
    assertEquals(result.getAssetSummary().getTemplates().size(), 1);

    AssetSummarySettingsTemplate template = result.getAssetSummary().getTemplates().get(0);
    assertNotNull(template);
    assertNotNull(template.getTemplate());
    assertEquals(template.getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(template.getTemplate().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testStaticMapMethodWithNullThrowsException() {
    // Test that passing null to static map method throws exception
    assertThrows(
        NullPointerException.class,
        () -> {
          AssetSettingsMapper.map(null);
        });
  }

  @Test
  public void testApplyMethodWithNullThrowsException() {
    // Test that passing null to apply method throws exception
    assertThrows(
        NullPointerException.class,
        () -> {
          AssetSettingsMapper.INSTANCE.apply(null);
        });
  }

  @Test
  public void testMapperInstanceSingleton() {
    // Test that INSTANCE is properly initialized
    assertNotNull(AssetSettingsMapper.INSTANCE);

    // Test that multiple calls return same instance
    assertSame(AssetSettingsMapper.INSTANCE, AssetSettingsMapper.INSTANCE);
  }

  @Test
  public void testMapAssetSummarySettingsReflection() throws Exception {
    // Create GMS AssetSummarySettings
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        createGmsAssetSummarySettings();

    // Use reflection to test the private mapAssetSummarySettings method
    java.lang.reflect.Method method =
        AssetSettingsMapper.class.getDeclaredMethod(
            "mapAssetSummarySettings", com.linkedin.settings.asset.AssetSummarySettings.class);
    method.setAccessible(true);

    AssetSummarySettings result =
        (AssetSummarySettings) method.invoke(AssetSettingsMapper.INSTANCE, gmsAssetSummary);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getTemplates());
    assertEquals(result.getTemplates().size(), 1);

    AssetSummarySettingsTemplate template = result.getTemplates().get(0);
    assertNotNull(template);
    assertNotNull(template.getTemplate());
    assertEquals(template.getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(template.getTemplate().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testMapAssetSummarySettingsWithNullTemplatesReflection() throws Exception {
    // Create GMS AssetSummarySettings with no templates set (defaults to null)
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        new com.linkedin.settings.asset.AssetSummarySettings();
    // Don't set templates - it will be null by default

    // Use reflection to test the private mapAssetSummarySettings method
    java.lang.reflect.Method method =
        AssetSettingsMapper.class.getDeclaredMethod(
            "mapAssetSummarySettings", com.linkedin.settings.asset.AssetSummarySettings.class);
    method.setAccessible(true);

    AssetSummarySettings result =
        (AssetSummarySettings) method.invoke(AssetSettingsMapper.INSTANCE, gmsAssetSummary);

    // Verify result
    assertNotNull(result);
    assertNotNull(result.getTemplates());
    assertEquals(result.getTemplates().size(), 0);
  }

  @Test
  public void testComplexScenarioWithMultipleOperations() {
    // Test a complex scenario with multiple operations
    com.linkedin.settings.asset.AssetSettings gmsSettings1 =
        createGmsAssetSettingsWithSingleTemplate();
    com.linkedin.settings.asset.AssetSettings gmsSettings2 =
        createGmsAssetSettingsWithMultipleTemplates();

    // Map both settings
    AssetSettings result1 = AssetSettingsMapper.map(gmsSettings1);
    AssetSettings result2 = AssetSettingsMapper.map(gmsSettings2);

    // Verify results are independent
    assertNotNull(result1);
    assertNotNull(result2);
    assertEquals(result1.getAssetSummary().getTemplates().size(), 1);
    assertEquals(result2.getAssetSummary().getTemplates().size(), 2);

    // Verify template URNs
    assertEquals(
        result1.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(
        result2.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(), TEST_TEMPLATE_URN);
    assertEquals(
        result2.getAssetSummary().getTemplates().get(1).getTemplate().getUrn(),
        TEST_TEMPLATE_URN_2);
  }

  @Test
  public void testTemplateUrnHandling() {
    // Test with various URN formats
    String[] testUrns = {
      "urn:li:dataHubPageTemplate:simple",
      "urn:li:dataHubPageTemplate:complex-name_with.special-chars",
      "urn:li:dataHubPageTemplate:123456789"
    };

    for (String testUrn : testUrns) {
      // Create GMS AssetSettings with test URN
      com.linkedin.settings.asset.AssetSettings gmsSettings =
          new com.linkedin.settings.asset.AssetSettings();
      com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
          new com.linkedin.settings.asset.AssetSummarySettings();
      AssetSummarySettingsTemplateArray templates = new AssetSummarySettingsTemplateArray();
      com.linkedin.settings.asset.AssetSummarySettingsTemplate gmsTemplate =
          new com.linkedin.settings.asset.AssetSummarySettingsTemplate();
      gmsTemplate.setTemplate(UrnUtils.getUrn(testUrn));
      templates.add(gmsTemplate);
      gmsAssetSummary.setTemplates(templates);
      gmsSettings.setAssetSummary(gmsAssetSummary);

      // Test mapping
      AssetSettings result = AssetSettingsMapper.map(gmsSettings);

      // Verify result
      assertNotNull(result);
      assertNotNull(result.getAssetSummary());
      assertNotNull(result.getAssetSummary().getTemplates());
      assertEquals(result.getAssetSummary().getTemplates().size(), 1);
      assertEquals(result.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(), testUrn);
      assertEquals(
          result.getAssetSummary().getTemplates().get(0).getTemplate().getType(),
          EntityType.DATAHUB_PAGE_TEMPLATE);
    }
  }

  // Helper methods

  private com.linkedin.settings.asset.AssetSettings createGmsAssetSettingsWithSingleTemplate() {
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        new com.linkedin.settings.asset.AssetSettings();
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        createGmsAssetSummarySettings();
    gmsSettings.setAssetSummary(gmsAssetSummary);
    return gmsSettings;
  }

  private com.linkedin.settings.asset.AssetSettings createGmsAssetSettingsWithMultipleTemplates() {
    com.linkedin.settings.asset.AssetSettings gmsSettings =
        new com.linkedin.settings.asset.AssetSettings();
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        new com.linkedin.settings.asset.AssetSummarySettings();

    AssetSummarySettingsTemplateArray templates = new AssetSummarySettingsTemplateArray();

    // First template
    com.linkedin.settings.asset.AssetSummarySettingsTemplate gmsTemplate1 =
        new com.linkedin.settings.asset.AssetSummarySettingsTemplate();
    gmsTemplate1.setTemplate(UrnUtils.getUrn(TEST_TEMPLATE_URN));
    templates.add(gmsTemplate1);

    // Second template
    com.linkedin.settings.asset.AssetSummarySettingsTemplate gmsTemplate2 =
        new com.linkedin.settings.asset.AssetSummarySettingsTemplate();
    gmsTemplate2.setTemplate(UrnUtils.getUrn(TEST_TEMPLATE_URN_2));
    templates.add(gmsTemplate2);

    gmsAssetSummary.setTemplates(templates);
    gmsSettings.setAssetSummary(gmsAssetSummary);
    return gmsSettings;
  }

  private com.linkedin.settings.asset.AssetSummarySettings createGmsAssetSummarySettings() {
    com.linkedin.settings.asset.AssetSummarySettings gmsAssetSummary =
        new com.linkedin.settings.asset.AssetSummarySettings();
    AssetSummarySettingsTemplateArray templates = new AssetSummarySettingsTemplateArray();
    com.linkedin.settings.asset.AssetSummarySettingsTemplate gmsTemplate =
        new com.linkedin.settings.asset.AssetSummarySettingsTemplate();
    gmsTemplate.setTemplate(UrnUtils.getUrn(TEST_TEMPLATE_URN));
    templates.add(gmsTemplate);
    gmsAssetSummary.setTemplates(templates);
    return gmsAssetSummary;
  }
}
