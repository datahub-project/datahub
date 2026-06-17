package com.linkedin.metadata.models;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.filter.SystemDataReadFilter;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadContext;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.models.annotation.SystemAnnotation;
import com.linkedin.metadata.models.annotation.SystemDataVisibility;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.models.SystemDataEntityFixture;
import com.linkedin.test.metadata.models.SystemDataEntityFixture.EntityFixture;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemDataReadFilterTest {

  private SystemDataReadFilter filter;
  private EntityRegistry entityRegistry;

  @BeforeMethod
  public void setup() {
    filter =
        new SystemDataReadFilter()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(SystemDataReadFilter.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("READ", "EXISTS"))
                    .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                    .build());
    entityRegistry = Mockito.mock(EntityRegistry.class);
  }

  @Test
  public void testDisabledFilterDoesNotApply() {
    filter.setConfig(
        AspectPluginConfig.builder()
            .className(SystemDataReadFilter.class.getName())
            .enabled(false)
            .supportedOperations(List.of("READ", "EXISTS"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build());

    assertFalse(filter.shouldApply(ReadIntent.READ, "system", "valueAspect"));
  }

  @Test
  public void testReadDeniedForHiddenSystemEntity() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    stubRegistry("system", fixture);

    assertFalse(filter.isAllowed(readContext("urn:li:system:abc", "valueAspect"), entityRegistry));
  }

  @Test
  public void testReadAllowedForPolicyEligibleSystemEntity() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", true, false);
    stubRegistry("system", fixture);

    assertTrue(filter.isAllowed(readContext("urn:li:system:abc", "valueAspect"), entityRegistry));
  }

  @Test
  public void testExistsDeniedForHiddenSystemEntityKeyAspect() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    stubRegistry("system", fixture);

    assertFalse(filter.isAllowed(existsContext("urn:li:system:abc", "keyAspect"), entityRegistry));
  }

  @Test
  public void testExistsAllowedWhenEntityAllowsReadOnly() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", true, false);
    stubRegistry("system", fixture);

    assertTrue(filter.isAllowed(existsContext("urn:li:system:abc", "keyAspect"), entityRegistry));
  }

  @Test
  public void testExistsAllowedWhenEntityAllowsExists() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, true);
    stubRegistry("system", fixture);

    assertTrue(filter.isAllowed(existsContext("urn:li:system:abc", "keyAspect"), entityRegistry));
  }

  @Test
  public void testExistsAllowedForAspectWithAllowExistsOverride() {
    EntityFixture fixture =
        SystemDataEntityFixture.systemEntity(
            "system",
            false,
            false,
            new SystemAnnotation(new SystemDataVisibility(true, false, true)));
    stubRegistry("system", fixture);

    assertFalse(filter.isAllowed(existsContext("urn:li:system:abc", "keyAspect"), entityRegistry));
    assertTrue(filter.isAllowed(existsContext("urn:li:system:abc", "valueAspect"), entityRegistry));
  }

  @Test
  public void testExistsAllowedForNonKeyAspectOnNormalEntity() {
    EntityFixture fixture = SystemDataEntityFixture.normalEntity("normal");
    stubRegistry("normal", fixture);

    assertTrue(filter.isAllowed(existsContext("urn:li:normal:abc", "valueAspect"), entityRegistry));
  }

  @Test
  public void testNormalEntityAlwaysAllowedForRead() {
    EntityFixture fixture = SystemDataEntityFixture.normalEntity("normal");
    stubRegistry("normal", fixture);

    assertTrue(filter.isAllowed(readContext("urn:li:normal:abc", "valueAspect"), entityRegistry));
  }

  private void stubRegistry(String entityName, EntityFixture fixture) {
    Mockito.when(entityRegistry.getEntitySpecs())
        .thenReturn(Map.of(entityName, fixture.getEntitySpec()));
    Mockito.when(entityRegistry.getEntitySpec(entityName)).thenReturn(fixture.getEntitySpec());
  }

  private static AspectReadContext readContext(String urn, String aspectName) {
    return AspectReadContext.builder()
        .urn(UrnUtils.getUrn(urn))
        .aspectName(aspectName)
        .intent(ReadIntent.READ)
        .build();
  }

  private static AspectReadContext existsContext(String urn, String aspectName) {
    return AspectReadContext.builder()
        .urn(UrnUtils.getUrn(urn))
        .aspectName(aspectName)
        .intent(ReadIntent.EXISTS)
        .build();
  }
}
