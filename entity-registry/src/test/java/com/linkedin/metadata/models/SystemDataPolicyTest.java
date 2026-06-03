package com.linkedin.metadata.models;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.annotation.SystemAnnotation;
import com.linkedin.metadata.models.annotation.SystemDataVisibility;
import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.policy.SystemDataPolicy;
import com.linkedin.test.metadata.models.SystemDataEntityFixture;
import com.linkedin.test.metadata.models.SystemDataEntityFixture.EntityFixture;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SystemDataPolicyTest {

  @Test(expectedExceptions = ModelValidationException.class)
  public void testSystemAspectOnNonSystemEntityFailsValidation() {
    EntityFixture fixture = SystemDataEntityFixture.normalEntity("normal");
    fixture
        .getValueAspect()
        .setSystemAnnotation(new SystemAnnotation(SystemDataVisibility.fullyHidden()));
    SystemDataPolicy.validateSystemAspectAnnotations(fixture.getEntitySpec());
  }

  @Test
  public void testSystemEntityValidationPasses() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    SystemDataPolicy.validateSystemAspectAnnotations(fixture.getEntitySpec());
  }

  @Test(expectedExceptions = ModelValidationException.class)
  public void testSystemEntityAnnotationOnNonKeyAspectFailsValidation() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    fixture
        .getValueAspect()
        .setSystemEntityAnnotation(new SystemEntityAnnotation(SystemDataVisibility.fullyHidden()));
    SystemDataPolicy.validateSystemAspectAnnotations(fixture.getEntitySpec());
  }

  @Test
  public void testEffectiveAllowReadUsesAspectOverride() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    fixture
        .getValueAspect()
        .setSystemAnnotation(new SystemAnnotation(new SystemDataVisibility(true, true, false)));
    assertTrue(
        SystemDataPolicy.effectiveAllowRead(fixture.getEntitySpec(), fixture.getValueAspect()));
  }

  @Test
  public void testEffectiveAllowExistsUsesAspectOverride() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    fixture
        .getValueAspect()
        .setSystemAnnotation(new SystemAnnotation(new SystemDataVisibility(true, false, true)));
    assertTrue(
        SystemDataPolicy.effectiveAllowExists(fixture.getEntitySpec(), fixture.getValueAspect()));
  }

  @Test
  public void testEffectiveAllowExistsImpliedByAllowRead() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", true, false);
    assertTrue(
        SystemDataPolicy.effectiveAllowExists(fixture.getEntitySpec(), fixture.getValueAspect()));
  }

  @Test
  public void testEffectiveAllowExistsImpliedByAspectAllowRead() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    fixture
        .getValueAspect()
        .setSystemAnnotation(new SystemAnnotation(new SystemDataVisibility(true, true, false)));
    assertTrue(
        SystemDataPolicy.effectiveAllowExists(fixture.getEntitySpec(), fixture.getValueAspect()));
    assertFalse(
        SystemDataPolicy.effectiveAllowRead(
            fixture.getEntitySpec(), fixture.getEntitySpec().getKeyAspectSpec()));
  }

  @Test
  public void testEffectiveAllowExistsWithoutReadUsesExplicitFlag() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", false, false);
    fixture
        .getValueAspect()
        .setSystemAnnotation(new SystemAnnotation(new SystemDataVisibility(true, false, true)));
    assertTrue(
        SystemDataPolicy.effectiveAllowExists(fixture.getEntitySpec(), fixture.getValueAspect()));
    assertFalse(
        SystemDataPolicy.effectiveAllowRead(fixture.getEntitySpec(), fixture.getValueAspect()));
  }

  @Test
  public void testEffectiveAllowReadInheritsEntityPolicy() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", true, false);
    assertTrue(
        SystemDataPolicy.effectiveAllowRead(fixture.getEntitySpec(), fixture.getValueAspect()));
  }

  @Test
  public void testResolveEntityNamesForReadOmitsHiddenSystemEntitiesForBulkScroll() {
    EntityFixture hidden = SystemDataEntityFixture.systemEntity("hiddenSystem", false, false);
    EntityFixture visible = SystemDataEntityFixture.systemEntity("visibleSystem", true, false);
    EntityFixture normal = SystemDataEntityFixture.normalEntity("dataset");
    EntityRegistry registry = org.mockito.Mockito.mock(EntityRegistry.class);
    org.mockito.Mockito.when(registry.getEntitySpecs())
        .thenReturn(
            Map.of(
                "hiddensystem", hidden.getEntitySpec(),
                "visiblesystem", visible.getEntitySpec(),
                "dataset", normal.getEntitySpec()));
    org.mockito.Mockito.when(registry.getEntitySpec("hiddenSystem"))
        .thenReturn(hidden.getEntitySpec());

    var filtered = SystemDataPolicy.resolveEntityNamesForRead(registry, null, true);
    assertTrue(filtered.contains("dataset"));
    assertTrue(filtered.contains("visibleSystem"));
    assertFalse(filtered.contains("hiddenSystem"));

    var unfiltered = SystemDataPolicy.resolveEntityNamesForRead(registry, null, false);
    assertTrue(unfiltered.contains("hiddenSystem"));

    var explicit =
        SystemDataPolicy.resolveEntityNamesForRead(registry, Set.of("hiddenSystem"), true);
    assertTrue(explicit.contains("hiddenSystem"));
  }
}
