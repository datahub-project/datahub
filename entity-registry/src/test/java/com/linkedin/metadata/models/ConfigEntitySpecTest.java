package com.linkedin.metadata.models;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/** Tests for {@link ConfigEntitySpec} */
public class ConfigEntitySpecTest {

  @Test
  public void testConfigEntitySpecWithSearchIndexGroup() {
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec("testEntity", "testKey", Arrays.asList(mockAspectSpec), "primary");

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
    assertEquals(entitySpec.getAspectSpecs().size(), 1);
    assertNotNull(entitySpec.getAspectSpec("testAspect"));
  }

  @Test
  public void testConfigEntitySpecWithDefaultSearchIndexGroup() {
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec(
            "testEntity",
            "testKey",
            Arrays.asList(mockAspectSpec),
            EntityAnnotation.DEFAULT_SEARCH_GROUP);

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
    assertEquals(entitySpec.getAspectSpecs().size(), 1);
    assertNotNull(entitySpec.getAspectSpec("testAspect"));
  }

  @Test
  public void testConfigEntitySpecWithTimeseriesGroup() {
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec("testEntity", "testKey", Arrays.asList(mockAspectSpec), "timeseries");

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "timeseries");
    assertEquals(entitySpec.getAspectSpecs().size(), 1);
    assertNotNull(entitySpec.getAspectSpec("testAspect"));
  }

  @Test
  public void testConfigEntitySpecWithCustomGroup() {
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec("testEntity", "testKey", Arrays.asList(mockAspectSpec), "customGroup");

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "customGroup");
    assertEquals(entitySpec.getAspectSpecs().size(), 1);
    assertNotNull(entitySpec.getAspectSpec("testAspect"));
  }

  @Test
  public void testConfigEntitySpecWithMultipleAspects() {
    AspectSpec mockAspectSpec1 = createMockAspectSpec("testAspect1");
    AspectSpec mockAspectSpec2 = createMockAspectSpec("testAspect2");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec(
            "testEntity", "testKey", Arrays.asList(mockAspectSpec1, mockAspectSpec2), "primary");

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
    assertEquals(entitySpec.getAspectSpecs().size(), 2);
    assertNotNull(entitySpec.getAspectSpec("testAspect1"));
    assertNotNull(entitySpec.getAspectSpec("testAspect2"));
  }

  @Test
  public void testConfigEntitySpecWithEmptyAspects() {
    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec("testEntity", "testKey", Collections.emptyList(), "primary");

    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
    assertEquals(entitySpec.getAspectSpecs().size(), 0);
  }

  @Test
  public void testConfigEntitySpecEntityAnnotation() {
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    ConfigEntitySpec entitySpec =
        new ConfigEntitySpec("testEntity", "testKey", Arrays.asList(mockAspectSpec), "primary");

    EntityAnnotation annotation = entitySpec.getEntityAnnotation();
    assertNotNull(annotation);
    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), "primary");
  }

  private AspectSpec createMockAspectSpec(String name) {
    return new AspectSpec(
        new com.linkedin.metadata.models.annotation.AspectAnnotation(name, false, false, null),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null);
  }
}
