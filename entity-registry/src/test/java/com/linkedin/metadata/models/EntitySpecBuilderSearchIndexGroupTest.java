package com.linkedin.metadata.models;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/** Tests for {@link EntitySpecBuilder} searchGroup functionality */
public class EntitySpecBuilderSearchIndexGroupTest {

  @Test
  public void testBuildConfigEntitySpecWithSearchIndexGroup() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec(
            "testEntity", "testKey", Arrays.asList(mockAspectSpec), "primary");

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
  }

  @Test
  public void testBuildConfigEntitySpecWithDefaultSearchIndexGroup() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec(
            "testEntity",
            "testKey",
            Arrays.asList(mockAspectSpec),
            EntityAnnotation.DEFAULT_SEARCH_GROUP);

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
  }

  @Test
  public void testBuildConfigEntitySpecWithTimeseriesGroup() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec(
            "testEntity", "testKey", Arrays.asList(mockAspectSpec), "timeseries");

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "timeseries");
  }

  @Test
  public void testBuildConfigEntitySpecWithCustomGroup() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec(
            "testEntity", "testKey", Arrays.asList(mockAspectSpec), "customGroup");

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "customGroup");
  }

  @Test
  public void testBuildConfigEntitySpecWithEmptyAspects() {
    EntitySpecBuilder builder = new EntitySpecBuilder();

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec("testEntity", "testKey", Collections.emptyList(), "primary");

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
    assertEquals(entitySpec.getAspectSpecs().size(), 0);
  }

  @Test
  public void testBuildConfigEntitySpecWithMultipleAspects() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec1 = createMockAspectSpec("testAspect1");
    AspectSpec mockAspectSpec2 = createMockAspectSpec("testAspect2");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec(
            "testEntity", "testKey", Arrays.asList(mockAspectSpec1, mockAspectSpec2), "primary");

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), "primary");
    assertEquals(entitySpec.getAspectSpecs().size(), 2);
  }

  @Test
  public void testBuildConfigEntitySpecWithNullSearchIndexGroup() {
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec mockAspectSpec = createMockAspectSpec("testAspect");

    EntitySpec entitySpec =
        builder.buildConfigEntitySpec("testEntity", "testKey", Arrays.asList(mockAspectSpec), null);

    assertNotNull(entitySpec);
    assertEquals(entitySpec.getName(), "testEntity");
    assertEquals(entitySpec.getKeyAspectName(), "testKey");
    assertEquals(entitySpec.getSearchGroup(), null);
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
