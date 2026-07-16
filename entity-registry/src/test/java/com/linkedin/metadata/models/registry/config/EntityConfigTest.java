package com.linkedin.metadata.models.registry.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/** Tests for {@link Entity} config class */
public class EntityConfigTest {

  @Test
  public void testEntityWithSearchIndexGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1", "aspect2"),
            "core",
            "search");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getDoc(), "Test entity description");
    assertEquals(entity.getKeyAspect(), "testKey");
    assertEquals(entity.getAspects(), Arrays.asList("aspect1", "aspect2"));
    assertEquals(entity.getCategory(), "core");
    assertEquals(entity.getSearchGroup(), "search");
  }

  @Test
  public void testEntityWithTimeseriesGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            "timeseries");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getSearchGroup(), "timeseries");
  }

  @Test
  public void testEntityWithCustomGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            "customGroup");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getSearchGroup(), "customGroup");
  }

  @Test
  public void testEntityWithoutSearchIndexGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            null);

    assertEquals(entity.getName(), "testEntity");
    assertNull(entity.getSearchGroup());
  }

  @Test
  public void testEntityWithDefaultGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            "default");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getSearchGroup(), "default");
  }

  @Test
  public void testEntityWithQueryGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            "query");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getSearchGroup(), "query");
  }

  @Test
  public void testEntityWithSchemaFieldGroup() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            "core",
            "schemaField");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getSearchGroup(), "schemaField");
  }

  @Test
  public void testEntityWithEmptyAspects() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Collections.emptyList(),
            "core",
            "search");

    assertEquals(entity.getName(), "testEntity");
    assertEquals(entity.getAspects().size(), 0);
    assertEquals(entity.getSearchGroup(), "search");
  }

  @Test
  public void testEntityWithoutCategory() {
    Entity entity =
        new Entity(
            "testEntity",
            "Test entity description",
            "testKey",
            Arrays.asList("aspect1"),
            null,
            "search");

    assertEquals(entity.getName(), "testEntity");
    assertNull(entity.getCategory());
    assertEquals(entity.getSearchGroup(), "search");
  }

  @Test
  public void testEntityWithoutDoc() {
    Entity entity =
        new Entity("testEntity", null, "testKey", Arrays.asList("aspect1"), "core", "search");

    assertEquals(entity.getName(), "testEntity");
    assertNull(entity.getDoc());
    assertEquals(entity.getSearchGroup(), "search");
  }
}
