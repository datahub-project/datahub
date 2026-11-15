package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;
import org.testng.annotations.Test;

public class EntityChangeEventGeneratorRegistryTest {

  @Test
  public void testRegisteredAspectNames() {
    final EntityChangeEventGeneratorRegistry registry = new EntityChangeEventGeneratorRegistry();
    assertTrue(registry.getRegisteredAspectNames().isEmpty());

    registry.register("globalTags", new GlobalTagsChangeEventGenerator());
    registry.register("corpGroupKey", new EntityKeyChangeEventGenerator<>());
    registry.register("corpGroupKey", new EntityKeyChangeEventGenerator<>());

    assertEquals(registry.getRegisteredAspectNames(), Set.of("globalTags", "corpGroupKey"));
    assertEquals(registry.getEntityChangeEventGenerators("corpGroupKey").size(), 2);
  }
}
