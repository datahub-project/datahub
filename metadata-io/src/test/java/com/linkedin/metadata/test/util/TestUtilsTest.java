package com.linkedin.metadata.test.util;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TestUtilsTest {

  @Test
  public void testGetSupportedEntityTypes() {
    EntityRegistry registry =
        new ConfigEntityRegistry(
            TestUtilsTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    Assert.assertEquals(ImmutableSet.of("testEntity"), TestUtils.getSupportedEntityTypes(registry));
  }
}
