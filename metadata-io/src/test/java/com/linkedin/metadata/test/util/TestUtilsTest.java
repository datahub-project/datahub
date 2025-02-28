package com.linkedin.metadata.test.util;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TestUtilsTest {

  @Test
  public void testGetSupportedEntityTypes() {
    EntityRegistry registry = new TestEntityRegistry();
    Assert.assertEquals(
        ImmutableSet.of(
            "container",
            "dataProduct",
            "mlModelGroup",
            "corpuser",
            "dataFlow",
            "glossaryNode",
            "dataProcess",
            "schemaField",
            "mlFeatureTable",
            "corpGroup",
            "mlModel",
            "mlFeature",
            "dataProcessInstance",
            "glossaryTerm",
            "mlPrimaryKey",
            "dataJob",
            "domain",
            "tag",
            "mlModelDeployment",
            "dataset",
            "chart",
            "dashboard",
            "notebook"),
        TestUtils.getSupportedEntityTypes(registry));
  }
}
