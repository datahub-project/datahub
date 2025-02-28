package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static org.testng.Assert.*;

import com.linkedin.metadata.models.StructuredPropertyUtils;
import java.util.UUID;
import org.testng.annotations.Test;

public class StructuredPropertyUtilsTest {

  @Test
  public void testGetIdMismatchedInput() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> StructuredPropertyUtils.getPropertyId("test1", "test2"));
  }

  @Test
  public void testGetIdConsistentInput() throws Exception {
    assertEquals(StructuredPropertyUtils.getPropertyId("test1", "test1"), "test1");
  }

  @Test
  public void testGetIdNullQualifiedName() throws Exception {
    assertEquals(StructuredPropertyUtils.getPropertyId("test1", null), "test1");
  }

  @Test
  public void testGetIdNullId() throws Exception {
    assertEquals(StructuredPropertyUtils.getPropertyId(null, "test1"), "test1");
  }

  @Test
  public void testGetIdNullForBoth() throws Exception {
    try {
      String id = StructuredPropertyUtils.getPropertyId(null, null);
      UUID.fromString(id);
    } catch (Exception e) {
      fail("ID produced is not a UUID");
    }
  }
}
