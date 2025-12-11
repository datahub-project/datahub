/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
