/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.entitytype;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import org.testng.annotations.Test;

public class EntityTypeUrnMapperTest {

  @Test
  public void testGetName() throws Exception {
    assertEquals(
        EntityTypeUrnMapper.getName("urn:li:entityType:datahub.dataset"),
        Constants.DATASET_ENTITY_NAME);
  }

  @Test
  public void testGetEntityType() throws Exception {
    assertEquals(
        EntityTypeUrnMapper.getEntityType("urn:li:entityType:datahub.dataset"), EntityType.DATASET);
  }

  @Test
  public void testGetEntityTypeUrn() throws Exception {
    assertEquals(
        EntityTypeUrnMapper.getEntityTypeUrn(Constants.DATASET_ENTITY_NAME),
        "urn:li:entityType:datahub.dataset");
  }
}
