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

public class EntityTypeMapperTest {

  @Test
  public void testGetType() throws Exception {
    assertEquals(EntityTypeMapper.getType(Constants.DATASET_ENTITY_NAME), EntityType.DATASET);
  }

  @Test
  public void testGetName() throws Exception {
    assertEquals(EntityTypeMapper.getName(EntityType.DATASET), Constants.DATASET_ENTITY_NAME);
  }

  @Test
  public void testGetTypeForDocument() throws Exception {
    assertEquals(EntityTypeMapper.getType(Constants.DOCUMENT_ENTITY_NAME), EntityType.DOCUMENT);
  }

  @Test
  public void testGetNameForDocument() throws Exception {
    assertEquals(EntityTypeMapper.getName(EntityType.DOCUMENT), Constants.DOCUMENT_ENTITY_NAME);
  }
}
