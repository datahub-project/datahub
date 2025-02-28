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
}
