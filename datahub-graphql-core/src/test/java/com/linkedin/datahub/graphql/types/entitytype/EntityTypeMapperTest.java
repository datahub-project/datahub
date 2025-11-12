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

  @Test
  public void testGetTypeForUnknownEntity() throws Exception {
    assertEquals(EntityTypeMapper.getType("unknown_entity_type"), EntityType.OTHER);
  }

  @Test
  public void testGetTypeCaseInsensitive() throws Exception {
    assertEquals(EntityTypeMapper.getType("DATASET"), EntityType.DATASET);
    assertEquals(EntityTypeMapper.getType("dataset"), EntityType.DATASET);
    assertEquals(EntityTypeMapper.getType("DaTaSeT"), EntityType.DATASET);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetNameForUnknownEntityType() throws Exception {
    EntityTypeMapper.getName(EntityType.OTHER);
  }
}
