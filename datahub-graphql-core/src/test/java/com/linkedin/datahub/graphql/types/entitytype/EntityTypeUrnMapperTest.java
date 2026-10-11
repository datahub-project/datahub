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

  @Test
  public void testMetricRoundTrip() throws Exception {
    final String metricUrn = "urn:li:entityType:datahub.metric";
    assertEquals(EntityTypeUrnMapper.getName(metricUrn), Constants.METRIC_ENTITY_NAME);
    assertEquals(EntityTypeUrnMapper.getEntityType(metricUrn), EntityType.METRIC);
    assertEquals(EntityTypeUrnMapper.getEntityTypeUrn(Constants.METRIC_ENTITY_NAME), metricUrn);
  }

  @Test
  public void testSemanticModelRoundTrip() throws Exception {
    final String semanticModelUrn = "urn:li:entityType:datahub.semanticModel";
    assertEquals(
        EntityTypeUrnMapper.getName(semanticModelUrn), Constants.SEMANTIC_MODEL_ENTITY_NAME);
    assertEquals(EntityTypeUrnMapper.getEntityType(semanticModelUrn), EntityType.SEMANTIC_MODEL);
    assertEquals(
        EntityTypeUrnMapper.getEntityTypeUrn(Constants.SEMANTIC_MODEL_ENTITY_NAME),
        semanticModelUrn);
  }
}
