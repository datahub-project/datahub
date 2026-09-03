package com.linkedin.datahub.graphql.types.structuredproperty;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import org.testng.annotations.Test;

public class StructuredPropertiesMapperTest {

  private static final Urn PROPERTY_URN = UrnUtils.getUrn("urn:li:structuredProperty:test");
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");

  private static StructuredProperties propertiesWithValue(String value) {
    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setPropertyUrn(PROPERTY_URN);
    assignment.setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(value)));
    StructuredProperties properties = new StructuredProperties();
    properties.setProperties(new StructuredPropertyValueAssignmentArray(assignment));
    return properties;
  }

  @Test
  public void testMappedUrnValueProducesValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(
            null, propertiesWithValue("urn:li:domain:marketing"), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 1);
    assertEquals(
        mapped.getProperties().get(0).getValueEntities().get(0).getType(), EntityType.DOMAIN);
  }

  // A value that parses as a URN of an entity type UrnToEntityMapper cannot map (here a valid but
  // unmapped entity type, e.g. free text on a non-urn property) must not add a null value entity —
  // otherwise valueEntities resolution NPEs on the null element.
  @Test
  public void testUnmappedUrnValueDoesNotProduceValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(
            null, propertiesWithValue("urn:li:dataHubUpgrade:foo"), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 0);
    assertEquals(mapped.getProperties().get(0).getValues().size(), 1);
  }

  @Test
  public void testNonUrnStringValueProducesNoValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(null, propertiesWithValue("just some text"), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 0);
    assertEquals(mapped.getProperties().get(0).getValues().size(), 1);
  }
}
