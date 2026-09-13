package com.linkedin.datahub.graphql.types.structuredproperty;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.StringValue;
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
  private static final Urn DATASET_VALUE_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,marketing_etl,PROD)");

  // The value reported in #19019: free text that contains an urn shaped substring. The urn parser
  // stops at the closing paren of the tuple key, so this used to be mapped as a dataset value
  // entity whose fabric was "PROD) (hop 1): stale for 30.0h", and resolving that entity threw
  // IllegalArgumentException ("No enum constant com.linkedin.common.FabricType...") and killed the
  // entity page.
  private static final String TEXT_CONTAINING_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,marketing_etl,PROD) (hop 1): stale for 30.0h";
  private static final String TEXT_ENDING_WITH_PARENS =
      "urn:li:dataset:(urn:li:dataPlatform:hive,marketing_etl,PROD) (hop 1)";

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

  // A tuple urn is still an entity reference: the fix must only reject values that are more than
  // an urn, not every value whose key is parenthesized.
  @Test
  public void testFullTupleUrnValueProducesValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(
            null, propertiesWithValue(DATASET_VALUE_URN.toString()), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 1);
    assertEquals(
        mapped.getProperties().get(0).getValueEntities().get(0).getType(), EntityType.DATASET);
    assertEquals(
        mapped.getProperties().get(0).getValueEntities().get(0).getUrn(),
        DATASET_VALUE_URN.toString());
  }

  @Test
  public void testUrnValueWithSurroundingWhitespaceProducesValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(
            null, propertiesWithValue("  urn:li:domain:marketing  "), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 1);
    assertEquals(
        mapped.getProperties().get(0).getValueEntities().get(0).getType(), EntityType.DOMAIN);
  }

  // A value that only contains an urn is text, and mapping it must never throw (#19019).
  @Test
  public void testTextContainingUrnProducesNoValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(null, propertiesWithValue(TEXT_CONTAINING_URN), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 0);
    assertEquals(mapped.getProperties().get(0).getValues().size(), 1);
    assertEquals(
        ((StringValue) mapped.getProperties().get(0).getValues().get(0)).getStringValue(),
        TEXT_CONTAINING_URN);
  }

  // Same, for text whose trailing part happens to end with a paren, which leaves the parentheses
  // of the value balanced.
  @Test
  public void testTextEndingWithParensProducesNoValueEntity() {
    com.linkedin.datahub.graphql.generated.StructuredProperties mapped =
        StructuredPropertiesMapper.map(
            null, propertiesWithValue(TEXT_ENDING_WITH_PARENS), ENTITY_URN);

    assertEquals(mapped.getProperties().get(0).getValueEntities().size(), 0);
    assertEquals(mapped.getProperties().get(0).getValues().size(), 1);
    assertEquals(
        ((StringValue) mapped.getProperties().get(0).getValues().get(0)).getStringValue(),
        TEXT_ENDING_WITH_PARENS);
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
