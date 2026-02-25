package com.linkedin.datahub.graphql.types.structuredproperty;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.PropertyCardinality;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import org.testng.annotations.Test;

public class StructuredPropertyMapperTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:structuredProperty:test");
  private static final Urn TEST_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.string");
  private static final Urn TEST_ENTITY_TYPE_URN =
      UrnUtils.getUrn("urn:li:entityType:datahub.dataset");

  @Test
  public void testMapFull() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(TEST_URN);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createFullDefinition().data())));
    aspectMap.put(
        STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createFullSettings().data())));
    entityResponse.setAspects(aspectMap);

    StructuredPropertyEntity mapped = StructuredPropertyMapper.map(null, entityResponse);

    assertEquals(mapped.getUrn(), TEST_URN.toString());
    assertEquals(mapped.getType(), EntityType.STRUCTURED_PROPERTY);
    assertEquals(mapped.getDefinition().getQualifiedName(), "test");
    assertEquals(mapped.getDefinition().getCardinality(), PropertyCardinality.SINGLE);
    assertEquals(mapped.getDefinition().getImmutable(), (Boolean) true);
    assertEquals(mapped.getDefinition().getValueType().getUrn(), TEST_DATA_TYPE_URN.toString());
    assertEquals(mapped.getDefinition().getDisplayName(), "Test");
    assertEquals(mapped.getDefinition().getDescription(), "Test description");
    assertEquals(mapped.getDefinition().getAllowedValues().size(), 1);
    assertEquals(
        ((com.linkedin.datahub.graphql.generated.StringValue)
                mapped.getDefinition().getAllowedValues().get(0).getValue())
            .getStringValue(),
        "test");
    assertEquals(mapped.getDefinition().getTypeQualifier().getAllowedTypes().size(), 1);
    assertEquals(
        mapped.getDefinition().getTypeQualifier().getAllowedTypes().get(0).getUrn(),
        TEST_ENTITY_TYPE_URN.toString());
    assertEquals(mapped.getSettings().getIsHidden(), (Boolean) true);
  }

  @Test
  public void testMapDefault() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(TEST_URN);
    entityResponse.setAspects(new EnvelopedAspectMap());

    StructuredPropertyEntity mapped = StructuredPropertyMapper.map(null, entityResponse);

    assertEquals(mapped.getUrn(), TEST_URN.toString());
    assertEquals(mapped.getType(), EntityType.STRUCTURED_PROPERTY);
    assertEquals(mapped.getDefinition().getQualifiedName(), "");
    assertEquals(mapped.getDefinition().getCardinality(), PropertyCardinality.SINGLE);
    assertEquals(mapped.getDefinition().getImmutable(), (Boolean) true);
    assertEquals(mapped.getDefinition().getValueType().getUrn(), "urn:li:dataType:datahub.string");
    assertEquals(mapped.getDefinition().getEntityTypes().size(), 1);
    assertEquals(
        mapped.getDefinition().getEntityTypes().get(0).getUrn(),
        "urn:li:entityType:datahub.dataset");
  }

  private com.linkedin.structured.StructuredPropertyDefinition createFullDefinition()
      throws Exception {
    com.linkedin.structured.StructuredPropertyDefinition definition =
        new com.linkedin.structured.StructuredPropertyDefinition();
    definition.setQualifiedName("test");
    definition.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    definition.setImmutable(true);
    definition.setValueType(TEST_DATA_TYPE_URN);
    definition.setDisplayName("Test");
    definition.setDescription("Test description");
    PropertyValueArray allowedValues = new PropertyValueArray();
    PropertyValue propertyValue = new PropertyValue();
    PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();
    primitivePropertyValue.setString("test");
    propertyValue.setValue(primitivePropertyValue);
    allowedValues.add(propertyValue);
    definition.setAllowedValues(allowedValues);
    StringArrayMap typeQualifier = new StringArrayMap();
    StringArray allowedTypes = new StringArray();
    allowedTypes.add(TEST_ENTITY_TYPE_URN.toString());
    typeQualifier.put("allowedTypes", allowedTypes);
    definition.setTypeQualifier(typeQualifier);
    definition.setCreated(
        new AuditStamp().setActor(Urn.createFromString("urn:li:corpuser:test")).setTime(0L));
    definition.setLastModified(
        new AuditStamp().setActor(Urn.createFromString("urn:li:corpuser:test")).setTime(0L));
    definition.setEntityTypes(new com.linkedin.common.UrnArray());
    return definition;
  }

  private com.linkedin.structured.StructuredPropertySettings createFullSettings() {
    com.linkedin.structured.StructuredPropertySettings settings =
        new com.linkedin.structured.StructuredPropertySettings();
    settings.setIsHidden(true);
    settings.setShowInSearchFilters(false);
    settings.setShowInAssetSummary(false);
    settings.setHideInAssetSummaryWhenEmpty(false);
    settings.setShowAsAssetBadge(false);
    settings.setShowInColumnsTable(false);
    return settings;
  }
}
