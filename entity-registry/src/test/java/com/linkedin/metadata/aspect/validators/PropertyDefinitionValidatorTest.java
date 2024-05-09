package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.PropertyDefinitionValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyKey;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PropertyDefinitionValidatorTest {

  private EntityRegistry entityRegistry;
  private Urn testPropertyUrn;
  private RetrieverContext mockRetrieverContext;

  @BeforeTest
  public void init() {
    entityRegistry = new TestEntityRegistry();
    testPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testValidatePreCommitNoPrevious()
      throws URISyntaxException, AspectValidationException {
    StructuredPropertyDefinition newProperty = new StructuredPropertyDefinition();
    newProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    newProperty.setDisplayName("newProp");
    newProperty.setQualifiedName("prop3");
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testCanChangeSingleToMultiple()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.SINGLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testCannotChangeMultipleToSingle()
      throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setCardinality(PropertyCardinality.SINGLE);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCannotChangeValueType() throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCanChangeDisplayName()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setDisplayName("newProp");
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testCannotChangeFullyQualifiedName()
      throws URISyntaxException, CloneNotSupportedException {
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    newProperty.setQualifiedName("newProp");
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCannotChangeRestrictAllowedValues()
      throws URISyntaxException, CloneNotSupportedException {
    // No constraint -> constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    newProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);

    // Remove allowed values from constraint case
    PropertyValue oldAllowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(3.0)).setDescription("hello");
    oldProperty.setAllowedValues((new PropertyValueArray(allowedValue, oldAllowedValue)));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCanExpandAllowedValues()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    // Constraint -> no constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    oldProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);

    // Add allowed values to constraint case
    PropertyValue newAllowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(3.0)).setDescription("hello");
    newProperty.setAllowedValues((new PropertyValueArray(allowedValue, newAllowedValue)));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testCanChangeAllowedValueDescriptions()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    // Constraint -> no constraint case
    StructuredPropertyDefinition oldProperty = new StructuredPropertyDefinition();
    oldProperty.setEntityTypes(
        new UrnArray(
            Urn.createFromString("urn:li:logicalEntity:dataset"),
            Urn.createFromString("urn:li:logicalEntity:chart"),
            Urn.createFromString("urn:li:logicalEntity:glossaryTerm")));
    oldProperty.setDisplayName("oldProp");
    oldProperty.setQualifiedName("prop3");
    oldProperty.setCardinality(PropertyCardinality.MULTIPLE);
    oldProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition newProperty = oldProperty.copy();
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    oldProperty.setAllowedValues(new PropertyValueArray(allowedValue));
    PropertyValue newAllowedValue =
        new PropertyValue()
            .setValue(PrimitivePropertyValue.create(1.0))
            .setDescription("hello there");
    newProperty.setAllowedValues(new PropertyValueArray(newAllowedValue));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testHardDeleteBlock() {
    PropertyDefinitionValidator test =
        new PropertyDefinitionValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .enabled(true)
                    .className(PropertyDefinitionValidator.class.getName())
                    .supportedOperations(List.of("DELETE"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                                .aspectName(Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                                .build(),
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                                .aspectName("structuredPropertyKey")
                                .build()))
                    .build());

    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(UrnUtils.getUrn("urn:li:structuredProperty:foo.bar"))
                        .entitySpec(entityRegistry.getEntitySpec("structuredProperty"))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME)
                                .getKeyAspectSpec())
                        .recordTemplate(new StructuredPropertyKey())
                        .build()),
                mockRetrieverContext)
            .count(),
        1);

    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(UrnUtils.getUrn("urn:li:structuredProperty:foo.bar"))
                        .entitySpec(entityRegistry.getEntitySpec("structuredProperty"))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME)
                                .getAspectSpecMap()
                                .get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))
                        .recordTemplate(new StructuredPropertyDefinition())
                        .build()),
                mockRetrieverContext)
            .count(),
        1);
  }
}
