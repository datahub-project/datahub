package com.linkedin.metadata.structuredproperties.validators;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.structuredproperties.validation.PropertyDefinitionValidator;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertyMappingLookup;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PropertyDefinitionValidatorTest {

  private EntityRegistry entityRegistry;
  private Urn testPropertyUrn;
  private RetrieverContext mockRetrieverContext;
  private OperationContext operationContext;
  private AspectRetriever mockAspectRetriever;
  private StructuredPropertyMappingLookup mockStructuredPropertyMappingLookup;

  @BeforeMethod
  public void init() throws IOException {
    entityRegistry = new TestEntityRegistry();
    operationContext = mock(OperationContext.class);
    testPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    HashMap<Urn, Boolean> map = new HashMap<>();
    when(mockAspectRetriever.entityExists(any(), any())).thenReturn(map);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Collections.emptyMap());
    mockStructuredPropertyMappingLookup = mock(StructuredPropertyMappingLookup.class);
    when(mockStructuredPropertyMappingLookup.fieldExists(any(), any())).thenReturn(false);
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
                operationContext,
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
                operationContext,
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
    newProperty.setVersion(null, SetMode.REMOVE_IF_NULL);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCanChangeMultipleToSingleWithNewVersion()
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
    newProperty.setVersion("00000000000001");
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
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
    newProperty.setVersion(null, SetMode.REMOVE_IF_NULL);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCanChangeValueTypeWithNewVersion()
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
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    newProperty.setVersion("00000000000001");
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
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
                operationContext,
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
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCannotChangeFullyQualifiedNameWithVersionChange()
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
    newProperty.setVersion("00000000000001");
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
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
    newProperty.setVersion(null, SetMode.REMOVE_IF_NULL);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
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
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testCanChangeRestrictAllowedValuesWithVersionChange()
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
    newProperty.setVersion("00000000000001");
    PropertyValue allowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(1.0)).setDescription("hello");
    newProperty.setAllowedValues(new PropertyValueArray(allowedValue));

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);

    // Remove allowed values from constraint case
    PropertyValue oldAllowedValue =
        new PropertyValue().setValue(PrimitivePropertyValue.create(3.0)).setDescription("hello");
    oldProperty.setAllowedValues((new PropertyValueArray(allowedValue, oldAllowedValue)));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
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
                operationContext,
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
                operationContext,
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
                operationContext,
                TestMCP.ofOneMCP(testPropertyUrn, oldProperty, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testUrnIdWithSpace()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test me out.foo.bar");
    StructuredPropertyDefinition newProperty = new StructuredPropertyDefinition();
    newProperty.setEntityTypes(new UrnArray(Urn.createFromString("urn:li:logicalEntity:dataset")));
    newProperty.setDisplayName("oldProp");
    newProperty.setQualifiedName("foo.bar");
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testQualifiedNameWithSpace()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = new StructuredPropertyDefinition();
    newProperty.setEntityTypes(new UrnArray(Urn.createFromString("urn:li:logicalEntity:dataset")));
    newProperty.setDisplayName("oldProp");
    newProperty.setQualifiedName("foo.bar with spaces");
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    newProperty.setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testValidAllowedTypes()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = createValidPropertyDefinition();
    StringArrayMap typeQualifier = new StringArrayMap();
    typeQualifier.put("allowedTypes", new StringArray("urn:li:entityType:datahub.dataset"));
    newProperty.setTypeQualifier(typeQualifier);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testInvalidUrnsInAllowedTypes()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = createValidPropertyDefinition();
    StringArrayMap typeQualifier = new StringArrayMap();
    // invalid urn here
    typeQualifier.put("allowedTypes", new StringArray("invalidUrn"));
    newProperty.setTypeQualifier(typeQualifier);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testNotEntityTypeInAllowedTypes()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = createValidPropertyDefinition();
    StringArrayMap typeQualifier = new StringArrayMap();
    // urn that is not an entityType
    typeQualifier.put("allowedTypes", new StringArray("urn:li:dataPlatform:snowflake"));
    newProperty.setTypeQualifier(typeQualifier);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testEntityTypeDoesNotExistInAllowedTypes()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    HashMap<Urn, Boolean> map = new HashMap<>();
    map.put(UrnUtils.getUrn("urn:li:entityType:datahub.fakeEntity"), false);
    when(aspectRetriever.entityExists(any(), any())).thenReturn(map);
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Collections.emptyMap());
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(retrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);

    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = createValidPropertyDefinition();
    StringArrayMap typeQualifier = new StringArrayMap();
    // urn that doesn't exist
    typeQualifier.put("allowedTypes", new StringArray("urn:li:entityType:datahub.fakeEntity"));
    newProperty.setTypeQualifier(typeQualifier);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                retrieverContext)
            .count(),
        1);
  }

  @Test
  public void testAllowedTypesMixOfValidAndInvalid()
      throws URISyntaxException, CloneNotSupportedException, AspectValidationException {
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:foo.bar");
    StructuredPropertyDefinition newProperty = createValidPropertyDefinition();
    StringArrayMap typeQualifier = new StringArrayMap();
    // urn that is not an entityType
    typeQualifier.put(
        "allowedTypes",
        new StringArray("urn:li:entityType:datahub.dataset", "urn:li:dataPlatform:snowflake"));
    newProperty.setTypeQualifier(typeQualifier);
    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpserts(
                operationContext,
                TestMCP.ofOneMCP(propertyUrn, null, newProperty, entityRegistry),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testRejectsElasticsearchFieldNameCollisionWithExistingProperty()
      throws URISyntaxException, IOException {
    // A DIFFERENT, live property (certification.status) already maps to the same ES field, so
    // creating certification_status is a genuine collision.
    Urn newUrn = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition newDefinition =
        createCollisionTestDefinition("certification_status");

    Urn otherUrn = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition otherDefinition =
        createCollisionTestDefinition("certification.status");
    when(mockStructuredPropertyMappingLookup.fieldExists(any(), eq("certification_status")))
        .thenReturn(true);
    when(mockAspectRetriever.getLatestAspectObjects(any(), eq(Set.of(otherUrn)), any()))
        .thenReturn(
            Map.of(
                otherUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(otherDefinition.data()))));

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext,
                TestMCP.ofOneMCP(newUrn, newDefinition, entityRegistry),
                mockRetrieverContext,
                mockStructuredPropertyMappingLookup)
            .count(),
        1);
  }

  @Test
  public void testAllowsRecreateWhenCollidingFieldIsOrphaned()
      throws URISyntaxException, IOException {
    // The ES field exists (additive mappings retain it after delete), but no live property owns
    // it — this is a recreate of a previously-deleted property, which must be allowed.
    Urn newUrn = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition newDefinition =
        createCollisionTestDefinition("certification_status");

    when(mockStructuredPropertyMappingLookup.fieldExists(any(), eq("certification_status")))
        .thenReturn(true);
    // No live owner for any '.'/'_' variant (default getLatestAspectObjects -> emptyMap).

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext,
                TestMCP.ofOneMCP(newUrn, newDefinition, entityRegistry),
                mockRetrieverContext,
                mockStructuredPropertyMappingLookup)
            .count(),
        0);
  }

  @Test
  public void testAllowsCreateWhenNormalizedNamesDiffer() throws URISyntaxException {
    Urn newUrn = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition newDefinition =
        createCollisionTestDefinition("certification_status");

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext,
                TestMCP.ofOneMCP(newUrn, newDefinition, entityRegistry),
                mockRetrieverContext,
                mockStructuredPropertyMappingLookup)
            .count(),
        0);
  }

  @Test
  public void testAllowsUpsertOfExistingDefinitionWithoutMappingLookup()
      throws URISyntaxException, IOException {
    // When a definition already exists for the URN the write is an update; the qualified name is
    // immutable, so the mapping-backed collision check must be skipped entirely.
    Urn urn = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition existing = createCollisionTestDefinition("certification_status");
    StructuredPropertyDefinition updated = createCollisionTestDefinition("certification_status");
    updated.setDisplayName("updated display");

    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(
                urn,
                Map.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new Aspect(existing.data()))));

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext,
                TestMCP.ofOneMCP(urn, updated, entityRegistry),
                mockRetrieverContext,
                mockStructuredPropertyMappingLookup)
            .count(),
        0);
    Mockito.verify(mockStructuredPropertyMappingLookup, Mockito.never()).fieldExists(any(), any());
  }

  @Test
  public void testRejectsInBatchElasticsearchFieldCollision() throws URISyntaxException {
    Urn urnA = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnB = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defA = createCollisionTestDefinition("certification.status");
    StructuredPropertyDefinition defB = createCollisionTestDefinition("certification_status");

    Set<ChangeMCP> batch = new java.util.HashSet<>();
    batch.addAll(TestMCP.ofOneMCP(urnA, defA, entityRegistry));
    batch.addAll(TestMCP.ofOneMCP(urnB, defB, entityRegistry));

    long exceptionCount =
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext, batch, mockRetrieverContext, mockStructuredPropertyMappingLookup)
            .count();
    assertTrue(exceptionCount >= 1);
  }

  @Test
  public void testFailsClosedWhenMappingLookupFails() throws URISyntaxException, IOException {
    Urn newUrn = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition newDefinition =
        createCollisionTestDefinition("certification_status");

    when(mockStructuredPropertyMappingLookup.fieldExists(any(), eq("certification_status")))
        .thenThrow(new IOException("mapping backend unavailable"));

    assertEquals(
        PropertyDefinitionValidator.validateDefinitionUpsertsProposed(
                operationContext,
                TestMCP.ofOneMCP(newUrn, newDefinition, entityRegistry),
                mockRetrieverContext,
                mockStructuredPropertyMappingLookup)
            .count(),
        1);
  }

  private StructuredPropertyDefinition createCollisionTestDefinition(String qualifiedName)
      throws URISyntaxException {
    return new StructuredPropertyDefinition()
        .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
        .setDisplayName(qualifiedName)
        .setQualifiedName(qualifiedName)
        .setCardinality(PropertyCardinality.MULTIPLE)
        .setValueType(Urn.createFromString("urn:li:dataType:datahub.string"));
  }

  private StructuredPropertyDefinition createValidPropertyDefinition() throws URISyntaxException {
    StructuredPropertyDefinition newProperty = new StructuredPropertyDefinition();
    newProperty.setEntityTypes(
        new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")));
    newProperty.setDisplayName("oldProp");
    newProperty.setQualifiedName("foo.bar");
    newProperty.setCardinality(PropertyCardinality.MULTIPLE);
    newProperty.setValueType(Urn.createFromString("urn:li:dataType:datahub.urn"));
    return newProperty;
  }
}
