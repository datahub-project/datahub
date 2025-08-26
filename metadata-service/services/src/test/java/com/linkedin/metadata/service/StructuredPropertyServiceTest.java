package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StructuredPropertyServiceTest {

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testGetEntityStructuredProperties() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();
    final StructuredPropertyValueAssignment structuredProperty1 =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:existingProperty1"))
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create("stringValue1"),
                        PrimitivePropertyValue.create("stringValue2"))));
    final StructuredPropertyValueAssignment structuredProperty2 =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:existingProperty2"))
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create(101.1),
                        PrimitivePropertyValue.create(101.2))));

    final StructuredProperties existingProperties = new StructuredProperties();
    existingProperties.setProperties(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(structuredProperty1, structuredProperty2)));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                eq(TEST_ENTITY_URN_1.getEntityType()),
                eq(TEST_ENTITY_URN_1),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            STRUCTURED_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingProperties.data()))))));

    final List<StructuredPropertyValueAssignment> properties =
        service.getEntityStructuredProperties(opContext, TEST_ENTITY_URN_1);

    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.get(0), structuredProperty1);
    Assert.assertEquals(properties.get(1), structuredProperty2);
  }

  @Test
  public void testGetEntityStructuredPropertiesNone() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                eq(TEST_ENTITY_URN_1.getEntityType()),
                eq(TEST_ENTITY_URN_1),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<StructuredPropertyValueAssignment> properties =
        service.getEntityStructuredProperties(opContext, TEST_ENTITY_URN_1);

    Assert.assertEquals(properties.size(), 0);
  }

  @Test
  public void testGetSchemaFieldStructuredProperties() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    final String testFieldPath = "testField";
    final Urn testFieldUrn =
        UrnUtils.getUrn(
            String.format("urn:li:schemaField:(%s,%s)", TEST_ENTITY_URN_1, testFieldPath));

    final StructuredPropertyValueAssignment structuredProperty1 =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:existingProperty1"))
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create("stringValue1"),
                        PrimitivePropertyValue.create("stringValue2"))));
    final StructuredPropertyValueAssignment structuredProperty2 =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:existingProperty2"))
            .setValues(
                new PrimitivePropertyValueArray(
                    ImmutableList.of(
                        PrimitivePropertyValue.create(101.1),
                        PrimitivePropertyValue.create(101.2))));

    final StructuredProperties existingProperties = new StructuredProperties();
    existingProperties.setProperties(
        new StructuredPropertyValueAssignmentArray(
            ImmutableList.of(structuredProperty1, structuredProperty2)));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                eq(testFieldUrn.getEntityType()),
                eq(testFieldUrn),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            STRUCTURED_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingProperties.data()))))));

    final List<StructuredPropertyValueAssignment> properties =
        service.getSchemaFieldStructuredProperties(opContext, TEST_ENTITY_URN_1, testFieldPath);

    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.get(0), structuredProperty1);
    Assert.assertEquals(properties.get(1), structuredProperty2);
  }

  @Test
  public void testGetSchemaFieldTagsNoTags() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    final String testFieldPath = "testField";
    final Urn testFieldUrn =
        UrnUtils.getUrn(
            String.format("urn:li:schemaField:(%s,%s)", TEST_ENTITY_URN_1, testFieldPath));

    Mockito.when(
            service.entityClient.getV2(
                Mockito.any(OperationContext.class),
                eq(testFieldUrn.getEntityType()),
                eq(testFieldUrn),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final List<StructuredPropertyValueAssignment> properties =
        service.getSchemaFieldStructuredProperties(opContext, TEST_ENTITY_URN_1, testFieldPath);

    Assert.assertEquals(properties.size(), 0);
  }

  @Test
  public void testAreProposedStructuredPropertyValuesValidThrowsWhenDefinitionMissing()
      throws RemoteInvocationException, URISyntaxException {
    // GIVEN
    final StructuredPropertyService service = createMockStructuredPropertyService();

    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test");
    List<PrimitivePropertyValue> values = Collections.emptyList();

    // Mock getStructuredPropertyDefinition to return null
    Mockito.when(
            service.entityClient.getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(null);

    // WHEN / THEN
    try {
      service.areProposedStructuredPropertyValuesValid(opContext, structuredPropertyUrn, values);
      Assert.fail("Expected EntityDoesNotExistException to be thrown when definition is missing.");
    } catch (EntityDoesNotExistException ex) {
      // success path
    } catch (Exception e) {
      Assert.fail("Expected EntityDoesNotExistException but caught a different exception.", e);
    }
  }

  @Test
  public void testAreProposedStructuredPropertyValuesValidReturnsTrueWhenValuesValid()
      throws RemoteInvocationException, URISyntaxException {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    // GIVEN
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test");

    // Create a mock definition with SINGLE cardinality and no allowed values
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setCardinality(PropertyCardinality.MULTIPLE);
    // Set a "valueType" that the code recognizes as stored-as-string, e.g. STRING
    Urn stringValueTypeUrn = UrnUtils.getUrn("urn:li:dataType:string");
    definition.setValueType(stringValueTypeUrn);
    definition.setAllowedValues(
        new PropertyValueArray(
            ImmutableList.of(
                new PropertyValue().setValue(PrimitivePropertyValue.create("validValue1")),
                new PropertyValue().setValue(PrimitivePropertyValue.create("validValue2")))));

    PrimitivePropertyValue singleValidStringValue = PrimitivePropertyValue.create("validValue1");
    singleValidStringValue.setString("validValue1");

    List<PrimitivePropertyValue> multipleValidStringValue =
        ImmutableList.of(
            PrimitivePropertyValue.create("validValue1"),
            PrimitivePropertyValue.create("validValue2"));

    // Mock the response so that we have a non-null definition
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(definition.data())))));

    Mockito.when(
            service.entityClient.getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(mockResponse);

    // Case 1: One string value provided.
    boolean result =
        service.areProposedStructuredPropertyValuesValid(
            opContext, structuredPropertyUrn, Collections.singletonList(singleValidStringValue));
    Assert.assertTrue(result, "Expected true when values are valid according to the definition.");

    // Case 2: Multiple string values provided.
    result =
        service.areProposedStructuredPropertyValuesValid(
            opContext, structuredPropertyUrn, multipleValidStringValue);
    Assert.assertTrue(result, "Expected true when values are valid according to the definition.");
  }

  @Test
  public void testAreProposedStructuredPropertyValuesValidReturnsFalseWhenValueTypeInvalid()
      throws RemoteInvocationException, URISyntaxException {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    // GIVEN
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test");

    // Create a mock definition with SINGLE cardinality and no allowed values
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setCardinality(PropertyCardinality.SINGLE);

    // Set a "valueType" that the code recognizes as stored-as-string, e.g. STRING
    Urn stringValueTypeUrn = UrnUtils.getUrn("urn:li:dataType:string");
    definition.setValueType(stringValueTypeUrn);

    // Prepare a valid single PrimitivePropertyValue
    PrimitivePropertyValue singleInvalidDoubleValue = new PrimitivePropertyValue();
    singleInvalidDoubleValue.setDouble(10.0);

    // Mock the response so that we have a non-null definition
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(definition.data())))));

    Mockito.when(
            service.entityClient.getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(mockResponse);

    // WHEN
    boolean result =
        service.areProposedStructuredPropertyValuesValid(
            opContext, structuredPropertyUrn, Collections.singletonList(singleInvalidDoubleValue));

    // THEN
    Assert.assertFalse(result, "Expected false when values are of invalid type.");
  }

  @Test
  public void testAreProposedStructuredPropertyValuesValidReturnsFalseWhenValueCardinalityInvalid()
      throws RemoteInvocationException, URISyntaxException {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    // GIVEN
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test");

    // Create a mock definition with SINGLE cardinality and no allowed values
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setCardinality(PropertyCardinality.SINGLE);

    // Set a "valueType" that the code recognizes as stored-as-string, e.g. STRING
    Urn stringValueTypeUrn = UrnUtils.getUrn("urn:li:dataType:string");
    definition.setValueType(stringValueTypeUrn);

    // Prepare a valid single PrimitivePropertyValue
    List<PrimitivePropertyValue> multipleInvalidStringValues =
        ImmutableList.of(
            PrimitivePropertyValue.create("invalidValue1"),
            PrimitivePropertyValue.create("invalidValue2"));

    // Mock the response so that we have a non-null definition
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(definition.data())))));

    Mockito.when(
            service.entityClient.getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(mockResponse);

    // WHEN
    boolean result =
        service.areProposedStructuredPropertyValuesValid(
            opContext, structuredPropertyUrn, multipleInvalidStringValues);

    // THEN
    Assert.assertFalse(
        result, "Expected false when multiple values are provided for single cardinality.");
  }

  @Test
  public void
      testAreProposedStructuredPropertyValuesValidReturnsFalseWhenValueAllowedValuesInvalid()
          throws RemoteInvocationException, URISyntaxException {
    final StructuredPropertyService service = createMockStructuredPropertyService();

    // GIVEN
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test");

    // Create a mock definition with SINGLE cardinality and no allowed values
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setCardinality(PropertyCardinality.SINGLE);
    definition.setAllowedValues(
        new PropertyValueArray(
            ImmutableList.of(
                new PropertyValue().setValue(PrimitivePropertyValue.create("validValue1")),
                new PropertyValue().setValue(PrimitivePropertyValue.create("validValue2")))));

    // Set a "valueType" that the code recognizes as stored-as-string, e.g. STRING
    Urn stringValueTypeUrn = UrnUtils.getUrn("urn:li:dataType:string");
    definition.setValueType(stringValueTypeUrn);

    // Prepare a valid single PrimitivePropertyValue
    PrimitivePropertyValue singleInvalidStringValue = new PrimitivePropertyValue();
    singleInvalidStringValue.setString("invalidValue");

    // Mock the response so that we have a non-null definition
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(definition.data())))));

    Mockito.when(
            service.entityClient.getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(mockResponse);

    // WHEN
    boolean result =
        service.areProposedStructuredPropertyValuesValid(
            opContext, structuredPropertyUrn, Collections.singletonList(singleInvalidStringValue));

    // THEN
    Assert.assertFalse(
        result, "Expected false when invalid values are provided when there are allowed values.");
  }

  //
  // ============ 1) Test when there are no existing properties  ============
  //
  @Test
  public void testUpdateEntityStructuredPropertiesNoExistingProperties() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();
    final OperationContext mockOpContext = mock(OperationContext.class);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(0L);
    auditStamp.setActor(Urn.createFromString("urn:li:corpuser:test"));

    Mockito.when(mockOpContext.getAuditStamp()).thenReturn(auditStamp);

    // Given
    Urn testUrn = Urn.createFromString("urn:li:dataJob:(sample, noExistingProps, PRODUCTION)");

    // Mock getV2(...) so that getStructuredProperties(...) returns null
    Mockito.when(
            service.entityClient.getV2(
                eq(mockOpContext),
                eq(testUrn.getEntityType()),
                eq(testUrn),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null); // No aspect => no existing properties.

    // Prepare new property assignments
    List<StructuredPropertyValueAssignment> newAssignments =
        Collections.singletonList(
            createAssignment("urn:li:structuredProperty:myProperty", "myValue", auditStamp));

    // When
    StructuredProperties updated =
        service.updateEntityStructuredProperties(mockOpContext, testUrn, newAssignments);

    // Then
    Assert.assertNotNull(updated);
    Assert.assertNotNull(updated.getProperties());
    Assert.assertEquals(updated.getProperties().size(), 1);

    // Verify that ingestProposal(...) was called with correct data
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(service.entityClient, Mockito.times(1))
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(false));

    MetadataChangeProposal mcp = proposalCaptor.getValue();
    Assert.assertNotNull(mcp);
    Assert.assertEquals(mcp.getEntityUrn(), testUrn);
    Assert.assertEquals(mcp.getAspectName(), STRUCTURED_PROPERTIES_ASPECT_NAME);

    StructuredProperties expectedProperties =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(newAssignments));

    StructuredProperties actualProperties =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            StructuredProperties.class);

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getPropertyUrn(),
        expectedProperties.getProperties().get(0).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getValues(),
        expectedProperties.getProperties().get(0).getValues());

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getLastModified().getActor(),
        expectedProperties.getProperties().get(0).getLastModified().getActor());

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getLastModified().getTime(),
        expectedProperties.getProperties().get(0).getLastModified().getTime());
  }

  //
  // ============ 2) Test when some existing properties with no overlap ============
  //
  @Test
  public void testUpdateEntityStructuredPropertiesExistingPropertiesNoOverlap() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();
    final OperationContext mockOpContext = mock(OperationContext.class);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(0L);
    auditStamp.setActor(Urn.createFromString("urn:li:corpuser:test"));

    Mockito.when(mockOpContext.getAuditStamp()).thenReturn(auditStamp);

    // Given
    Urn testUrn = Urn.createFromString("urn:li:dataJob:(sample, existingNoOverlap, PRODUCTION)");

    // Existing properties in the aspect
    StructuredProperties existingStructuredProperties = new StructuredProperties();
    StructuredPropertyValueAssignment existingProp =
        createAssignment("urn:li:structuredProperty:existingProp", "existingValue", auditStamp);
    existingStructuredProperties.setProperties(
        new StructuredPropertyValueAssignmentArray(Collections.singletonList(existingProp)));

    // Mock getV2(...) to return an EntityResponse that has existing properties
    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Mockito.when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    STRUCTURED_PROPERTIES_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setValue(
                            new com.linkedin.entity.Aspect(existingStructuredProperties.data())))));

    Mockito.when(
            service.entityClient.getV2(
                eq(mockOpContext),
                eq(testUrn.getEntityType()),
                eq(testUrn),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockEntityResponse);

    // New properties that do not overlap with the existing one
    List<StructuredPropertyValueAssignment> newAssignments =
        Collections.singletonList(
            createAssignment("urn:li:structuredProperty:newProp", "newValue", auditStamp));

    // When
    StructuredProperties updated =
        service.updateEntityStructuredProperties(mockOpContext, testUrn, newAssignments);

    // Then
    Assert.assertNotNull(updated);
    // Expect 2 properties: existingProp + newProp
    Assert.assertEquals(updated.getProperties().size(), 2);

    // Verify that ingestProposal(...) was called with correct data
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(service.entityClient, Mockito.times(1))
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(false));

    // Optional: Inspect the final properties in the captured proposal
    MetadataChangeProposal mcp = proposalCaptor.getValue();
    StructuredProperties actualProperties =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            StructuredProperties.class);
    Assert.assertEquals(actualProperties.getProperties().size(), 2);

    List<StructuredPropertyValueAssignment> allAssignments =
        ImmutableList.<StructuredPropertyValueAssignment>builder()
            .addAll(existingStructuredProperties.getProperties())
            .addAll(newAssignments)
            .build();

    StructuredProperties expectedProperties =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(allAssignments));

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getPropertyUrn(),
        expectedProperties.getProperties().get(0).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getValues(),
        expectedProperties.getProperties().get(0).getValues());

    Assert.assertEquals(
        actualProperties.getProperties().get(1).getPropertyUrn(),
        expectedProperties.getProperties().get(1).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(1).getValues(),
        expectedProperties.getProperties().get(1).getValues());
  }

  //
  // ============ 3) Test when some existing properties with overlap ============
  //
  @Test
  public void testUpdateEntityStructuredPropertiesExistingPropertiesWithOverlap() throws Exception {
    final StructuredPropertyService service = createMockStructuredPropertyService();
    final OperationContext mockOpContext = mock(OperationContext.class);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(0L);
    auditStamp.setActor(Urn.createFromString("urn:li:corpuser:test"));

    Mockito.when(mockOpContext.getAuditStamp()).thenReturn(auditStamp);

    // Given
    Urn testUrn = Urn.createFromString("urn:li:dataJob:(sample, existingWithOverlap, PRODUCTION)");

    // Existing properties
    StructuredProperties existingStructuredProperties = new StructuredProperties();
    StructuredPropertyValueAssignment existingProp1 =
        createAssignment("urn:li:structuredProperty:prop1", "oldValue1", auditStamp);
    StructuredPropertyValueAssignment existingProp2 =
        createAssignment("urn:li:structuredProperty:prop2", "oldValue2", auditStamp);
    existingStructuredProperties.setProperties(
        new StructuredPropertyValueAssignmentArray(List.of(existingProp1, existingProp2)));

    // Mock getV2(...) to return an EntityResponse that has these existing properties
    EntityResponse mockEntityResponse = mock(EntityResponse.class);
    Mockito.when(mockEntityResponse.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    STRUCTURED_PROPERTIES_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setValue(
                            new com.linkedin.entity.Aspect(existingStructuredProperties.data())))));

    Mockito.when(
            service.entityClient.getV2(
                eq(mockOpContext),
                eq(testUrn.getEntityType()),
                eq(testUrn),
                eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockEntityResponse);

    // New assignments that overlap on prop1 and add prop3
    List<StructuredPropertyValueAssignment> newAssignments =
        List.of(
            createAssignment(
                "urn:li:structuredProperty:prop1",
                "newValue1",
                auditStamp), // Overlaps with existing prop1
            createAssignment(
                "urn:li:structuredProperty:prop3", "value3", auditStamp) // New property
            );

    // When
    StructuredProperties updated =
        service.updateEntityStructuredProperties(mockOpContext, testUrn, newAssignments);

    // Then
    // Expect final list: prop1 (updated value), prop2 (unchanged), prop3 (new)
    Assert.assertNotNull(updated);
    Assert.assertEquals(updated.getProperties().size(), 3);

    StructuredPropertyValueAssignment updatedProp1 =
        updated.getProperties().stream()
            .filter(p -> p.getPropertyUrn().toString().equals("urn:li:structuredProperty:prop1"))
            .findFirst()
            .orElseThrow();
    Assert.assertEquals(updatedProp1.getValues().size(), 1);
    Assert.assertEquals(updatedProp1.getValues().get(0).getString(), "newValue1");

    // Verify the ingestion
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(service.entityClient, Mockito.times(1))
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(false));

    // Inspect final aspect in the captured proposal
    MetadataChangeProposal mcp = proposalCaptor.getValue();
    StructuredProperties actualProperties =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            StructuredProperties.class);
    Assert.assertEquals(actualProperties.getProperties().size(), 3);

    List<StructuredPropertyValueAssignment> allAssignments =
        ImmutableList.<StructuredPropertyValueAssignment>builder()
            .addAll(existingStructuredProperties.getProperties())
            .add(newAssignments.get(1))
            .build();

    StructuredProperties expectedProperties =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(allAssignments));

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getPropertyUrn(),
        expectedProperties.getProperties().get(0).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(0).getValues(),
        expectedProperties.getProperties().get(0).getValues());

    Assert.assertEquals(
        actualProperties.getProperties().get(1).getPropertyUrn(),
        expectedProperties.getProperties().get(1).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(1).getValues(),
        expectedProperties.getProperties().get(1).getValues());

    Assert.assertEquals(
        actualProperties.getProperties().get(2).getPropertyUrn(),
        expectedProperties.getProperties().get(2).getPropertyUrn());

    Assert.assertEquals(
        actualProperties.getProperties().get(2).getValues(),
        expectedProperties.getProperties().get(2).getValues());
  }

  // -----------------------------------------------------------------------------------------------------------
  // Utility Methods
  // -----------------------------------------------------------------------------------------------------------

  /** Helper method to create a single-valued StructuredPropertyValueAssignment. */
  private StructuredPropertyValueAssignment createAssignment(
      String propUrnStr, String value, AuditStamp auditStamp) {
    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setLastModified(auditStamp);
    try {
      assignment.setPropertyUrn(Urn.createFromString(propUrnStr));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    PrimitivePropertyValue primitiveVal = new PrimitivePropertyValue();
    primitiveVal.setString(value);
    assignment.setValues(new PrimitivePropertyValueArray(Collections.singletonList(primitiveVal)));
    return assignment;
  }

  private static StructuredPropertyService createMockStructuredPropertyService() {
    return new StructuredPropertyService(
        mock(SystemEntityClient.class), mock(OpenApiClient.class), opContext.getObjectMapper());
  }
}
