package com.linkedin.metadata.test.query.schemafield;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.MapType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaFieldEvaluatorTest {
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,TestDataset,PROD)");

  private static final Urn STRUCTURED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:property");
  private static final Urn STRUCTURED_PROPERTY_URN_2 =
      UrnUtils.getUrn("urn:li:structuredProperty:property2");
  private SchemaFieldEvaluator evaluator;
  private EntityService<?> entityService;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    entityService = mock(EntityService.class);
    evaluator = new SchemaFieldEvaluator(entityService);
    opContext = mock(OperationContext.class);
  }

  @Test
  public void testIsEligibleTrueForSchemaFieldsQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of("schemaFields"));
    assertTrue(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testIsEligibleTrueForSchemaFieldsLengthQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of("schemaFields", "length"));
    assertTrue(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testIsIneligibleBadEntityType() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of("schemaFields", "length"));
    assertFalse(evaluator.isEligible("container", query));
  }

  @Test
  public void testIsIneligibleBadQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn("bad.query");
    when(query.getQueryParts()).thenReturn(ImmutableList.of("bad", "query"));
    assertFalse(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testValidateQueryValid() {
    TestQuery query = mock(TestQuery.class);

    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of("schemaFields"));

    ValidationResult result = evaluator.validateQuery("dataset", query);
    assertTrue(result.isValid());

    query = mock(TestQuery.class);

    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of("schemaFields", "length"));

    result = evaluator.validateQuery("dataset", query);
    assertTrue(result.isValid());
  }

  @Test
  public void testEvaluateWithValidResponses() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Arrays.asList(TEST_DATASET_URN));
    Set<TestQuery> queries = new HashSet<>(Arrays.asList(mock(TestQuery.class)));
    when(queries.iterator().next().getQuery())
        .thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY);

    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setHash("hash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new OtherSchema()));
    schemaMetadata.setVersion(0L);
    schemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new MapType())))
                    .setFieldPath("path")
                    .setDescription("description"))));

    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    editableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setDescription("editableDescription")
                    .setFieldPath("path"))));

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(schemaMetadata.data())),
                        EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(new Aspect(editableSchemaMetadata.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(mockResponses);

    com.linkedin.metadata.test.query.schemafield.SchemaField expectedSchemaField =
        new com.linkedin.metadata.test.query.schemafield.SchemaField(
            "path", "description", "editableDescription");

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, "dataset", urns, queries);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().size(), 1);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().get(0),
        TestsSchemaFieldUtils.serializeSchemaField(expectedSchemaField));
  }

  @Test
  public void testEvaluateStructuredPropWithValidResponses() throws URISyntaxException {
    // Sets up one schema field with a structured property and verifies it is in the result
    Set<Urn> urns = new HashSet<>(Arrays.asList(TEST_DATASET_URN));
    TestQuery testQuery =
        new TestQuery(
            TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY
                + "."
                + STRUCTURED_PROPERTIES_ASPECT_NAME
                + "."
                + STRUCTURED_PROPERTY_URN);
    assertTrue(evaluator.isEligible(DATASET_ENTITY_NAME, testQuery));
    Set<TestQuery> queries = new HashSet<>(Arrays.asList(testQuery));
    Urn schemaFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "path");

    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setHash("hash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new OtherSchema()));
    schemaMetadata.setVersion(0L);
    schemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new MapType())))
                    .setFieldPath("path")
                    .setDescription("description"))));

    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    editableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setDescription("editableDescription")
                    .setFieldPath("path"))));

    PrimitivePropertyValueArray primitivePropertyArray = new PrimitivePropertyValueArray();
    PrimitivePropertyValue prop1 = new PrimitivePropertyValue();
    prop1.setString("prop1");
    primitivePropertyArray.add(prop1);
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn("urn:li:corpuser:user"));
    StructuredPropertyValueAssignment valueAssignment =
        new StructuredPropertyValueAssignment()
            .setValues(primitivePropertyArray)
            .setPropertyUrn(STRUCTURED_PROPERTY_URN)
            .setCreated(auditStamp)
            .setLastModified(auditStamp);
    StructuredPropertyValueAssignmentArray valueAssignments =
        new StructuredPropertyValueAssignmentArray();
    valueAssignments.add(valueAssignment);
    StructuredProperties structuredProperties =
        new StructuredProperties().setProperties(valueAssignments);

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(schemaMetadata.data())),
                        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(new Aspect(editableSchemaMetadata.data()))))));

    Map<Urn, EntityResponse> mockStructuredPropResponses = new HashMap<>();
    mockStructuredPropResponses.put(
        STRUCTURED_PROPERTY_URN,
        new EntityResponse()
            .setUrn(schemaFieldUrn)
            .setEntityName(SCHEMA_FIELD_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        STRUCTURED_PROPERTIES_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(structuredProperties.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(mockResponses);
    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(SCHEMA_FIELD_ENTITY_NAME),
            eq(Collections.singleton(schemaFieldUrn)),
            eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockStructuredPropResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, "dataset", urns, queries);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().size(), 1);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().get(0),
        STRUCTURED_PROPERTY_URN.toString());
  }

  @Test
  public void testEvaluateSharedStructuredPropWithValidResponses() throws URISyntaxException {
    // Sets up two schema fields, one with two structured properties, prop1 and prop2 and the other
    // with just prop1,
    // validates that the shared result only has prop1
    Set<Urn> urns = new HashSet<>(Arrays.asList(TEST_DATASET_URN));
    TestQuery testQuery =
        new TestQuery(
            TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY
                + "."
                + STRUCTURED_PROPERTIES_ASPECT_NAME
                + "."
                + TestsSchemaFieldUtils.SHARED_PROPERTIES);
    assertTrue(evaluator.isEligible(DATASET_ENTITY_NAME, testQuery));
    Set<TestQuery> queries = new HashSet<>(Arrays.asList(testQuery));
    Urn schemaFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "path");
    Urn schemaFieldUrn2 = SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "path2");

    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setHash("hash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new OtherSchema()));
    schemaMetadata.setVersion(0L);
    schemaMetadata.setFields(
        new SchemaFieldArray(
            ImmutableList.of(
                new SchemaField()
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new MapType())))
                    .setFieldPath("path")
                    .setDescription("description"),
                new SchemaField()
                    .setType(
                        new SchemaFieldDataType()
                            .setType(SchemaFieldDataType.Type.create(new MapType())))
                    .setFieldPath("path2")
                    .setDescription("description"))));

    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    editableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(
            ImmutableList.of(
                new EditableSchemaFieldInfo()
                    .setDescription("editableDescription")
                    .setFieldPath("path"),
                new EditableSchemaFieldInfo()
                    .setDescription("editableDescription")
                    .setFieldPath("path2"))));

    PrimitivePropertyValueArray primitivePropertyArray = new PrimitivePropertyValueArray();
    PrimitivePropertyValue prop1 = new PrimitivePropertyValue();
    prop1.setString("prop1");
    primitivePropertyArray.add(prop1);
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn("urn:li:corpuser:user"));
    StructuredPropertyValueAssignment valueAssignment =
        new StructuredPropertyValueAssignment()
            .setValues(primitivePropertyArray)
            .setPropertyUrn(STRUCTURED_PROPERTY_URN)
            .setCreated(auditStamp)
            .setLastModified(auditStamp);
    StructuredPropertyValueAssignment valueAssignment2 =
        new StructuredPropertyValueAssignment()
            .setValues(primitivePropertyArray)
            .setPropertyUrn(STRUCTURED_PROPERTY_URN_2)
            .setCreated(auditStamp)
            .setLastModified(auditStamp);
    StructuredPropertyValueAssignmentArray valueAssignments =
        new StructuredPropertyValueAssignmentArray();
    valueAssignments.add(valueAssignment);
    valueAssignments.add(valueAssignment2);
    StructuredProperties structuredProperties =
        new StructuredProperties().setProperties(valueAssignments);
    StructuredPropertyValueAssignmentArray valueAssignments2 =
        new StructuredPropertyValueAssignmentArray();
    valueAssignments2.add(valueAssignment);
    StructuredProperties structuredProperties2 =
        new StructuredProperties().setProperties(valueAssignments2);

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(schemaMetadata.data())),
                        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(new Aspect(editableSchemaMetadata.data()))))));

    Map<Urn, EntityResponse> mockStructuredPropResponses = new HashMap<>();
    mockStructuredPropResponses.put(
        STRUCTURED_PROPERTY_URN,
        new EntityResponse()
            .setUrn(schemaFieldUrn)
            .setEntityName(SCHEMA_FIELD_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        STRUCTURED_PROPERTIES_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(structuredProperties.data()))))));
    mockStructuredPropResponses.put(
        STRUCTURED_PROPERTY_URN,
        new EntityResponse()
            .setUrn(schemaFieldUrn2)
            .setEntityName(SCHEMA_FIELD_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        STRUCTURED_PROPERTIES_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(new Aspect(structuredProperties2.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(mockResponses);
    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(SCHEMA_FIELD_ENTITY_NAME),
            eq(ImmutableSet.of(schemaFieldUrn, schemaFieldUrn2)),
            eq(ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(mockStructuredPropResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, "dataset", urns, queries);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().size(), 1);
    assertEquals(
        results.get(TEST_DATASET_URN).get(queries.iterator().next()).getValues().get(0),
        STRUCTURED_PROPERTY_URN.toString());
  }
}
