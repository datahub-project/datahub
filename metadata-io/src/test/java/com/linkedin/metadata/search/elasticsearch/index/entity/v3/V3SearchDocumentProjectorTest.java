package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V3SearchDocumentProjectorTest {

  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private MCLItem event;
  @Mock private AspectSpec aspectSpec;
  @Mock private EntitySpec entitySpec;
  @Mock private RecordTemplate aspect;
  @Mock private RecordTemplate previousAspect;
  @Mock private AuditStamp auditStamp;

  private OperationContext opContext;
  private V3SearchDocumentProjector projector;
  private Urn urn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    projector = new V3SearchDocumentProjector(searchDocumentTransformer);
    urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.my_table,PROD)");

    when(event.getUrn()).thenReturn(urn);
    when(event.getAspectName()).thenReturn("datasetProperties");
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getEntitySpec()).thenReturn(entitySpec);
    when(event.getRecordTemplate()).thenReturn(aspect);
    when(event.getPreviousRecordTemplate()).thenReturn(previousAspect);
    when(event.getAuditStamp()).thenReturn(auditStamp);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(aspectSpec.getName()).thenReturn("datasetProperties");
    when(aspectSpec.getSearchableFieldSpecs()).thenReturn(List.of());
    when(entitySpec.getName()).thenReturn("dataset");
    when(entitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(auditStamp.getTime()).thenReturn(123L);
  }

  @Test
  public void testAnnotatedUrnReferencesProjectIdentityOnTheAspect() throws Exception {
    SearchDocumentTransformer transformer =
        new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);
    V3SearchDocumentProjector realProjector = new V3SearchDocumentProjector(transformer);
    EntityRegistry registry = opContext.getEntityRegistry();

    EntitySpec chart = registry.getEntitySpec("chart");
    GlobalTags tags = new GlobalTags();
    tags.setTags(new TagAssociationArray(new TagAssociation().setTag(new TagUrn("Confidential"))));
    when(event.getUrn()).thenReturn(UrnUtils.getUrn("urn:li:chart:(looker,orders)"));
    when(event.getAspectName()).thenReturn("globalTags");
    when(event.getAspectSpec()).thenReturn(chart.getAspectSpec("globalTags"));
    when(event.getEntitySpec()).thenReturn(chart);
    when(event.getRecordTemplate()).thenReturn(tags);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(event.getSystemMetadata()).thenReturn(null);

    V3SearchDocumentProjector.ProjectedAspect tagsProjection =
        realProjector.projectAspect(opContext, event).orElseThrow();
    assertEquals(
        tagsProjection.aspectFields().get("tags__urn_name").get(0).asText(), "Confidential");
    assertEquals(tagsProjection.aspectFields().get("tags__urn_name").size(), 1);
    assertTrue(tagsProjection.rootFields().get("tags").isNull());
    assertEquals(tagsProjection.rootFields().get("urn_name").asText(), "looker orders");

    EntitySpec dataset = registry.getEntitySpec("dataset");
    DatasetKey datasetKey =
        new DatasetKey()
            .setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:hive"))
            .setName("sales.orders")
            .setOrigin(FabricType.PROD);
    when(event.getUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)"));
    when(event.getAspectName()).thenReturn("datasetKey");
    when(event.getAspectSpec()).thenReturn(dataset.getAspectSpec("datasetKey"));
    when(event.getEntitySpec()).thenReturn(dataset);
    when(event.getRecordTemplate()).thenReturn(datasetKey);

    V3SearchDocumentProjector.ProjectedAspect datasetProjection =
        realProjector.projectAspect(opContext, event).orElseThrow();
    assertEquals(
        datasetProjection.aspectFields().get("platform__urn_name").get(0).asText(), "hive");
    assertEquals(datasetProjection.aspectFields().get("platform__urn_name").size(), 1);
    assertTrue(
        datasetProjection.aspectFields().get("platform").asText().contains("dataPlatform:hive"));
    assertTrue(datasetProjection.rootFields().get("platform").isNull());
    assertTrue(datasetProjection.rootFields().path("urnName").isMissingNode());
    assertEquals(datasetProjection.rootFields().get("urn_name").asText(), "sales.orders");
    assertTrue(datasetProjection.rootFields().path("name").isMissingNode());

    EntitySpec schemaField = registry.getEntitySpec("schemaField");
    SchemaFieldKey schemaFieldKey =
        new SchemaFieldKey()
            .setParent(
                UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)"))
            .setFieldPath("[version=2.0].[type=string].order_id");
    when(event.getUrn())
        .thenReturn(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD),[version=2.0].[type=string].order_id)"));
    when(event.getAspectName()).thenReturn("schemaFieldKey");
    when(event.getAspectSpec()).thenReturn(schemaField.getAspectSpec("schemaFieldKey"));
    when(event.getEntitySpec()).thenReturn(schemaField);
    when(event.getRecordTemplate()).thenReturn(schemaFieldKey);

    V3SearchDocumentProjector.ProjectedAspect fieldProjection =
        realProjector.projectAspect(opContext, event).orElseThrow();
    assertEquals(
        fieldProjection.aspectFields().get("parent__urn_name").get(0).asText(), "sales.orders");
    assertEquals(fieldProjection.aspectFields().get("parent__urn_name").size(), 1);
    assertTrue(fieldProjection.rootFields().get("parent").isNull());
    assertEquals(fieldProjection.rootFields().get("urn_name").asText(), "order_id");
  }

  @Test
  public void testProjectsRootFieldsAndAspectFieldsWithRemovalTombstones() throws Exception {
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    currentDocument.put("name", "new name");

    ObjectNode previousDocument = JsonNodeFactory.instance.objectNode();
    previousDocument.put("urn", urn.toString());
    previousDocument.put("name", "old name");
    previousDocument.put("description", "old description");

    SystemMetadata systemMetadata = new SystemMetadata().setRunId("run-1");
    when(event.getSystemMetadata()).thenReturn(systemMetadata);
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(previousAspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(previousDocument));

    V3SearchDocumentProjector.ProjectedAspect projectedAspect =
        projector.projectAspect(opContext, event).orElseThrow();
    ObjectNode document = projector.newEntityDocument(urn, entitySpec);
    projector.applyProjection(document, projectedAspect);

    assertEquals(document.get("urn").asText(), urn.toString());
    assertEquals(document.get("_entityType").asText(), "dataset");
    assertEquals(document.get("name").asText(), "new name");
    assertTrue(document.get("description").isNull());
    assertFalse(document.has("_systemMetadata"));
    assertFalse(document.has("_systemmetadata"));

    ObjectNode aspectNode =
        (ObjectNode) document.get(MappingConstants.ASPECTS_FIELD_NAME).get("datasetProperties");
    assertEquals(aspectNode.get("name").asText(), "new name");
    assertTrue(aspectNode.get("description").isNull());
    assertEquals(aspectNode.get("_systemMetadata").get("runId").asText(), "run-1");
    assertFalse(aspectNode.has("_systemmetadata"));
  }

  @Test
  public void testBackfillRestateOfTheKeyAspectRecordsTheCreationTime() throws Exception {
    // A backfill restates stored rows; its key aspect carries the row's creation time, which the
    // document records as it does on the upsert that created the entity. A live restate does not.
    ObjectNode keyDocument = JsonNodeFactory.instance.objectNode();
    keyDocument.put("urn", urn.toString());
    when(event.getAspectName()).thenReturn("datasetKey");
    when(aspectSpec.getName()).thenReturn("datasetKey");
    when(event.getChangeType()).thenReturn(ChangeType.RESTATE);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenAnswer(invocation -> Optional.of(keyDocument.deepCopy()));
    when(event.getMetadataChangeLog())
        .thenReturn(
            new MetadataChangeLog()
                .setHeaders(
                    new StringMap(
                        Map.of(BackfillHeaders.MODE_HEADER_NAME, BackfillHeaders.MODE_BASELINE))));

    V3SearchDocumentProjector.ProjectedAspect backfilled =
        projector.projectAspect(opContext, event).orElseThrow();
    assertEquals(backfilled.rootFields().get("systemCreated").asLong(), 123L);

    when(event.getMetadataChangeLog()).thenReturn(new MetadataChangeLog());
    V3SearchDocumentProjector.ProjectedAspect live =
        projector.projectAspect(opContext, event).orElseThrow();
    assertFalse(live.rootFields().has("systemCreated"));
  }

  @Test
  public void testStructuredPropertiesStayRootOnly() throws Exception {
    when(aspectSpec.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    currentDocument.put("structuredProperties.my_property", "value");

    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));

    V3SearchDocumentProjector.ProjectedAspect projectedAspect =
        projector.projectAspect(opContext, event).orElseThrow();
    ObjectNode document = projector.newEntityDocument(urn, entitySpec);
    projector.applyProjection(document, projectedAspect);

    assertTrue(projectedAspect.rootOnly());
    assertEquals(document.get("structuredProperties.my_property").asText(), "value");
    assertFalse(document.has(MappingConstants.ASPECTS_FIELD_NAME));
  }

  @Test
  public void testDeleteProjectionUsesDeleteTransform() throws Exception {
    when(event.getChangeType()).thenReturn(ChangeType.DELETE);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    ObjectNode deleteDocument = JsonNodeFactory.instance.objectNode();
    deleteDocument.put("urn", urn.toString());
    deleteDocument.set("name", JsonNodeFactory.instance.nullNode());

    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(true), eq(auditStamp)))
        .thenReturn(Optional.of(deleteDocument));

    V3SearchDocumentProjector.ProjectedAspect projectedAspect =
        projector.projectAspect(opContext, event).orElseThrow();

    assertTrue(projectedAspect.rootFields().get("name").isNull());
    assertTrue(projectedAspect.aspectFields().get("name").isNull());
  }

  @Test
  public void testProjectAspectPropagatesTransformerFailure() throws Exception {
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenThrow(new IllegalStateException("transform failed"));

    assertThrows(IllegalStateException.class, () -> projector.projectAspect(opContext, event));
  }

  @Test
  public void testProjectAspectWrapsCheckedPreviousTransformFailure() throws Exception {
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(JsonNodeFactory.instance.objectNode()));
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(previousAspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenThrow(new URISyntaxException("bad", "previous transform failed"));

    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> projector.projectAspect(opContext, event));
    assertTrue(exception.getCause() instanceof URISyntaxException);
  }

  @Test
  public void testProjectAspectReturnsEmptyForValidNoProjectionAspect() throws Exception {
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.empty());

    assertTrue(projector.projectAspect(opContext, event).isEmpty());
  }

  @Test
  public void testProjectAspectPropagatesSystemMetadataSerializationFailure() throws Exception {
    OperationContext failingContext = mock(OperationContext.class);
    ObjectMapper objectMapper = mock(ObjectMapper.class);
    when(failingContext.getObjectMapper()).thenReturn(objectMapper);
    when(failingContext.getEntityRegistry()).thenReturn(opContext.getEntityRegistry());
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(event.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("run-1"));
    when(searchDocumentTransformer.transformAspect(
            eq(failingContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(JsonNodeFactory.instance.objectNode()));
    when(objectMapper.readTree(anyString()))
        .thenThrow(
            new JsonProcessingException("serialization failed") {
              private static final long serialVersionUID = 1L;
            });

    IllegalStateException exception =
        expectThrows(
            IllegalStateException.class, () -> projector.projectAspect(failingContext, event));
    assertTrue(exception.getCause() instanceof JsonProcessingException);
  }

  @Test
  public void testSystemMetadataMappingMatchesProjectedFieldSpelling() {
    when(entitySpec.getAspectSpecs()).thenReturn(java.util.List.of(aspectSpec));
    when(aspectSpec.getSearchableFieldSpecs()).thenReturn(java.util.List.of());

    Map<String, Object> mappings =
        AspectMappingBuilder.createAspectMappings(entitySpec, null, null);
    @SuppressWarnings("unchecked")
    Map<String, Object> aspectMapping = (Map<String, Object>) mappings.get("datasetProperties");
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) aspectMapping.get("properties");
    assertTrue(properties.containsKey("_systemMetadata"));
    assertFalse(properties.containsKey("_systemmetadata"));
    @SuppressWarnings("unchecked")
    Map<String, Object> systemMetadata = (Map<String, Object>) properties.get("_systemMetadata");
    @SuppressWarnings("unchecked")
    Map<String, Object> systemMetadataProperties =
        (Map<String, Object>) systemMetadata.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> runId = (Map<String, Object>) systemMetadataProperties.get("runId");

    assertEquals(runId.get("copy_to"), List.of("_system.runId"));
    assertEquals(V3SearchDocumentProjector.SYSTEM_METADATA_FIELD, "_systemMetadata");
  }

  @Test
  public void testSemanticContentStaysUnderAspects() throws Exception {
    when(aspectSpec.getName()).thenReturn("semanticContent");
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    currentDocument.set("embeddings", JsonNodeFactory.instance.objectNode());
    currentDocument.put("skipReason", "EMPTY_TEXT");
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));

    ObjectNode document = projector.newEntityDocument(urn, entitySpec);
    projector.applyProjection(document, projector.projectAspect(opContext, event).orElseThrow());

    // The strategy lifts vectors to the root only for semantic-enabled entities
    assertFalse(document.has("embeddings"));
    assertFalse(document.has("skipReason"));
    ObjectNode aspectNode =
        (ObjectNode) document.get(MappingConstants.ASPECTS_FIELD_NAME).get("semanticContent");
    assertTrue(aspectNode.has("embeddings"));
    assertEquals(aspectNode.get("skipReason").asText(), "EMPTY_TEXT");
  }

  @Test
  public void testEditedDescriptionSetsSharedHasValuesWithoutARootFalse() throws Exception {
    V3SearchDocumentProjector realProjector = realProjector();
    EntityRegistry registry = opContext.getEntityRegistry();
    EntitySpec dataset = registry.getEntitySpec("dataset");
    AspectSpec editable = dataset.getAspectSpec("editableDatasetProperties");
    when(event.getEntitySpec()).thenReturn(dataset);
    when(event.getAspectSpec()).thenReturn(editable);
    when(event.getAspectName()).thenReturn(editable.getName());
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    EditableDatasetProperties properties = new EditableDatasetProperties();
    properties.setDescription("An edited description");
    when(event.getRecordTemplate()).thenReturn(properties);

    V3SearchDocumentProjector.ProjectedAspect projected =
        realProjector.projectAspect(opContext, event).orElseThrow();

    assertTrue(projected.aspectFields().get("hasDescription").booleanValue());
    assertTrue(projected.rootFields().get("hasDescription").isNull());
    assertEquals(
        projected.aspectFields().get("editedDescription").asText(), "An edited description");
  }

  @Test
  public void testClearingTheOnlyDescriptionRemovesSharedHasValues() throws Exception {
    V3SearchDocumentProjector realProjector = realProjector();
    EntityRegistry registry = opContext.getEntityRegistry();
    EntitySpec dataset = registry.getEntitySpec("dataset");
    AspectSpec editable = dataset.getAspectSpec("editableDatasetProperties");
    when(event.getEntitySpec()).thenReturn(dataset);
    when(event.getAspectSpec()).thenReturn(editable);
    when(event.getAspectName()).thenReturn(editable.getName());
    EditableDatasetProperties previous = new EditableDatasetProperties();
    previous.setDescription("An edited description");
    when(event.getPreviousRecordTemplate()).thenReturn(previous);
    when(event.getRecordTemplate()).thenReturn(new EditableDatasetProperties());

    V3SearchDocumentProjector.ProjectedAspect projected =
        realProjector.projectAspect(opContext, event).orElseThrow();

    assertFalse(projected.aspectFields().has("hasDescription"));
    assertTrue(projected.rootFields().get("hasDescription").isNull());
  }

  @Test
  public void testAspectJsonIsStoredAndNotIndexed() throws Exception {
    EditableDatasetProperties properties = new EditableDatasetProperties();
    properties.setDescription("Order events");
    when(aspectSpec.getAspectAnnotation())
        .thenReturn(new AspectAnnotation("datasetProperties", false, false, null, 1L, true));
    when(event.getRecordTemplate()).thenReturn(properties);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    currentDocument.put("description", "Order events");
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(properties), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));

    V3SearchDocumentProjector.ProjectedAspect projected =
        projector.projectAspect(opContext, event).orElseThrow();

    JsonNode stored =
        opContext
            .getObjectMapper()
            .readTree(
                projected.aspectFields().get(V3SearchDocumentProjector.ASPECT_JSON_FIELD).asText());
    assertEquals(stored.get("description").asText(), "Order events");
    assertFalse(projected.rootFields().has(V3SearchDocumentProjector.ASPECT_JSON_FIELD));
    assertAspectJsonNotIndexed();
  }

  @Test
  public void testAspectJsonDropsAClearedField() throws Exception {
    when(aspectSpec.getAspectAnnotation())
        .thenReturn(new AspectAnnotation("datasetProperties", false, false, null, 1L, true));
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getRecordTemplate()).thenReturn(new EditableDatasetProperties());
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    when(searchDocumentTransformer.transformAspect(
            eq(opContext),
            eq(urn),
            any(EditableDatasetProperties.class),
            eq(aspectSpec),
            eq(false),
            eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));

    V3SearchDocumentProjector.ProjectedAspect projected =
        projector.projectAspect(opContext, event).orElseThrow();

    JsonNode stored =
        opContext
            .getObjectMapper()
            .readTree(
                projected.aspectFields().get(V3SearchDocumentProjector.ASPECT_JSON_FIELD).asText());
    assertFalse(stored.has("description"));
  }

  @Test
  public void testAspectJsonDeleteClearsStoredAspect() throws Exception {
    when(aspectSpec.getAspectAnnotation())
        .thenReturn(new AspectAnnotation("datasetProperties", false, false, null, 1L, true));
    when(event.getChangeType()).thenReturn(ChangeType.DELETE);
    when(event.getRecordTemplate()).thenReturn(null);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    ObjectNode deleteDocument = JsonNodeFactory.instance.objectNode();
    deleteDocument.put("urn", urn.toString());
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), isNull(), eq(aspectSpec), eq(true), eq(auditStamp)))
        .thenReturn(Optional.of(deleteDocument));

    V3SearchDocumentProjector.ProjectedAspect projected =
        projector.projectAspect(opContext, event).orElseThrow();

    assertTrue(projected.aspectFields().get(V3SearchDocumentProjector.ASPECT_JSON_FIELD).isNull());
    assertFalse(projected.rootFields().has(V3SearchDocumentProjector.ASPECT_JSON_FIELD));
  }

  @Test
  public void testAspectJsonStaysUnsetWhenAspectDoesNotOptIn() throws Exception {
    when(aspectSpec.getAspectAnnotation())
        .thenReturn(new AspectAnnotation("datasetProperties", false, false, null, 1L, false));
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    ObjectNode currentDocument = JsonNodeFactory.instance.objectNode();
    currentDocument.put("urn", urn.toString());
    currentDocument.put("name", "orders");
    when(searchDocumentTransformer.transformAspect(
            eq(opContext), eq(urn), eq(aspect), eq(aspectSpec), eq(false), eq(auditStamp)))
        .thenReturn(Optional.of(currentDocument));

    V3SearchDocumentProjector.ProjectedAspect projected =
        projector.projectAspect(opContext, event).orElseThrow();

    assertFalse(projected.aspectFields().has(V3SearchDocumentProjector.ASPECT_JSON_FIELD));
    assertFalse(projected.rootFields().has(V3SearchDocumentProjector.ASPECT_JSON_FIELD));
  }

  private void assertAspectJsonNotIndexed() {
    AspectSpec storedAspect = mock(AspectSpec.class);
    when(storedAspect.getName()).thenReturn("datasetProperties");
    when(storedAspect.getAspectAnnotation())
        .thenReturn(new AspectAnnotation("datasetProperties", false, false, null, 1L, true));
    when(storedAspect.getSearchableFieldSpecs()).thenReturn(List.of());
    EntitySpec storedEntity = mock(EntitySpec.class);
    when(storedEntity.getAspectSpecs()).thenReturn(List.of(storedAspect));

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectMapping =
        (Map<String, Object>)
            ((Map<String, Object>)
                    AspectMappingBuilder.createAspectMappings(storedEntity, Map.of(), Map.of())
                        .get("datasetProperties"))
                .get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> stored =
        (Map<String, Object>) aspectMapping.get(V3SearchDocumentProjector.ASPECT_JSON_FIELD);
    assertEquals(stored.get("type"), "text");
    assertEquals(stored.get("index"), false);
  }

  @Test
  public void testAspectJsonReadsStringAndObject() {
    ObjectMapper mapper = opContext.getObjectMapper();
    ObjectNode aspect = mapper.createObjectNode();
    aspect.put(V3SearchDocumentProjector.ASPECT_JSON_FIELD, "{\"description\":\"orders\"}");
    assertEquals(V3SearchDocumentProjector.aspectJson(aspect), "{\"description\":\"orders\"}");

    ObjectNode objectValue = mapper.createObjectNode();
    objectValue.put("description", "orders");
    aspect.set(V3SearchDocumentProjector.ASPECT_JSON_FIELD, objectValue);
    assertEquals(V3SearchDocumentProjector.aspectJson(aspect), objectValue.toString());

    assertNull(V3SearchDocumentProjector.aspectJson(mapper.createObjectNode()));
  }

  @Test
  public void testSearchTemplateBodyIsStoredAndNotIndexed() throws Exception {
    V3SearchDocumentProjector realProjector = realProjector();
    EntityRegistry registry = opContext.getEntityRegistry();
    EntitySpec entity = registry.getEntitySpec("searchTemplate");
    AspectSpec aspect = entity.getAspectSpec("searchTemplateProperties");
    assertTrue(aspect.getAspectAnnotation().isAspectJson());
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName("Fulltext");
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setQuery("{ \"match_all\": {} }");
    when(event.getUrn()).thenReturn(UrnUtils.getUrn("urn:li:searchTemplate:fulltext"));
    when(event.getEntitySpec()).thenReturn(entity);
    when(event.getAspectSpec()).thenReturn(aspect);
    when(event.getAspectName()).thenReturn(aspect.getName());
    when(event.getRecordTemplate()).thenReturn(properties);
    when(event.getPreviousRecordTemplate()).thenReturn(null);
    when(event.getSystemMetadata()).thenReturn(null);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);

    V3SearchDocumentProjector.ProjectedAspect projected =
        realProjector.projectAspect(opContext, event).orElseThrow();
    JsonNode stored =
        opContext
            .getObjectMapper()
            .readTree(
                projected.aspectFields().get(V3SearchDocumentProjector.ASPECT_JSON_FIELD).asText());
    assertEquals(stored.get("query").asText(), "{ \"match_all\": {} }");
    assertFalse(projected.rootFields().has(V3SearchDocumentProjector.ASPECT_JSON_FIELD));

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectMapping =
        (Map<String, Object>)
            ((Map<String, Object>)
                    AspectMappingBuilder.createAspectMappings(entity, Map.of(), Map.of())
                        .get("searchTemplateProperties"))
                .get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> mapping =
        (Map<String, Object>) aspectMapping.get(V3SearchDocumentProjector.ASPECT_JSON_FIELD);
    assertEquals(mapping.get("type"), "text");
    assertEquals(mapping.get("index"), false);
    assertTrue(aspectMapping.containsKey("parent"));
    assertEquals(((Map<String, Object>) aspectMapping.get("dialect")).get("type"), "keyword");
    assertEquals(((Map<String, Object>) aspectMapping.get("apiVersion")).get("type"), "integer");
    assertEquals(((Map<String, Object>) aspectMapping.get("editable")).get("type"), "boolean");
    assertFalse(aspectMapping.containsKey("query"));
  }

  private V3SearchDocumentProjector realProjector() {
    return new V3SearchDocumentProjector(
        new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH));
  }
}
