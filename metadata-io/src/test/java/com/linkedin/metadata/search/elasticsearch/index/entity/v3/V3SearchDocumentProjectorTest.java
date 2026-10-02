package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.mxe.SystemMetadata;
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
    when(entitySpec.getName()).thenReturn("dataset");
    when(entitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(auditStamp.getTime()).thenReturn(123L);
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

    assertEquals(runId.get("copy_to"), List.of("_search._system_runId"));
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
}
