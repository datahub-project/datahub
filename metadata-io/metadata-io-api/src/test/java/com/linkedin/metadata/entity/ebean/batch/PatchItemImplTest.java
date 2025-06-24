package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class PatchItemImplTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final ObjectMapper objectMapper = opContext.getObjectMapper();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private final AspectRetriever aspectRetriever = opContext.getAspectRetriever();
  private final Urn urn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,patchTest,PROD)");
  ;
  private final AuditStamp auditStamp =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));

  @Test
  public void testSimplePatch() {
    // Create a simple JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch)
            .build(entityRegistry);

    // Verify the built object
    assertEquals(patchItem.getUrn(), urn);
    assertEquals(patchItem.getAspectName(), DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(patchItem.getAuditStamp(), auditStamp);
    assertEquals(patchItem.getPatch(), patch);
    assertEquals(patchItem.getChangeType(), ChangeType.PATCH);
    assertNotNull(
        patchItem.getSystemMetadata(), "System metadata should be auto-generated if not provided");
    assertNull(patchItem.getRecordTemplate(), "Record template should be null for PatchItemImpl");
  }

  @Test
  public void testBuildWithMCP() {
    // Create a MetadataChangeProposal
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);

    // Create a JSON patch array string
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());
    mcp.setAspect(GenericRecordUtils.serializePatch(patch));

    // Build a PatchItemImpl with MCP
    PatchItemImpl patchItem = PatchItemImpl.builder().build(mcp, auditStamp, entityRegistry);

    // Verify the built object
    assertEquals(patchItem.getUrn(), urn);
    assertEquals(patchItem.getAspectName(), DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(patchItem.getAuditStamp(), auditStamp);
    assertNotNull(patchItem.getPatch(), "Patch should be extracted from MCP");
    assertEquals(patchItem.getMetadataChangeProposal(), mcp);
    assertEquals(patchItem.getChangeType(), ChangeType.PATCH);
  }

  @Test
  public void testBuildWithGenericJsonPatch() throws Exception {
    // Create a GenericJsonPatch
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/tags/urn:li:platformResource:my-source/urn:li:tag:tag1");
    patchOp.setValue(
        objectMapper.convertValue(
            Map.of("tag", "urn:li:tag:tag1", "source", "urn:li:platformResource:my-source"),
            JsonNode.class));

    Map<String, List<String>> arrayPrimaryKeys = new HashMap<>();
    arrayPrimaryKeys.put("tags", List.of("attribution", "source"));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(arrayPrimaryKeys)
            .build();

    // Create MCP with GenericJsonPatch
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);
    mcp.setAspect(GenericRecordUtils.serializePatch(genericJsonPatch, objectMapper));

    // Build a PatchItemImpl with MCP containing GenericJsonPatch
    PatchItemImpl patchItem = PatchItemImpl.builder().build(mcp, auditStamp, entityRegistry);

    // Verify the built object
    assertEquals(patchItem.getUrn(), urn);
    assertEquals(patchItem.getAspectName(), GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(patchItem.getAuditStamp(), auditStamp);
    assertNotNull(patchItem.getPatch(), "JsonPatch should be extracted from GenericJsonPatch");
    assertNotNull(patchItem.getGenericJsonPatch(), "GenericJsonPatch should be extracted from MCP");
    assertEquals(patchItem.getMetadataChangeProposal(), mcp);
  }

  @Test
  public void testApplyPatchWithExistingRecord() throws Exception {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch)
            .build(entityRegistry);

    // Apply the patch
    ChangeItemImpl result = patchItem.applyPatch(new DatasetProperties(), aspectRetriever);

    // Verify the result
    assertNotNull(result);
    assertEquals(result.getUrn(), urn);
    assertEquals(result.getAspectName(), DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(
        result.getRecordTemplate(),
        new DatasetProperties().setDescription("Test description").setTags(new StringArray()));
  }

  @Test
  public void testApplyPatchWithDefaultTemplate() throws Exception {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch)
            .build(entityRegistry);

    // Apply the patch with no existing record
    ChangeItemImpl result = patchItem.applyPatch(null, aspectRetriever);

    // Verify the result
    assertNotNull(result);
    assertEquals(result.getUrn(), urn);
    assertEquals(result.getAspectName(), DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(
        result.getRecordTemplate(),
        new DatasetProperties()
            .setDescription("Test description")
            .setTags(new StringArray())
            .setCustomProperties(new StringMap()));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testApplyPatchWithNoExistingOrDefaultTemplate() {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader("[{\"op\":\"add\",\"path\":\"/removed\",\"value\":false}]"))
                .readArray());

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(STATUS_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch)
            .build(entityRegistry);

    // This should throw UnsupportedOperationException
    patchItem.applyPatch(null, aspectRetriever);
  }

  @Test
  public void testGetMetadataChangeProposal() {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Create SystemMetadata
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId("test-run-id");

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .systemMetadata(systemMetadata)
            .patch(patch)
            .build(entityRegistry);

    // Get the MCP
    MetadataChangeProposal mcp = patchItem.getMetadataChangeProposal();

    // Verify the MCP
    assertNotNull(mcp);
    assertEquals(mcp.getEntityUrn(), urn);
    assertEquals(mcp.getAspectName(), DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(mcp.getEntityType(), DATASET_ENTITY_NAME);
    assertEquals(mcp.getChangeType(), ChangeType.PATCH);
    assertNotNull(mcp.getAspect());
    assertEquals(mcp.getSystemMetadata(), systemMetadata);
    assertNotNull(mcp.getEntityKeyAspect());
  }

  @Test
  public void testApplyGenericPatch() throws Exception {
    // Create a GenericJsonPatch
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/description");
    patchOp.setValue("Test description");

    List<GenericJsonPatch.PatchOp> patchOps = new ArrayList<>();
    patchOps.add(patchOp);

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(patchOps)
            .arrayPrimaryKeys(Map.of())
            .forceGenericPatch(true)
            .build();

    // Build PatchItemImpl with genericJsonPatch
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .genericJsonPatch(genericJsonPatch)
            .patch(genericJsonPatch.getJsonPatch())
            .build(entityRegistry);

    assertNotNull(patchItem.getGenericJsonPatch());

    // Apply the generic patch
    ChangeItemImpl result = patchItem.applyPatch(new DatasetProperties(), aspectRetriever);

    // Verify the result
    assertTrue(
        DataTemplateUtil.areEqual(
            result.getRecordTemplate(),
            new DatasetProperties().setDescription("Test description")));
  }

  @Test
  public void testEqualsAndHashCode() {
    // Create two identical JSON patches
    JsonPatch patch1 =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());
    JsonPatch patch2 =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Create two identical system metadata objects
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId("test-run-id");

    // Build two PatchItemImpl objects with the same values
    PatchItemImpl patchItem1 =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .systemMetadata(systemMetadata)
            .patch(patch1)
            .build(entityRegistry);

    PatchItemImpl patchItem2 =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .systemMetadata(systemMetadata)
            .patch(patch2)
            .build(entityRegistry);

    // Verify equals and hashCode
    assertTrue(patchItem1.equals(patchItem2));
    assertEquals(patchItem1.hashCode(), patchItem2.hashCode());

    // Create a different patch
    JsonPatch differentPatch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Different description\"}]"))
                .readArray());

    // Build a PatchItemImpl with a different patch
    PatchItemImpl differentPatchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .systemMetadata(systemMetadata)
            .patch(differentPatch)
            .build(entityRegistry);

    // Verify not equal
    assertFalse(patchItem1.equals(differentPatchItem));
    assertNotEquals(patchItem1.hashCode(), differentPatchItem.hashCode());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testBuildWithUnsupportedChangeType() {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // This should throw UnsupportedOperationException
    PatchItemImpl.builder()
        .urn(urn)
        .aspectName(STATUS_ASPECT_NAME)
        .auditStamp(auditStamp)
        .patch(patch)
        .aspectSpec(entityRegistry.getAspectSpecs().get(STATUS_ASPECT_NAME))
        .build(entityRegistry);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildWithNullPatch() {
    // This should throw IllegalArgumentException
    PatchItemImpl.builder()
        .urn(urn)
        .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
        .auditStamp(auditStamp)
        .patch(null)
        .build(entityRegistry);
  }

  @Test
  public void testSetSystemMetadata() {
    // Create a JSON patch
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());

    // Build a PatchItemImpl
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch)
            .build(entityRegistry);

    // Create a new SystemMetadata
    SystemMetadata newSystemMetadata = new SystemMetadata();
    newSystemMetadata.setRunId("new-run-id");

    // Set the new SystemMetadata
    patchItem.setSystemMetadata(newSystemMetadata);

    // Verify the SystemMetadata was set
    assertEquals(patchItem.getSystemMetadata(), newSystemMetadata);

    // Get the MCP and verify it has the new SystemMetadata
    MetadataChangeProposal mcp = patchItem.getMetadataChangeProposal();
    assertEquals(mcp.getSystemMetadata(), newSystemMetadata);
  }

  @Test
  public void testIsDatabaseDuplicateOf() {
    // Create two identical JSON patches
    JsonPatch patch1 =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());
    JsonPatch patch2 =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Test description\"}]"))
                .readArray());
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();

    // Build two PatchItemImpl objects with the same values
    PatchItemImpl patchItem1 =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch1)
            .systemMetadata(systemMetadata)
            .build(entityRegistry);

    PatchItemImpl patchItem2 =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(patch2)
            .systemMetadata(systemMetadata)
            .build(entityRegistry);

    // Verify isDatabaseDuplicateOf returns true for identical items
    assertTrue(patchItem1.isDatabaseDuplicateOf(patchItem2));

    // Create a different patch
    JsonPatch differentPatch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/description\",\"value\":\"Different description\"}]"))
                .readArray());

    // Build a PatchItemImpl with a different patch
    PatchItemImpl differentPatchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(differentPatch)
            .build(entityRegistry);

    // Verify isDatabaseDuplicateOf returns false for different items
    assertFalse(patchItem1.isDatabaseDuplicateOf(differentPatchItem));
  }

  @Test
  public void testApplyGenericPatchWithForceFlag() throws Exception {
    // Create a GenericJsonPatch with force flag
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/description");
    patchOp.setValue("Test description");

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(Map.of()) // Empty primary keys
            .forceGenericPatch(true) // Force generic patch
            .build();

    // Build PatchItemImpl with genericJsonPatch
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .genericJsonPatch(genericJsonPatch)
            .patch(genericJsonPatch.getJsonPatch())
            .build(entityRegistry);

    // Apply the generic patch
    ChangeItemImpl result = patchItem.applyPatch(new DatasetProperties(), aspectRetriever);

    // Verify the result was processed by genericPatch method
    assertNotNull(result);
    assertEquals(
        "Test description", ((DatasetProperties) result.getRecordTemplate()).getDescription());
  }

  @Test
  public void testFallbackToTemplatePatchWhenGenericPatchConditionsNotMet() throws Exception {
    // Create a GenericJsonPatch with empty primary keys and force flag false
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/description");
    patchOp.setValue("Test description");

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(Map.of()) // Empty primary keys
            .forceGenericPatch(false) // Don't force generic patch
            .build();

    // Build PatchItemImpl with genericJsonPatch
    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(urn)
            .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .auditStamp(auditStamp)
            .genericJsonPatch(genericJsonPatch)
            .patch(genericJsonPatch.getJsonPatch())
            .build(entityRegistry);

    // Spies entity registry
    EntityRegistry mockRegistry = spy(entityRegistry);
    AspectTemplateEngine mockTemplateEngine = spy(entityRegistry.getAspectTemplateEngine());

    // Mock aspectRetriever to verify which method is called
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockRegistry);

    // Create a template engine that will return a default template
    when(mockRegistry.getAspectTemplateEngine()).thenReturn(mockTemplateEngine);

    // Apply the patch - should use template patch
    patchItem.applyPatch(new DatasetProperties(), mockAspectRetriever);

    // Verify the template engine was called (indicating template patch was used)
    verify(mockTemplateEngine).applyPatch(any(), any(), any());
  }
}
