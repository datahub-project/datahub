package com.linkedin.metadata.aspect.patch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class PatchOperationUtilsTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testConvertToJsonPatchFromObjectPayload() throws Exception {
    GenericJsonPatch.PatchOp addOp = new GenericJsonPatch.PatchOp();
    addOp.setOp("add");
    addOp.setPath("/properties/urn:li:structuredProperty:foo/");
    addOp.setValue(Map.of("propertyUrn", "urn:li:structuredProperty:foo"));

    GenericJsonPatch patch =
        GenericJsonPatch.builder()
            .arrayPrimaryKeys(Map.of("properties", List.of("propertyUrn")))
            .patch(List.of(addOp))
            .build();

    Pair<JsonPatch, Optional<GenericJsonPatch>> parsed =
        PatchOperationUtils.convertToJsonPatch(OBJECT_MAPPER.writeValueAsString(patch));

    assertNotNull(parsed.getFirst());
    assertTrue(parsed.getSecond().isPresent());
    assertEquals(parsed.getSecond().get().getPatch().size(), 1);
    assertEquals(
        parsed.getSecond().get().getArrayPrimaryKeys().get("properties"), List.of("propertyUrn"));
  }

  @Test
  public void testConvertToJsonPatchFromBareArrayAlsoBuildsGeneric() throws Exception {
    String payload =
        "[{\"op\":\"add\",\"path\":\"/properties/x/\",\"value\":{\"propertyUrn\":\"urn:li:structuredProperty:x\"}}]";

    Pair<JsonPatch, Optional<GenericJsonPatch>> parsed =
        PatchOperationUtils.convertToJsonPatch(payload);

    assertNotNull(parsed.getFirst());
    assertTrue(parsed.getSecond().isPresent());
    assertEquals(parsed.getSecond().get().getPatch().get(0).getPath(), "/properties/x/");
  }

  @Test
  public void testConvertToJsonPatchBareArrayMoveKeepsJsonPatchWithoutGeneric() {
    // RFC-6902 move/copy carry "from", which GenericJsonPatch.PatchOp cannot deserialize. Prior
    // PatchItemImpl behavior accepted the bare array as JsonPatch with no GenericJsonPatch.
    String payload = "[{\"op\":\"move\",\"from\":\"/a\",\"path\":\"/b\"}]";

    Pair<JsonPatch, Optional<GenericJsonPatch>> parsed =
        PatchOperationUtils.convertToJsonPatch(payload);

    assertNotNull(parsed.getFirst());
    assertTrue(parsed.getSecond().isEmpty());
  }

  @Test
  public void testResolveGenericJsonPatchReusesPatchMcpGeneric() {
    GenericJsonPatch.PatchOp addOp = new GenericJsonPatch.PatchOp();
    addOp.setOp("add");
    addOp.setPath("/system");
    addOp.setValue(true);
    GenericJsonPatch existing = GenericJsonPatch.builder().patch(List.of(addOp)).build();

    com.linkedin.metadata.aspect.batch.PatchMCP patchItem =
        mock(com.linkedin.metadata.aspect.batch.PatchMCP.class);
    when(patchItem.getGenericJsonPatch()).thenReturn(existing);

    assertEquals(PatchOperationUtils.resolveGenericJsonPatch(patchItem), existing);
  }

  @Test
  public void testResolveGenericJsonPatchFromObjectPayload() throws Exception {
    GenericJsonPatch.PatchOp addOp = new GenericJsonPatch.PatchOp();
    addOp.setOp("add");
    addOp.setPath("/properties/urn:li:structuredProperty:foo/");
    addOp.setValue(Map.of("propertyUrn", "urn:li:structuredProperty:foo"));

    GenericJsonPatch patch =
        GenericJsonPatch.builder()
            .arrayPrimaryKeys(Map.of("properties", List.of("propertyUrn")))
            .patch(List.of(addOp))
            .build();

    GenericJsonPatch resolved =
        PatchOperationUtils.resolveGenericJsonPatch(
            mcpItem(OBJECT_MAPPER.writeValueAsString(patch)));

    assertNotNull(resolved);
    assertEquals(resolved.getPatch().size(), 1);
    assertEquals(resolved.getArrayPrimaryKeys().get("properties"), List.of("propertyUrn"));
  }

  @Test
  public void testResolveGenericJsonPatchFromBareArray() {
    String payload =
        "[{\"op\":\"add\",\"path\":\"/properties/x/\",\"value\":{\"propertyUrn\":\"urn:li:structuredProperty:x\"}}]";

    GenericJsonPatch resolved = PatchOperationUtils.resolveGenericJsonPatch(mcpItem(payload));

    assertNotNull(resolved);
    assertEquals(resolved.getPatch().get(0).getPath(), "/properties/x/");
  }

  @Test
  public void testResolveGenericJsonPatchNullWithoutAspect() {
    MCPItem item = mock(MCPItem.class);
    when(item.getMetadataChangeProposal()).thenReturn(null);
    assertNull(PatchOperationUtils.resolveGenericJsonPatch(item));
  }

  @Test
  public void testAddAndReplaceValuesFromProposedBareArray() {
    String payload =
        "[{\"op\":\"add\",\"path\":\"/status\",\"value\":\"ACTIVE\"},"
            + "{\"op\":\"remove\",\"path\":\"/old\"}]";

    List<Pair<String, JsonValue>> ops =
        PatchOperationUtils.addAndReplaceValues(mcpItem(payload)).collect(Collectors.toList());

    assertEquals(ops.size(), 1);
    assertEquals(ops.get(0).getFirst(), "/status");
  }

  @Test
  public void testNestValueAtObjectPath() {
    JsonValue nested =
        PatchOperationUtils.nestValueAtObjectPath("/resources/filter", Json.createValue("x"))
            .orElseThrow();
    assertEquals(nested.asJsonObject().getJsonObject("resources").getString("filter"), "x");
    assertTrue(
        PatchOperationUtils.nestValueAtObjectPath("/items/0", Json.createValue("x")).isEmpty());
  }

  private static MCPItem mcpItem(String payload) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"));
    mcp.setEntityType("dataset");
    mcp.setAspectName("structuredProperties");
    mcp.setChangeType(ChangeType.PATCH);
    mcp.setAspect(
        new GenericAspect()
            .setContentType("application/json")
            .setValue(ByteString.copyString(payload, StandardCharsets.UTF_8)));

    MCPItem item = mock(MCPItem.class);
    when(item.getMetadataChangeProposal()).thenReturn(mcp);
    when(item.getUrn()).thenReturn(mcp.getEntityUrn());
    when(item.getAspectName()).thenReturn(mcp.getAspectName());
    return item;
  }
}
