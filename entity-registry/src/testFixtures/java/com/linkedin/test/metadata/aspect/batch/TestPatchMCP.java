package com.linkedin.test.metadata.aspect.batch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

/** Builds minimal patch items around a raw json-patch ops array, for validator tests. */
public class TestPatchMCP {

  private TestPatchMCP() {}

  public static PatchMCP of(Urn urn, String aspectName, String patchOpsJson) {
    JsonPatch patch;
    try (JsonReader reader = Json.createReader(new StringReader(patchOpsJson))) {
      patch = Json.createPatch(reader.readArray());
    }
    PatchMCP item = mock(PatchMCP.class);
    when(item.getPatch()).thenReturn(patch);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(ChangeType.PATCH);
    return item;
  }

  /**
   * A raw-proposal-shaped patch item (not a {@link PatchMCP}), as produced under alternate MCP
   * validation: the serialized patch lives in the MCP's aspect payload.
   */
  public static MCPItem ofProposed(Urn urn, String aspectName, String serializedPatchJson) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setAspect(
        new GenericAspect()
            .setValue(ByteString.copyString(serializedPatchJson, StandardCharsets.UTF_8))
            .setContentType("application/json-patch+json"));
    MCPItem item = mock(MCPItem.class);
    when(item.getMetadataChangeProposal()).thenReturn(mcp);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(ChangeType.PATCH);
    return item;
  }
}
