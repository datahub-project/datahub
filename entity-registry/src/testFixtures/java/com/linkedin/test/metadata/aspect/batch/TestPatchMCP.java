package com.linkedin.test.metadata.aspect.batch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import java.io.StringReader;

/** Builds a minimal {@link PatchMCP} around a raw json-patch ops array, for validator tests. */
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
}
