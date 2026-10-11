package com.linkedin.metadata.timeline.data;

import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import org.testng.annotations.Test;

public class ChangeTransactionSerializationTest {

  // A populated rawDiff (raw=true) previously threw "No serializer found for
  // org.eclipse.parsson.JsonPatchImpl" and failed the whole /timeline response. It must now
  // serialize as its RFC-6902 JSON array.
  @Test
  public void testRawDiffSerializesAsJsonArray() throws Exception {
    JsonPatch patch = Json.createPatchBuilder().add("/foo", "bar").build();
    ChangeTransaction txn = ChangeTransaction.builder().timestamp(1L).rawDiff(patch).build();

    String json = new ObjectMapper().writeValueAsString(txn);

    assertTrue(json.contains("\"rawDiff\":["), json);
    assertTrue(json.contains("\"op\":\"add\""), json);
  }
}
