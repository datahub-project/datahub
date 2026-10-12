package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import java.util.List;
import org.testng.annotations.Test;

public class InstitutionalMemoryPatchBuilderTest {

  private static final String URL = "https://example.org/a";
  private static final String ENCODED_URL = "https:~1~1example.org~1a";

  @Test
  public void testAddLinkEncodesSlashAndSetsArrayPrimaryKeys() {
    InstitutionalMemoryPatchBuilder builder = new InstitutionalMemoryPatchBuilder();
    builder
        .urn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)"))
        .addLink(URL, "design/doc", UrnUtils.getUrn("urn:li:corpuser:datahub"));

    GenericJsonPatch patch = builder.getJsonPatch();
    assertEquals(patch.getArrayPrimaryKeys().get("elements"), List.of("url", "description"));

    GenericJsonPatch.PatchOp op = patch.getPatch().get(0);
    assertEquals(op.getOp(), "add");
    assertEquals(op.getPath(), "/elements/" + ENCODED_URL + "/design~1doc");

    JsonObject value = ((JsonValue) op.getValue()).asJsonObject();
    assertEquals(value.getString("url"), URL);
    assertEquals(value.getString("description"), "design/doc");
    assertNotNull(value.getJsonObject("createStamp"));
    assertEquals(value.getJsonObject("createStamp").getString("actor"), "urn:li:corpuser:datahub");
    assertNotNull(value.getJsonObject("createStamp").get("time"));
  }

  @Test
  public void testAddLinkEncodesTildeBeforeSlash() {
    InstitutionalMemoryPatchBuilder builder = new InstitutionalMemoryPatchBuilder();
    builder.addLink(
        "https://example.org/~user/a", "note~ 1", UrnUtils.getUrn("urn:li:corpuser:datahub"));

    GenericJsonPatch.PatchOp op = builder.getJsonPatch().getPatch().get(0);
    assertEquals(op.getPath(), "/elements/https:~1~1example.org~1~0user~1a/note~0 1");

    JsonObject value = ((JsonValue) op.getValue()).asJsonObject();
    assertEquals(value.getString("url"), "https://example.org/~user/a");
    assertEquals(value.getString("description"), "note~ 1");
  }

  @Test
  public void testRemoveLinkPaths() {
    InstitutionalMemoryPatchBuilder pair = new InstitutionalMemoryPatchBuilder();
    pair.removeLink(URL, "design");
    GenericJsonPatch.PatchOp pairOp = pair.getJsonPatch().getPatch().get(0);
    assertEquals(pairOp.getOp(), "remove");
    assertEquals(pairOp.getPath(), "/elements/" + ENCODED_URL + "/design");
    assertNull(pairOp.getValue());

    InstitutionalMemoryPatchBuilder urlOnly = new InstitutionalMemoryPatchBuilder();
    urlOnly.removeLink(URL);
    GenericJsonPatch.PatchOp urlOp = urlOnly.getJsonPatch().getPatch().get(0);
    assertEquals(urlOp.getOp(), "remove");
    assertEquals(urlOp.getPath(), "/elements/" + ENCODED_URL);
    assertNull(urlOp.getValue());
  }
}
