package com.linkedin.metadata.aspect.patch.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TemplateUtilTest {

  @Test
  public void testPopulateTopLevelKeysScaffoldsTrailingEmptyTokenForAddOp() {
    // Paths ending in "/" (e.g. /tags/<urn>/) need the parent to expose the empty key
    // before JsonPatch apply — otherwise Parsson throws.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:foo/")
                        .add("value", Json.createObjectBuilder().add("tag", "urn:li:tag:foo")))
                .build());

    JsonNode result =
        TemplateUtil.populateTopLevelKeys(
            JsonNodeFactory.instance
                .objectNode()
                .set("tags", JsonNodeFactory.instance.objectNode()),
            patch);

    JsonNode entryNode = result.get("tags").get("urn:li:tag:foo");
    Assert.assertNotNull(entryNode);
    Assert.assertTrue(entryNode.has(""));
  }

  @Test
  public void testPopulateTopLevelKeysAddOpUnchangedForNonEmptyTrailingToken() {
    // Non-empty trailing token: target key stays uncreated, JsonPatch will create it.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:foo")
                        .add("value", Json.createObjectBuilder().add("tag", "urn:li:tag:foo")))
                .build());

    JsonNode result =
        TemplateUtil.populateTopLevelKeys(
            JsonNodeFactory.instance
                .objectNode()
                .set("tags", JsonNodeFactory.instance.objectNode()),
            patch);

    Assert.assertFalse(result.get("tags").has("urn:li:tag:foo"));
  }
}
