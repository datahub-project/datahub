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
    // Regression for paths ending in "/" (e.g. /tags/<urn>/), produced by every unattributed
    // patch builder. Parsson rejects the apply unless the parent already exposes the empty key.
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
    Assert.assertNotNull(entryNode, "entity-key node must be scaffolded");
    Assert.assertTrue(entryNode.has(""), "trailing empty source key must also be scaffolded");
  }

  @Test
  public void testPopulateTopLevelKeysAddOpUnchangedForNonEmptyTrailingToken() {
    // The fix must not change behavior when the trailing token is non-empty — the target key
    // itself stays uncreated (JsonPatch will create it).
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

    Assert.assertFalse(
        result.get("tags").has("urn:li:tag:foo"),
        "non-empty trailing token must still be left to JsonPatch to create");
  }
}
