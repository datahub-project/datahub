package com.linkedin.metadata.aspect.patch.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.linkedin.common.GlobalTags;
import com.linkedin.dataset.DatasetProperties;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.Map;
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

  @Test
  public void testObjectMapperAllowsPropertyNamesBeyondDefaultNameLimit() throws Exception {
    // Deeply-nested dbt struct field paths (column-level lineage) become JSON property names in
    // upstreamLineage patches that exceed Jackson's default maxNameLength (50000).
    // Template.applyPatch round-trips the patched JSON through TemplateUtil.OBJECT_MAPPER.readTree,
    // which previously failed with "StreamConstraintsException: Name length (N) exceeds the maximum
    // allowed (50000)". The mapper must now accept names beyond that default.
    String longName = "f".repeat(60000);
    String json = "{\"" + longName + "\":\"v\"}";

    JsonNode node = TemplateUtil.OBJECT_MAPPER.readTree(json);

    Assert.assertTrue(node.has(longName), "Property name beyond the default limit must parse");
  }

  @Test
  public void testUnknownPaths() {
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(op("add", "/customProperties/any~1key"))
                .add(op("remove", "/tags/0/context"))
                .add(op("add", "/description"))
                .add(op("add", "/noSuchField/x"))
                .add(op("add", "/description/tooDeep"))
                .build());

    Assert.assertEquals(
        TemplateUtil.unknownPaths(patch, new DatasetProperties().schema(), Map.of()),
        List.of("/tags/0/context", "/noSuchField/x", "/description/tooDeep"));
  }

  @Test
  public void testUnknownPathsWithKeyedArray() {
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(op("add", "/tags//urn:li:tag:foo/context"))
                .add(op("add", "/tags//urn:li:tag:foo/noSuchField"))
                .build());

    Assert.assertEquals(
        TemplateUtil.unknownPaths(
            patch,
            new GlobalTags().schema(),
            Map.of("tags", List.of("attribution\u241fsource", "tag"))),
        List.of("/tags//urn:li:tag:foo/noSuchField"));
  }

  private static jakarta.json.JsonObjectBuilder op(String op, String path) {
    return Json.createObjectBuilder().add("op", op).add("path", path).add("value", "x");
  }
}
