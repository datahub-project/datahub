package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlobalTagsPatchBuilderTest {

  private TestableGlobalTagsPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_TAG_URN = "urn:li:tag:Test/Tag";

  // Test helper class to expose protected method
  private static class TestableGlobalTagsPatchBuilder extends GlobalTagsPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableGlobalTagsPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);

    builder.addTag(tagUrn, "Test context");

    // First call build()
    builder.build();

    // Then verify we can still access pathValues and they're correct
    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    // Verify we can call build() again without issues
    builder.build();

    // And verify pathValues are still accessible and correct
    pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);
  }

  @Test
  public void testAddTagWithContext() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    String context = "Test context";
    builder.addTag(tagUrn, context);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/tags/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("tag").asText(), tagUrn.toString());
    assertEquals(operation.getRight().get("context").asText(), context);
  }

  @Test
  public void testAddTagWithoutContext() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.addTag(tagUrn, null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/tags/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("tag").asText(), tagUrn.toString());
    assertNull(operation.getRight().get("context"));
  }

  @Test
  public void testRemoveTag() throws Exception {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.removeTag(tagUrn);

    MetadataChangeProposal mcp = builder.build();
    JsonNode payload = parseAspect(mcp);
    JsonNode patch = payload.get("patch");
    assertNotNull(patch);
    assertEquals(patch.size(), 1);
    assertEquals(patch.get(0).get("op").asText(), "remove");
    assertTrue(patch.get(0).get("path").asText().startsWith("/tags/"));
  }

  @Test
  public void testMultipleAddOperations() throws URISyntaxException {
    TagUrn tagUrn1 = TagUrn.createFromString(TEST_TAG_URN);
    TagUrn tagUrn2 = TagUrn.createFromString("urn:li:tag:AnotherTag");

    builder.addTag(tagUrn1, "Context 1").addTag(tagUrn2, null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);
  }

  @Test
  public void testGetEntityTypeWithoutUrnThrowsException() {
    TestableGlobalTagsPatchBuilder builderWithoutUrn = new TestableGlobalTagsPatchBuilder();
    TagUrn tagUrn;
    try {
      tagUrn = TagUrn.createFromString(TEST_TAG_URN);
      builderWithoutUrn.addTag(tagUrn, null);

      assertThrows(IllegalStateException.class, builderWithoutUrn::build);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  // --- Parsson bug regression + APK ordering tests ---

  @Test
  public void testAddTagUnattributedShipsSourceFirstApkAndDoubleSlashPath() throws Exception {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.addTag(tagUrn, null);

    MetadataChangeProposal mcp = builder.build();
    JsonNode payload = parseAspect(mcp);

    // APK must be source-first to avoid the Parsson trailing-slash bug.
    JsonNode apkTags = payload.get("arrayPrimaryKeys").get("tags");
    assertEquals(apkTags.get(0).asText(), "attribution\u241fsource");
    assertEquals(apkTags.get(1).asText(), "tag");

    JsonNode patch = payload.get("patch");
    assertEquals(patch.size(), 1);
    String path = patch.get(0).get("path").asText();
    // Source-first layout: /tags//<encodedUrn> — empty source component in the middle,
    // not at the end. Parsson rejects an empty final path component on add.
    assertTrue(path.startsWith("/tags//"), "Expected source-first path, got: " + path);
    // URN slashes are encoded as ~1 in JSON Pointer paths
    String encodedUrn = tagUrn.toString().replace("~", "~0").replace("/", "~1");
    assertTrue(path.contains(encodedUrn), "Path must contain the encoded tag URN");
  }

  @Test
  public void testRemoveTagUnattributedShipsTagFirstApkAndSingleSegmentPath() throws Exception {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.removeTag(tagUrn);

    MetadataChangeProposal mcp = builder.build();
    JsonNode payload = parseAspect(mcp);

    // Tag-first APK for wildcard removal across all sources.
    JsonNode apkTags = payload.get("arrayPrimaryKeys").get("tags");
    assertEquals(apkTags.get(0).asText(), "tag");
    assertEquals(apkTags.get(1).asText(), "attribution\u241fsource");

    JsonNode patch = payload.get("patch");
    assertEquals(patch.size(), 1);
    String path = patch.get(0).get("path").asText();
    assertEquals(patch.get(0).get("op").asText(), "remove");
    // Single-segment path /tags/<encodedUrn> — URN slashes are encoded as ~1 in JSON Pointer.
    String encodedUrn = tagUrn.toString().replace("~", "~0").replace("/", "~1");
    assertEquals(path, "/tags/" + encodedUrn);
  }

  @Test
  public void testMixedAddRemoveBuildAllProducesTwoConsistentEnvelopes() throws Exception {
    TagUrn tagA = TagUrn.createFromString("urn:li:tag:TagA");
    TagUrn tagB = TagUrn.createFromString("urn:li:tag:TagB");
    builder.addTag(tagA, null).removeTag(tagB);

    List<MetadataChangeProposal> mcps = builder.buildAll();
    assertEquals(mcps.size(), 2);

    // First MCP: the add op, source-first APK.
    JsonNode addPayload = parseAspect(mcps.get(0));
    assertEquals(
        addPayload.get("arrayPrimaryKeys").get("tags").get(0).asText(),
        "attribution\u241fsource",
        "add MCP must use source-first APK");

    // Second MCP: the wildcard remove, tag-first APK.
    JsonNode removePayload = parseAspect(mcps.get(1));
    assertEquals(
        removePayload.get("arrayPrimaryKeys").get("tags").get(0).asText(),
        "tag",
        "remove MCP must use tag-first APK");
  }

  @Test
  public void testMixedAddRemoveBuildThrowsIllegalState() throws URISyntaxException {
    TagUrn tagA = TagUrn.createFromString("urn:li:tag:TagA");
    TagUrn tagB = TagUrn.createFromString("urn:li:tag:TagB");
    builder.addTag(tagA, null).removeTag(tagB);

    assertThrows(IllegalStateException.class, builder::build);
  }

  private static JsonNode parseAspect(MetadataChangeProposal mcp) throws Exception {
    byte[] bytes = mcp.getAspect().getValue().copyBytes();
    return new ObjectMapper().readTree(new String(bytes, StandardCharsets.UTF_8));
  }
}
