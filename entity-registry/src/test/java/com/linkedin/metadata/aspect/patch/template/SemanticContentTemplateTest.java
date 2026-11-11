package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.OBJECT_MAPPER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.aspect.patch.builder.SemanticProfilePatchBuilder;
import com.linkedin.metadata.aspect.patch.template.common.SemanticContentTemplate;
import com.linkedin.metadata.aspect.patch.template.common.SemanticProfileConflictException;
import com.linkedin.mxe.MetadataChangeProposal;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class SemanticContentTemplateTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:one");

  @Test
  public void testPatchOneProfileLeavesTheOther() throws Exception {
    SemanticContent original = contentWith("profileA", "profileB");
    SemanticContent patched =
        new SemanticContentTemplate()
            .applyPatch(original, patch("[{\"op\":\"remove\",\"path\":\"/embeddings/profileA\"}]"));

    assertFalse(patched.getEmbeddings().containsKey("profileA"));
    assertTrue(patched.getEmbeddings().containsKey("profileB"));
    assertEquals(patched.getEmbeddings().get("profileB").getModelVersion(), "model-profileB");
  }

  @Test
  public void testAddProfileLeavesTheOther() throws Exception {
    SemanticContent original = contentWith("profileA", "profileB");
    String add =
        "[{\"op\":\"add\",\"path\":\"/embeddings/profileC\",\"value\":{"
            + "\"modelVersion\":\"model-profileC\",\"generatedAt\":1,\"totalChunks\":0,\"chunks\":[]}}]";
    SemanticContent patched = new SemanticContentTemplate().applyPatch(original, patch(add));

    assertTrue(patched.getEmbeddings().containsKey("profileA"));
    assertTrue(patched.getEmbeddings().containsKey("profileB"));
    assertEquals(patched.getEmbeddings().get("profileC").getModelVersion(), "model-profileC");
  }

  @Test
  public void testFieldPatchLeavesTheOtherProfile() throws Exception {
    SemanticContent original = contentWith("profileA", "profileB");
    String setCurrent =
        "[{\"op\":\"add\",\"path\":\"/embeddings/profileA/current\",\"value\":false}]";
    SemanticContent patched = new SemanticContentTemplate().applyPatch(original, patch(setCurrent));

    assertFalse(patched.getEmbeddings().get("profileA").isCurrent());
    assertFalse(patched.getEmbeddings().get("profileB").hasCurrent());
    assertEquals(patched.getEmbeddings().get("profileB").getModelVersion(), "model-profileB");
  }

  @Test
  public void testCompleteSkipOnAnEmptyAspect() throws Exception {
    SemanticContent patched =
        new SemanticContentTemplate().applyPatch(contentWith(), patch(skip("skipped")));

    assertEquals(patched.getEmbeddings().size(), 1);
    assertEquals(
        patched.getEmbeddings().get("skipped").getVectorState(), SemanticVectorState.SKIPPED);
    assertEquals(patched.getEmbeddings().get("skipped").getModelVersion(), "model-skipped");
  }

  @Test
  public void testCompleteSkipLeavesTheSiblingProfile() throws Exception {
    SemanticContent patched =
        new SemanticContentTemplate().applyPatch(contentWith("profileB"), patch(skip("skipped")));

    assertEquals(patched.getEmbeddings().get("profileB").getModelVersion(), "model-profileB");
    assertEquals(
        patched.getEmbeddings().get("skipped").getVectorState(), SemanticVectorState.SKIPPED);
    assertEquals(patched.getEmbeddings().get("skipped").getModelVersion(), "model-skipped");
  }

  @Test
  public void testRefreshSourceOnAnAspectWithNoMarker() throws Exception {
    String add = "[{\"op\":\"add\",\"path\":\"/refreshMarker/sources/documentInfo\",\"value\":50}]";
    SemanticContent patched =
        new SemanticContentTemplate().applyPatch(contentWith("profileA"), patch(add));

    assertEquals(patched.getRefreshMarker().getSources().get("documentInfo"), Long.valueOf(50L));
    assertEquals(patched.getEmbeddings().get("profileA").getModelVersion(), "model-profileA");
  }

  @Test
  public void testAMatchingProfileExpectationAppliesAndKeepsTheSibling() throws Exception {
    SemanticContent original = contentWith("profileA", "profileB");
    String write =
        "[{\"op\":\"test\",\"path\":\"/embeddings/profileA/generatedAt\",\"value\":1},"
            + "{\"op\":\"add\",\"path\":\"/embeddings/profileA/current\",\"value\":true}]";
    SemanticContent patched = new SemanticContentTemplate().applyPatch(original, patch(write));

    assertTrue(patched.getEmbeddings().get("profileA").isCurrent());
    assertEquals(patched.getEmbeddings().get("profileB").getModelVersion(), "model-profileB");
  }

  @Test
  public void testAMismatchingProfileExpectationConflicts() {
    String write =
        "[{\"op\":\"test\",\"path\":\"/embeddings/profileA/modelVersion\",\"value\":\"other\"},"
            + "{\"op\":\"add\",\"path\":\"/embeddings/profileA/current\",\"value\":true}]";

    assertThrows(
        SemanticProfileConflictException.class,
        () -> new SemanticContentTemplate().applyPatch(contentWith("profileA"), patch(write)));
  }

  @Test
  public void testAnAbsentExpectationPassesOnlyForAMissingKey() throws Exception {
    MetadataChangeProposal write =
        new SemanticProfilePatchBuilder()
            .expectProfileAbsent("skipped")
            .urn(URN)
            .setProfile("skipped", OBJECT_MAPPER.readTree(skipValue("skipped")))
            .build();

    SemanticContent created =
        new SemanticContentTemplate().applyPatch(contentWith("profileB"), patch(write));
    assertEquals(
        created.getEmbeddings().get("skipped").getVectorState(), SemanticVectorState.SKIPPED);

    assertThrows(
        SemanticProfileConflictException.class,
        () -> new SemanticContentTemplate().applyPatch(contentWith("skipped"), patch(write)));
  }

  @Test
  public void testASourceExpectationConflictsAfterTheSourceMoved() throws Exception {
    SemanticContent read = contentWith("profileA");
    LongMap sources = new LongMap();
    sources.put("documentInfo", 10L);
    read.setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
    MetadataChangeProposal write =
        new SemanticProfilePatchBuilder()
            .expectBoundSources(read, List.of("documentInfo", "semanticText"))
            .urn(URN)
            .setProfileField("profileA", "current", BooleanNode.TRUE)
            .build();

    SemanticContent unchanged = new SemanticContentTemplate().applyPatch(read, patch(write));
    assertTrue(unchanged.getEmbeddings().get("profileA").isCurrent());

    SemanticContent moved = contentWith("profileA");
    LongMap newer = new LongMap();
    newer.put("documentInfo", 20L);
    moved.setRefreshMarker(new SemanticRefreshMarker().setSources(newer));
    assertThrows(
        SemanticProfileConflictException.class,
        () -> new SemanticContentTemplate().applyPatch(moved, patch(write)));
  }

  @Test
  public void testProfileStateExpectationsTreatAMissingFieldAsAbsent() throws Exception {
    SemanticContent read = contentWith("profileA");
    MetadataChangeProposal write =
        new SemanticProfilePatchBuilder()
            .expectProfileState(
                "profileA", read.getEmbeddings().get("profileA"), List.of("contentHash"))
            .urn(URN)
            .setProfileField("profileA", "current", BooleanNode.TRUE)
            .build();

    new SemanticContentTemplate().applyPatch(read, patch(write));

    SemanticContent hashed = contentWith("profileA");
    hashed.getEmbeddings().get("profileA").setContentHash("newer");
    assertThrows(
        SemanticProfileConflictException.class,
        () -> new SemanticContentTemplate().applyPatch(hashed, patch(write)));
  }

  private static JsonPatch patch(MetadataChangeProposal proposal) {
    return patch(new String(proposal.getAspect().getValue().copyBytes(), StandardCharsets.UTF_8));
  }

  private static String skipValue(String key) {
    return "{\"modelVersion\":\"model-"
        + key
        + "\",\"generatedAt\":1,\"totalChunks\":0,\"chunks\":[],"
        + "\"vectorState\":\"SKIPPED\",\"stateReason\":\"EMPTY_TEXT\"}";
  }

  private static String skip(String key) {
    return "[{\"op\":\"add\",\"path\":\"/embeddings/"
        + key
        + "\",\"value\":{\"modelVersion\":\"model-"
        + key
        + "\",\"generatedAt\":1,\"totalChunks\":0,\"chunks\":[],"
        + "\"vectorState\":\"SKIPPED\",\"stateReason\":\"EMPTY_TEXT\"}}]";
  }

  private static SemanticContent contentWith(String... keys) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    for (String key : keys) {
      embeddings.put(key, profile(key));
    }
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static EmbeddingModelData profile(String key) {
    return new EmbeddingModelData()
        .setModelVersion("model-" + key)
        .setGeneratedAt(1L)
        .setTotalChunks(0)
        .setChunks(new EmbeddingChunkArray());
  }

  private static JsonPatch patch(String json) {
    return Json.createPatch(Json.createReader(new StringReader(json)).readArray());
  }
}
