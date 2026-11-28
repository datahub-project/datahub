package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_TEXT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class SemanticRefreshMarkerHookTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:one");
  private static final ContentBinding DOCUMENT_TEXT =
      new ContentBinding(
          Map.of(
              DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text"),
              SEMANTIC_TEXT_ASPECT_NAME, Set.of("text")));
  private static final ContentBinding METADATA =
      new ContentBinding(Map.of("ownership", Set.of("owners")));
  private static final ContentBinding MIXED =
      new ContentBinding(
          Map.of(
              DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text"), "ownership", Set.of("owners")));
  private static final ContentBinding TITLE =
      new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("title")));
  private static final String DOCUMENT_TEXT_KEY = DOCUMENT_INFO_ASPECT_NAME + "#contents.text";
  private static final String SEMANTIC_TEXT_KEY = SEMANTIC_TEXT_ASPECT_NAME + "#text";
  private static final String TITLE_KEY = DOCUMENT_INFO_ASPECT_NAME + "#title";
  private static final String OWNERS_KEY = "ownership#owners";

  @Test
  public void testAFullWriteCannotMoveTheMarkerBackward() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = content(5L, 999L, true);

    run(live, previous);

    assertEquals(live.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), token(10L));
    assertEquals(live.getRefreshMarker().getSources().get(SEMANTIC_TEXT_KEY), token(999L));
  }

  @Test
  public void testALaterVersionOfTheSameAspectMovesTheMarker() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = content(10L, 40L, true);
    live.getRefreshMarker()
        .getSources()
        .put(DOCUMENT_TEXT_KEY, new SemanticSourceToken().setCreatedTime(10L).setVersion(2L));

    run(live, previous);

    assertEquals(
        live.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY).getVersion(), Long.valueOf(2L));
    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAFullWriteWithoutAMarkerKeepsTheStoredMarker() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = content(10L, 40L, true);
    live.removeRefreshMarker();

    run(live, previous);

    assertEquals(live.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), token(10L));
  }

  @Test
  public void testAMetadataMarkerMoveClearsOnlyTheProfilesBoundToIt() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = moved(content(10L, 40L, true), OWNERS_KEY, 80L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("meta").isCurrent());
    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAMovedTextKeyClearsCurrentWithoutReadingTheSource() {
    SemanticContent previous = legacy("documentText", true);
    SemanticContent live = moved(legacy("documentText", true), DOCUMENT_TEXT_KEY, 80L);
    AspectRetriever aspects = mock(AspectRetriever.class);
    RetrieverContext context = mock(RetrieverContext.class);
    when(context.getAspectRetriever()).thenReturn(aspects);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), context)
        .toList();

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
    verify(aspects, never()).getLatestAspectObject(any(), any(), any());
  }

  @Test
  public void testAPayloadWriteWithNoMovedSourceIsCurrent() {
    SemanticContent previous = legacy("documentText", false);
    SemanticContent live = legacy("documentText", false);
    live.getEmbeddings().get("documentText").setContentHash("rewritten");

    run(live, previous);

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAnApplyWriteKeepsTheCurrentItDecided() {
    SemanticContent live = legacy("documentText", false);
    ChangeMCP item = semantic(live, new SemanticContent());
    StringMap properties = new StringMap();
    properties.put(SEMANTIC_APPLY_PROPERTY, "true");
    when(item.getSystemMetadata()).thenReturn(new SystemMetadata().setProperties(properties));

    hook().writeMutation(OperationFingerprint.EMPTY, List.of(item), retriever()).toList();

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAMixedBindingIsNotCurrentWhenOwnershipMoves() {
    SemanticContent previous = legacy("mixed", true);
    SemanticContent live = moved(legacy("mixed", true), OWNERS_KEY, 80L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("mixed").isCurrent());
  }

  @Test
  public void testATitleEditLeavesTheBodyProfileCurrent() {
    SemanticContent previous = scopedProfiles(10L, 10L);
    SemanticContent live = moved(scopedProfiles(10L, 10L), TITLE_KEY, 80L);

    run(live, previous);

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
    assertFalse(live.getEmbeddings().get("title").isCurrent());
  }

  @Test
  public void testAProfileMarkerOlderThanAMovedEntityMarkerClearsCurrent() {
    SemanticContent previous = entityClock(clocked(10L, true), 10L);
    SemanticContent live = entityClock(clocked(10L, true), 80L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAProfileMarkerThatCoversAMovedEntityMarkerStaysCurrent() {
    SemanticContent previous = entityClock(clocked(80L, true), 10L);
    SemanticContent live = entityClock(clocked(80L, true), 80L);

    run(live, previous);

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAProfileMarkerThatFallsBehindTheEntityMarkerClearsCurrent() {
    SemanticContent previous = entityClock(clocked(80L, true), 80L);
    SemanticContent live = entityClock(clocked(10L, true), 80L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAProfileMarkerThatCatchesTheEntityMarkerSetsCurrent() {
    SemanticContent previous = entityClock(clocked(10L, false), 80L);
    SemanticContent live = entityClock(clocked(80L, false), 80L);

    run(live, previous);

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testABodyProfileCarryingADeletedOverrideTokenIsCurrent() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = deletedOverride(content(10L, 40L, true));
    live.getEmbeddings().get("documentText").setRefreshMarker(bodyMarker(10L, deletionToken(40L)));

    run(live, previous);

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testABodyProfileMissingDeletedOverrideTokenIsNotCurrent() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = deletedOverride(content(10L, 40L, true));
    live.getEmbeddings().get("documentText").setRefreshMarker(bodyMarker(10L, null));

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAnExternalProfileKeepsTheCurrentItWasWrittenWith() {
    SemanticContent previous = content(10L, 40L, true);
    previous.getEmbeddings().put("vendor", profile(true));
    SemanticContent live = moved(content(10L, 40L, true), OWNERS_KEY, 80L);
    live.getEmbeddings().put("vendor", profile(true).setContentHash("rewritten"));

    run(live, previous);

    assertTrue(live.getEmbeddings().get("vendor").isCurrent());
  }

  @Test
  public void testAnUnchangedProfileIsNotReportedAsMutated() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = content(10L, 40L, true);

    List<Pair<ChangeMCP, Boolean>> results =
        hook()
            .writeMutation(
                OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
            .toList();

    assertFalse(results.get(0).getSecond());
    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testACurrentOnlyRewriteKeepsTheStoredValue() {
    SemanticContent previous = legacy("documentText", false);
    SemanticContent live = legacy("documentText", true);

    List<Pair<ChangeMCP, Boolean>> results =
        hook()
            .writeMutation(
                OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
            .toList();

    assertTrue(results.get(0).getSecond());
    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testACurrentOnlyRewriteClearsCurrentWhenTheStoredProfileHadNone() {
    SemanticContent previous = legacy("documentText", false);
    previous.getEmbeddings().get("documentText").removeCurrent();
    SemanticContent live = legacy("documentText", true);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").hasCurrent());
  }

  @Test
  public void testAWatchedCurrentOnlyWriteKeepsTheStoredValue() {
    SemanticContent previous = entityClock(clocked(10L, false), 10L);
    SemanticContent live = entityClock(clocked(10L, true), 10L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAWatchedCurrentOnlyWriteWithoutAStoredValueLeavesCurrentUnset() {
    SemanticContent previous = entityClock(clocked(10L, false), 10L);
    previous.getEmbeddings().get("documentText").removeCurrent();
    SemanticContent live = entityClock(clocked(10L, true), 10L);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").hasCurrent());
  }

  @Test
  public void testAWatchedPayloadChangeRecomputesFromItsStaleMarker() {
    SemanticContent previous = entityClock(clocked(10L, true), 80L);
    SemanticContent live = entityClock(clocked(10L, true), 80L);
    live.getEmbeddings().get("documentText").setContentHash("rewritten");

    run(live, previous);

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAnExternalCurrentOnlyWriteIsStoredAsWritten() {
    SemanticContent previous = externalVendor(false);
    SemanticContent live = externalVendor(true);

    run(live, previous);

    assertTrue(live.getEmbeddings().get("vendor").isCurrent());
  }

  @Test
  public void testAnExternalCurrentOnlyWriteToFalseIsStoredAsWritten() {
    SemanticContent previous = externalVendor(true);
    SemanticContent live = externalVendor(false);

    run(live, previous);

    assertFalse(live.getEmbeddings().get("vendor").isCurrent());
  }

  @Test
  public void testAProfileRewrittenWithTheSamePayloadStampsKeepsItsStoredCurrent() {
    SemanticContent previous = content(10L, 40L, false);
    SemanticContent live = content(10L, 40L, true);
    // Same stamps and identity; only the vector values differ. Vectors are not compared, so this
    // is not a new payload and the stored current stands.
    withVector(previous.getEmbeddings().get("documentText"), 0.25f);
    withVector(live.getEmbeddings().get("documentText"), 0.5f);

    List<Pair<ChangeMCP, Boolean>> results =
        hook()
            .writeMutation(
                OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
            .toList();

    // The hook puts back the stored current, which changes the live aspect.
    assertTrue(results.get(0).getSecond());
    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAProfileWithAMovedGeneratedAtIsRecomputedAsCurrent() {
    SemanticContent previous = content(10L, 40L, false);
    SemanticContent live = content(10L, 40L, false);
    live.getEmbeddings().get("documentText").setGeneratedAt(2L);

    List<Pair<ChangeMCP, Boolean>> results =
        hook()
            .writeMutation(
                OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
            .toList();

    assertTrue(results.get(0).getSecond());
    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  private static void withVector(EmbeddingModelData profile, float value) {
    EmbeddingChunk chunk =
        new EmbeddingChunk().setPosition(0).setVector(new FloatArray(List.of(value)));
    profile.setChunks(new EmbeddingChunkArray(List.of(chunk)));
  }

  private static SemanticContent externalVendor(boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("vendor", profile(current));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static void run(SemanticContent live, SemanticContent previous) {
    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();
  }

  private static SemanticSourceToken token(long createdTime) {
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(1L);
  }

  private static SemanticContent moved(SemanticContent content, String watchKey, long time) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    if (content.hasRefreshMarker()) {
      sources.putAll(content.getRefreshMarker().getSources());
    }
    sources.put(watchKey, token(time));
    return content.setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  /** Entity marker after the semanticText override was deleted: its token moves to version 2. */
  private static SemanticContent deletedOverride(SemanticContent content) {
    content.getRefreshMarker().getSources().put(SEMANTIC_TEXT_KEY, deletionToken(40L));
    return content;
  }

  private static SemanticSourceToken deletionToken(long createdTime) {
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(2L);
  }

  /** A body profile's own marker: the document text token, and the override token if given. */
  private static SemanticRefreshMarker bodyMarker(
      long documentInfo, @Nullable SemanticSourceToken semanticText) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(DOCUMENT_TEXT_KEY, token(documentInfo));
    if (semanticText != null) {
      sources.put(SEMANTIC_TEXT_KEY, semanticText);
    }
    return new SemanticRefreshMarker().setSources(sources);
  }

  private static SemanticContent legacy(String key, boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(key, profile(current));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticContent entityClock(SemanticContent content, long documentInfo) {
    return content.setRefreshMarker(new SemanticRefreshMarker().setSources(sources(documentInfo)));
  }

  private static SemanticContent clocked(long represented, boolean current) {
    EmbeddingModelData profile =
        profile(current)
            .setRefreshMarker(new SemanticRefreshMarker().setSources(sources(represented)));
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("documentText", profile);
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticSourceTokenMap sources(long time) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(DOCUMENT_TEXT_KEY, token(time));
    return sources;
  }

  /** A body profile and a title profile, each with its own clock, under one entity marker. */
  private static SemanticContent scopedProfiles(long bodyClock, long titleClock) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(
        "documentText", profile(true).setRefreshMarker(clock(DOCUMENT_TEXT_KEY, bodyClock)));
    embeddings.put("title", profile(true).setRefreshMarker(clock(TITLE_KEY, titleClock)));
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(DOCUMENT_TEXT_KEY, token(bodyClock));
    sources.put(TITLE_KEY, token(titleClock));
    return new SemanticContent()
        .setEmbeddings(embeddings)
        .setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  private static SemanticRefreshMarker clock(String watchKey, long time) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(watchKey, token(time));
    return new SemanticRefreshMarker().setSources(sources);
  }

  private static SemanticRefreshMarkerHook hook() {
    SemanticRefreshMarkerHook hook = new SemanticRefreshMarkerHook();
    hook.setConfig(
        AspectPluginConfig.builder()
            .className(SemanticRefreshMarkerHook.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(List.of())
            .build());
    hook.setRegistry(
        new SemanticProfileRegistry() {
          @Override
          public boolean generationEnabled() {
            return false;
          }

          @Override
          public boolean isServiceProfile(String entityType, String profileKey) {
            return false;
          }

          @Override
          public boolean isConfigured(String entityType, String profileKey) {
            return true;
          }

          @Override
          public ContentBinding bindingFor(String entityType, String profileKey) {
            return switch (profileKey) {
              case "meta" -> METADATA;
              case "mixed" -> MIXED;
              case "title" -> TITLE;
              case "vendor" -> ContentBinding.EXTERNAL;
              default -> DOCUMENT_TEXT;
            };
          }
        });
    return hook;
  }

  private static RetrieverContext retriever() {
    RetrieverContext context = mock(RetrieverContext.class);
    when(context.getAspectRetriever()).thenReturn(mock(AspectRetriever.class));
    return context;
  }

  private static ChangeMCP semantic(SemanticContent live, SemanticContent previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn(SEMANTIC_CONTENT_ASPECT_NAME);
    when(item.getUrn()).thenReturn(URN);
    when(item.getRecordTemplate()).thenReturn(live);
    when(item.getAspect(SemanticContent.class)).thenReturn(live);
    when(item.getPreviousAspect(SemanticContent.class)).thenReturn(previous);
    return item;
  }

  private static SemanticContent content(long documentInfo, long semanticText, boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("documentText", profile(current));
    embeddings.put("meta", profile(current));
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(DOCUMENT_TEXT_KEY, token(documentInfo));
    sources.put(SEMANTIC_TEXT_KEY, token(semanticText));
    return new SemanticContent()
        .setEmbeddings(embeddings)
        .setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  private static EmbeddingModelData profile(boolean current) {
    return new EmbeddingModelData()
        .setModelVersion("model")
        .setGeneratedAt(1L)
        .setTotalChunks(0)
        .setChunks(new EmbeddingChunkArray())
        .setCurrent(current);
  }
}
