package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_TEXT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertEquals(live.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), Long.valueOf(10L));
    assertEquals(live.getRefreshMarker().getSources().get(SEMANTIC_TEXT_KEY), Long.valueOf(999L));
  }

  @Test
  public void testAFullWriteWithoutAMarkerKeepsTheStoredMarker() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = content(10L, 40L, true);
    live.removeRefreshMarker();

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertEquals(live.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), Long.valueOf(10L));
  }

  @Test
  public void testAMetadataMarkerMoveClearsOnlyTheProfilesBoundToIt() {
    SemanticContent previous = content(10L, 40L, true);
    SemanticContent live = moved(content(10L, 40L, true), OWNERS_KEY, 80L);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertFalse(live.getEmbeddings().get("meta").isCurrent());
    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testATextMarkerMoveHashesTheStoredDocument() {
    for (String storedText : List.of("body", "edited")) {
      SemanticContent previous = written(SemanticDocumentProvenance.sha256Hex("body"), true);
      SemanticContent live =
          moved(
              written(SemanticDocumentProvenance.sha256Hex("body"), true), DOCUMENT_TEXT_KEY, 80L);

      hook()
          .writeMutation(
              OperationFingerprint.EMPTY,
              List.of(semantic(live, previous)),
              retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document(storedText))))
          .toList();

      assertEquals(
          live.getEmbeddings().get("documentText").isCurrent().booleanValue(),
          "body".equals(storedText),
          storedText);
    }
  }

  @Test
  public void testAContentOnlyWriteMarksAMatchingProfileCurrent() {
    SemanticContent live = written(SemanticDocumentProvenance.sha256Hex("body"), false);

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(live, new SemanticContent())),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAnApplyWriteKeepsTheCurrentItDecided() {
    SemanticContent live = written(SemanticDocumentProvenance.sha256Hex("body"), false);
    ChangeMCP item = semantic(live, new SemanticContent());
    StringMap properties = new StringMap();
    properties.put(SEMANTIC_APPLY_PROPERTY, "true");
    when(item.getSystemMetadata()).thenReturn(new SystemMetadata().setProperties(properties));

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(item),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testTheSemanticTextOverrideIsHashedBeforeTheBody() {
    SemanticContent live = written(SemanticDocumentProvenance.sha256Hex("override"), false);

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(live, new SemanticContent())),
            retriever(
                Map.of(
                    DOCUMENT_INFO_ASPECT_NAME,
                    document("body"),
                    SEMANTIC_TEXT_ASPECT_NAME,
                    text("override"))))
        .toList();

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAMixedBindingStaysNotCurrentWhenOwnershipMovesWithAMatchingTextHash() {
    String hash = SemanticDocumentProvenance.sha256Hex("body");
    SemanticContent previous = mixed(hash);
    SemanticContent live = moved(mixed(hash), OWNERS_KEY, 80L);

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(live, previous)),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertFalse(live.getEmbeddings().get("mixed").isCurrent());
  }

  @Test
  public void testAMixedBindingBecomesCurrentWhenOnlyItsTextMovesToAMatchingHash() {
    String hash = SemanticDocumentProvenance.sha256Hex("body");
    SemanticContent previous = mixed(hash);
    SemanticContent live = moved(mixed(hash), DOCUMENT_TEXT_KEY, 80L);

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(live, previous)),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertTrue(live.getEmbeddings().get("mixed").isCurrent());
  }

  @Test
  public void testATitleEditLeavesTheBodyProfileCurrent() {
    SemanticContent previous = scopedProfiles(10L, 10L);
    SemanticContent live = moved(scopedProfiles(10L, 10L), TITLE_KEY, 80L);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
    assertFalse(live.getEmbeddings().get("title").isCurrent());
  }

  @Test
  public void testALegacyTitleProfileIsNotCurrentWhenTheTitleMoves() {
    SemanticContent previous = titled(true);
    SemanticContent live = moved(titled(true), TITLE_KEY, 80L);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertFalse(live.getEmbeddings().get("title").isCurrent());
  }

  @Test
  public void testAProfileMarkerOlderThanAMovedEntityMarkerClearsCurrent() {
    String hash = SemanticDocumentProvenance.sha256Hex("body");
    SemanticContent previous = entityClock(clocked(hash, 10L, true), 10L);
    SemanticContent live = entityClock(clocked(hash, 10L, true), 80L);
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
  public void testAProfileMarkerThatCoversAMovedEntityMarkerStaysCurrent() {
    String hash = SemanticDocumentProvenance.sha256Hex("stale");
    SemanticContent previous = entityClock(clocked(hash, 80L, true), 10L);
    SemanticContent live = entityClock(clocked(hash, 80L, true), 80L);
    AspectRetriever aspects = mock(AspectRetriever.class);
    RetrieverContext context = mock(RetrieverContext.class);
    when(context.getAspectRetriever()).thenReturn(aspects);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), context)
        .toList();

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
    verify(aspects, never()).getLatestAspectObject(any(), any(), any());
  }

  @Test
  public void testAProfileMarkerThatFallsBehindTheEntityMarkerClearsCurrent() {
    SemanticContent previous =
        entityClock(clocked(SemanticDocumentProvenance.sha256Hex("body"), 80L, true), 80L);
    SemanticContent live =
        entityClock(clocked(SemanticDocumentProvenance.sha256Hex("body"), 10L, true), 80L);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAProfileMarkerThatCatchesTheEntityMarkerSetsCurrent() {
    SemanticContent previous =
        entityClock(clocked(SemanticDocumentProvenance.sha256Hex("body"), 10L, false), 80L);
    SemanticContent live =
        entityClock(clocked(SemanticDocumentProvenance.sha256Hex("body"), 80L, false), 80L);

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

    assertTrue(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testALegacyProfileWithNoDigestFollowsWhetherTheTextIsBlank() {
    SemanticContent blankPrevious = writtenWithoutDigest(true);
    SemanticContent blankLive = writtenWithoutDigest(true);
    blankLive.getEmbeddings().get("documentText").setContentHash("rewritten");

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(blankLive, blankPrevious)),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("  "))))
        .toList();

    assertTrue(blankLive.getEmbeddings().get("documentText").isCurrent());

    SemanticContent textPrevious = writtenWithoutDigest(true);
    SemanticContent textLive = writtenWithoutDigest(true);
    textLive.getEmbeddings().get("documentText").setContentHash("rewritten");

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(textLive, textPrevious)),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertFalse(textLive.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testALegacyProfileWithAMismatchedHashIsNotCurrent() {
    SemanticContent previous = written(SemanticDocumentProvenance.sha256Hex("old"), true);
    SemanticContent live = written(SemanticDocumentProvenance.sha256Hex("old"), true);
    live.getEmbeddings().get("documentText").setContentHash("rewritten");

    hook()
        .writeMutation(
            OperationFingerprint.EMPTY,
            List.of(semantic(live, previous)),
            retriever(Map.of(DOCUMENT_INFO_ASPECT_NAME, document("body"))))
        .toList();

    assertFalse(live.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAnExternalProfileKeepsTheCurrentItWasWrittenWith() {
    SemanticContent previous = content(10L, 40L, true);
    previous.getEmbeddings().put("vendor", profile(true));
    SemanticContent live = moved(content(10L, 40L, true), OWNERS_KEY, 80L);
    live.getEmbeddings().put("vendor", profile(true).setContentHash("rewritten"));

    hook()
        .writeMutation(OperationFingerprint.EMPTY, List.of(semantic(live, previous)), retriever())
        .toList();

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

  private static SemanticContent moved(SemanticContent content, String watchKey, long time) {
    LongMap sources = new LongMap();
    if (content.hasRefreshMarker()) {
      sources.putAll(content.getRefreshMarker().getSources());
    }
    sources.put(watchKey, time);
    return content.setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  private static SemanticContent mixed(String sourceTextSha256) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("mixed", profile(false).setSourceTextSha256(sourceTextSha256));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticContent entityClock(SemanticContent content, long documentInfo) {
    return content.setRefreshMarker(new SemanticRefreshMarker().setSources(sources(documentInfo)));
  }

  private static SemanticContent clocked(
      String sourceTextSha256, long represented, boolean current) {
    EmbeddingModelData profile =
        profile(current)
            .setSourceTextSha256(sourceTextSha256)
            .setRefreshMarker(new SemanticRefreshMarker().setSources(sources(represented)));
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("documentText", profile);
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static LongMap sources(long time) {
    LongMap sources = new LongMap();
    sources.put(DOCUMENT_TEXT_KEY, time);
    return sources;
  }

  /** A body profile and a title profile, each with its own clock, under one entity marker. */
  private static SemanticContent scopedProfiles(long bodyClock, long titleClock) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(
        "documentText", profile(true).setRefreshMarker(clock(DOCUMENT_TEXT_KEY, bodyClock)));
    embeddings.put("title", profile(true).setRefreshMarker(clock(TITLE_KEY, titleClock)));
    LongMap sources = new LongMap();
    sources.put(DOCUMENT_TEXT_KEY, bodyClock);
    sources.put(TITLE_KEY, titleClock);
    return new SemanticContent()
        .setEmbeddings(embeddings)
        .setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  private static SemanticRefreshMarker clock(String watchKey, long time) {
    LongMap sources = new LongMap();
    sources.put(watchKey, time);
    return new SemanticRefreshMarker().setSources(sources);
  }

  private static SemanticContent titled(boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("title", profile(current));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticContent writtenWithoutDigest(boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("documentText", profile(current));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticContent written(String sourceTextSha256, boolean current) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put("documentText", profile(current).setSourceTextSha256(sourceTextSha256));
    return new SemanticContent().setEmbeddings(embeddings);
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
    return retriever(Map.of());
  }

  private static RetrieverContext retriever(Map<String, DataMap> stored) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObject(any(), any(), any())).thenReturn(null);
    stored.forEach(
        (name, data) ->
            when(retriever.getLatestAspectObject(any(), any(), eq(name)))
                .thenReturn(new Aspect(data)));
    RetrieverContext context = mock(RetrieverContext.class);
    when(context.getAspectRetriever()).thenReturn(retriever);
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
    LongMap sources = new LongMap();
    sources.put(DOCUMENT_TEXT_KEY, documentInfo);
    sources.put(SEMANTIC_TEXT_KEY, semanticText);
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

  private static DataMap document(String body) {
    DataMap contents = new DataMap();
    contents.put("text", body);
    DataMap document = new DataMap();
    document.put("contents", contents);
    return document;
  }

  private static DataMap text(String value) {
    DataMap map = new DataMap();
    map.put("text", value);
    return map;
  }
}
