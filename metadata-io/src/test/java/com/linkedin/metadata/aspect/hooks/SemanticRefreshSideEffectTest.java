package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_TEXT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.template.common.SemanticContentTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SemanticRefreshSideEffectTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:one");
  private static final ContentBinding DOCUMENT_TEXT =
      new ContentBinding(
          Map.of(
              DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text"),
              SEMANTIC_TEXT_ASPECT_NAME, Set.of("text")));
  private static final ContentBinding METADATA =
      new ContentBinding(Map.of("ownership", Set.of("owners")));
  private static final String DOCUMENT_TEXT_KEY = DOCUMENT_INFO_ASPECT_NAME + "#contents.text";
  private static final String TITLE_KEY = DOCUMENT_INFO_ASPECT_NAME + "#title";
  private static final String SEMANTIC_TEXT_KEY = SEMANTIC_TEXT_ASPECT_NAME + "#text";

  @Test
  public void testAWatchedTextChangeMovesOnlyThatAspectsMarker() throws Exception {
    SemanticContent stored = content(10L);

    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT, "meta", METADATA))
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L),
                    source(SEMANTIC_TEXT_ASPECT_NAME, text("same"), text("same"), 90L)),
                stored(stored))
            .toList();

    assertEquals(patches.size(), 1);
    SemanticContent patched = applied(stored, patches.get(0));
    assertEquals(patched.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), Long.valueOf(80L));
    assertNull(patched.getRefreshMarker().getSources().get(SEMANTIC_TEXT_KEY));
    assertTrue(patched.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testAWatchedPathOfASharedAspectDoesNotMoveItsSiblingPathClock() throws Exception {
    ContentBinding body =
        new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text")));
    ContentBinding heading = new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("title")));

    SemanticContent patched =
        movedBy(Map.of("documentText", body, "meta", heading), fields("body", "edited"));

    assertEquals(patched.getRefreshMarker().getSources().get(TITLE_KEY), Long.valueOf(80L));
    assertNull(patched.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY));
  }

  @Test
  public void testAnUnwatchedFieldChangeEmitsNoPatch() {
    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT))
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    source(
                        DOCUMENT_INFO_ASPECT_NAME,
                        fields("body", "old"),
                        fields("body", "new"),
                        80L)),
                stored(content(10L)))
            .toList();

    assertTrue(patches.isEmpty());
  }

  @Test
  public void testEitherWatchedPathOfASharedAspectMovesItsOwnClock() throws Exception {
    ContentBinding both =
        new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text", "title")));
    ContentBinding other = new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("summary")));

    SemanticContent bodyEdit =
        movedBy(Map.of("both", both, "other", other), fields("edited", "heading"));
    assertEquals(
        bodyEdit.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), Long.valueOf(80L));
    assertNull(bodyEdit.getRefreshMarker().getSources().get(TITLE_KEY));

    SemanticContent titleEdit =
        movedBy(Map.of("both", both, "other", other), fields("body", "edited"));
    assertEquals(titleEdit.getRefreshMarker().getSources().get(TITLE_KEY), Long.valueOf(80L));
    assertNull(titleEdit.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY));
  }

  @Test
  public void testAnEntityWithoutProfilesEmitsNoPatch() {
    RetrieverContext context = stored(null);

    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT))
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L)),
                context)
            .toList();

    assertTrue(patches.isEmpty());
  }

  @Test
  public void testTheDerivedPatchDoesNotCarryTheSourceRunOrApplyMarker() {
    MCLItem source = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L);
    StringMap properties = new StringMap();
    properties.put(SEMANTIC_APPLY_PROPERTY, "true");
    when(source.getSystemMetadata())
        .thenReturn(new SystemMetadata().setRunId("source-run").setProperties(properties));

    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT))
            .postMCPSideEffect(OperationFingerprint.EMPTY, List.of(source), stored(content(10L)))
            .toList();

    MCPItem patch = patches.get(0);
    assertEquals(patch.getAspectName(), SEMANTIC_CONTENT_ASPECT_NAME);
    assertEquals(patch.getChangeType(), ChangeType.PATCH);
    assertNotEquals(patch.getSystemMetadata().getRunId(), "source-run");
    assertFalse(
        patch.getSystemMetadata().hasProperties()
            && patch.getSystemMetadata().getProperties().containsKey(SEMANTIC_APPLY_PROPERTY));
  }

  private static SemanticContent applied(SemanticContent stored, MCPItem patch) throws Exception {
    return new SemanticContentTemplate()
        .applyPatch(new SemanticContent(stored.data().copy()), ((PatchItemImpl) patch).getPatch());
  }

  /** Runs one documentInfo write from the default body and heading against stored markers. */
  private static SemanticContent movedBy(Map<String, ContentBinding> bindings, DataMap after)
      throws Exception {
    SemanticContent stored = content(10L);
    List<MCPItem> patches =
        sideEffect(bindings)
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(source(DOCUMENT_INFO_ASPECT_NAME, fields("body", "heading"), after, 80L)),
                stored(stored))
            .toList();
    assertEquals(patches.size(), 1);
    return applied(stored, patches.get(0));
  }

  private static SemanticRefreshSideEffect sideEffect(Map<String, ContentBinding> bindings) {
    return new SemanticRefreshSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .className(SemanticRefreshSideEffect.class.getName())
                .enabled(true)
                .supportedOperations(List.of("UPSERT"))
                .supportedEntityAspectNames(List.of())
                .build())
        .setRegistry(
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
                return bindings.getOrDefault(profileKey, ContentBinding.EXTERNAL);
              }
            });
  }

  private static RetrieverContext stored(SemanticContent content) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(TestOperationContexts.defaultEntityRegistry());
    when(retriever.getLatestAspectObject(any(), any(), eq(SEMANTIC_CONTENT_ASPECT_NAME)))
        .thenReturn(content == null ? null : new Aspect(content.data()));
    RetrieverContext context = mock(RetrieverContext.class);
    when(context.getAspectRetriever()).thenReturn(retriever);
    return context;
  }

  private static MCLItem source(String aspect, DataMap before, DataMap after, long time) {
    RecordTemplate previous = mock(RecordTemplate.class);
    RecordTemplate proposed = mock(RecordTemplate.class);
    when(previous.data()).thenReturn(before);
    when(proposed.data()).thenReturn(after);
    MCLItem item = mock(MCLItem.class);
    when(item.getAspectName()).thenReturn(aspect);
    when(item.getUrn()).thenReturn(URN);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getPreviousRecordTemplate()).thenReturn(previous);
    when(item.getRecordTemplate()).thenReturn(proposed);
    when(item.getAuditStamp())
        .thenReturn(
            new AuditStamp().setTime(time).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")));
    return item;
  }

  private static SemanticContent content(long documentInfo) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    for (String key : List.of("documentText", "meta", "both", "other")) {
      embeddings.put(
          key,
          new EmbeddingModelData()
              .setModelVersion("model")
              .setGeneratedAt(1L)
              .setTotalChunks(0)
              .setChunks(new EmbeddingChunkArray())
              .setCurrent(true));
    }
    LongMap sources = new LongMap();
    sources.put(DOCUMENT_INFO_ASPECT_NAME, documentInfo);
    return new SemanticContent()
        .setEmbeddings(embeddings)
        .setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
  }

  private static DataMap fields(String text, String title) {
    DataMap body = document(text);
    body.put("title", title);
    return body;
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
