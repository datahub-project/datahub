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
import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.template.common.SemanticContentTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
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
    assertToken(patched.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), 80L, 1L);
    assertNull(patched.getRefreshMarker().getSources().get(SEMANTIC_TEXT_KEY));
    assertTrue(patched.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testARollbackUpsertMovesTheMarkerPastTheDiscardedVersion() throws Exception {
    SemanticContent stored = content(10L);
    stored.getRefreshMarker().getSources().put(DOCUMENT_TEXT_KEY, token(10L, 6L));

    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT))
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    rolledBackSource(
                        DOCUMENT_INFO_ASPECT_NAME, document("new"), document("old"), 10L, 6L, 7L)),
                stored(stored))
            .toList();

    assertEquals(patches.size(), 1);
    SemanticContent patched = applied(stored, patches.get(0));
    assertToken(patched.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), 10L, 7L);
  }

  @Test
  public void testAWatchedPathOfASharedAspectDoesNotMoveItsSiblingPathClock() throws Exception {
    ContentBinding body =
        new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("contents.text")));
    ContentBinding heading = new ContentBinding(Map.of(DOCUMENT_INFO_ASPECT_NAME, Set.of("title")));

    SemanticContent patched =
        movedBy(Map.of("documentText", body, "meta", heading), fields("body", "edited"));

    assertToken(patched.getRefreshMarker().getSources().get(TITLE_KEY), 80L, 1L);
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
    assertToken(bodyEdit.getRefreshMarker().getSources().get(DOCUMENT_TEXT_KEY), 80L, 1L);
    assertNull(bodyEdit.getRefreshMarker().getSources().get(TITLE_KEY));

    SemanticContent titleEdit =
        movedBy(Map.of("both", both, "other", other), fields("body", "edited"));
    assertToken(titleEdit.getRefreshMarker().getSources().get(TITLE_KEY), 80L, 1L);
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
  public void testAnEntityWithOnlyExternalProfilesEmitsNoPatch() {
    List<MCPItem> patches =
        sideEffect(Map.of())
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L)),
                stored(content(10L)))
            .toList();

    assertTrue(patches.isEmpty());
  }

  @Test
  public void testTheMarkerPatchNeverCreatesTheAspect() {
    List<MCPItem> patches =
        sideEffect(Map.of("documentText", DOCUMENT_TEXT))
            .postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L)),
                stored(content(10L)))
            .toList();

    assertTrue(patches.get(0).requiresExistingAspect());
  }

  @Test
  public void testTheTokenNamesTheSourceVersion() {
    MCLItem firstWrite = source(DOCUMENT_INFO_ASPECT_NAME, null, document("new"), 80L);
    when(firstWrite.getSystemMetadata())
        .thenReturn(new SystemMetadata().setVersion("1").setAspectCreated(stamp(30L)));
    assertEquals(SemanticRefreshSideEffect.sourceToken(firstWrite, stamp(80L)), token(30L, 1L));

    MCLItem update = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 80L);
    when(update.getSystemMetadata()).thenReturn(new SystemMetadata().setVersion("5"));
    when(update.getPreviousSystemMetadata())
        .thenReturn(new SystemMetadata().setVersion("4").setAspectCreated(stamp(20L)));
    assertEquals(SemanticRefreshSideEffect.sourceToken(update, stamp(80L)), token(20L, 5L));

    MCLItem delete = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), null, 90L);
    when(delete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(delete.getPreviousSystemMetadata())
        .thenReturn(new SystemMetadata().setVersion("5").setAspectCreated(stamp(20L)));
    assertEquals(SemanticRefreshSideEffect.sourceToken(delete, stamp(90L)), token(20L, 6L));

    MCLItem legacyDelete = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), null, 90L);
    when(legacyDelete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(legacyDelete.getPreviousSystemMetadata()).thenReturn(new SystemMetadata().setVersion("5"));
    assertEquals(SemanticRefreshSideEffect.sourceToken(legacyDelete, stamp(90L)), token(90L, 6L));
  }

  @Test
  public void testALegacyRowUsesItsCreatedOnEvenWhenTheAuditTimeIsEarlier() {
    long createdOn = 50L;
    EbeanSystemAspect row =
        EbeanSystemAspect.builder()
            .forUpdate(
                new EbeanAspectV2(
                    "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                    "status",
                    0L,
                    RecordUtils.toJsonString(new Status().setRemoved(false)),
                    new Timestamp(createdOn),
                    "urn:li:corpuser:tester",
                    null,
                    RecordUtils.toJsonString(new SystemMetadata().setVersion("4"))),
                TestOperationContexts.defaultEntityRegistry());
    assertEquals(row.getSystemMetadata().getAspectCreated().getTime(), Long.valueOf(createdOn));
    SystemAspect previous = row.getDatabaseAspect().orElseThrow();
    assertEquals(
        previous.getSystemMetadata().getAspectCreated().getTime(), Long.valueOf(createdOn));

    MCLItem update = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), document("new"), 10L);
    when(update.getSystemMetadata()).thenReturn(new SystemMetadata().setVersion("5"));
    when(update.getPreviousSystemMetadata()).thenReturn(previous.getSystemMetadata());
    assertEquals(SemanticRefreshSideEffect.sourceToken(update, stamp(10L)), token(createdOn, 5L));

    MCLItem delete = source(DOCUMENT_INFO_ASPECT_NAME, document("old"), null, 10L);
    when(delete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(delete.getPreviousSystemMetadata()).thenReturn(previous.getSystemMetadata());
    assertEquals(SemanticRefreshSideEffect.sourceToken(delete, stamp(10L)), token(createdOn, 5L));
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
              public boolean isConfigured(String entityType, String profileKey) {
                return true;
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
    RecordTemplate previous = before == null ? null : mock(RecordTemplate.class);
    RecordTemplate proposed = after == null ? null : mock(RecordTemplate.class);
    if (previous != null) {
      when(previous.data()).thenReturn(before);
    }
    if (proposed != null) {
      when(proposed.data()).thenReturn(after);
    }
    MCLItem item = mock(MCLItem.class);
    when(item.getAspectName()).thenReturn(aspect);
    when(item.getUrn()).thenReturn(URN);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getPreviousRecordTemplate()).thenReturn(previous);
    when(item.getRecordTemplate()).thenReturn(proposed);
    when(item.getAuditStamp()).thenReturn(stamp(time));
    return item;
  }

  private static MCLItem rolledBackSource(
      String aspect,
      DataMap before,
      DataMap after,
      long createdTime,
      long previousVersion,
      long version) {
    MCLItem item = source(aspect, before, after, createdTime);
    when(item.getSystemMetadata()).thenReturn(systemMetadata(createdTime, version));
    when(item.getPreviousSystemMetadata()).thenReturn(systemMetadata(createdTime, previousVersion));
    return item;
  }

  private static SystemMetadata systemMetadata(long createdTime, long version) {
    return new SystemMetadata()
        .setVersion(String.valueOf(version))
        .setAspectCreated(stamp(createdTime));
  }

  private static AuditStamp stamp(long time) {
    return new AuditStamp().setTime(time).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
  }

  private static void assertToken(SemanticSourceToken actual, long createdTime, long version) {
    assertEquals(actual.getCreatedTime(), Long.valueOf(createdTime));
    assertEquals(actual.getVersion(), Long.valueOf(version));
  }

  private static SemanticSourceToken token(long createdTime, long version) {
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version);
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
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(DOCUMENT_INFO_ASPECT_NAME, token(documentInfo, 1L));
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
