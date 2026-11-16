package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class LifecycleVersionSideEffectTest {

  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(test-term,dataset)");
  // V2 has higher sortId — the "effective latest"
  private static final Urn ENTITY_V2_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,term-v2,PROD)");
  // V1 has lower sortId — the sibling
  private static final Urn ENTITY_V1_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,term-v1,PROD)");

  private static final Urn HIDDEN_STAGE_URN =
      UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT"); // hideInSearch=true
  private static final Urn VISIBLE_STAGE_URN =
      UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"); // hideInSearch=false

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(LifecycleVersionSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "UPSERT", "RESTATE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(ALL)
                      .aspectName(STATUS_ASPECT_NAME)
                      .build()))
          .build();

  // ---------------------------------------------------------------------------
  // No-op guard cases
  // ---------------------------------------------------------------------------

  @Test
  public void testNonStatusAspect_noOp() {
    MockAspectRetriever aspectRetriever = buildAspectRetriever(v2IsLatest(), v1NotLatest());
    RetrieverContext ctx = buildContext(aspectRetriever, mock(SearchRetriever.class));

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
            .recordTemplate(
                new VersionProperties()
                    .setVersionSet(VERSION_SET_URN)
                    .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
                    .setVersion(new VersionTag().setVersionTag("v2"))
                    .setSortId("bbb"))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 0, "Non-status aspect must produce no side effects");
  }

  @Test
  public void testStatusChangeOnNonVersionedEntity_noOp() {
    Urn nonVersionedUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,unversioned,PROD)");
    // No versionProperties for this URN
    MockAspectRetriever aspectRetriever = buildAspectRetriever();
    RetrieverContext ctx = buildContext(aspectRetriever, mock(SearchRetriever.class));

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(nonVersionedUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status().setLifecycleStage(HIDDEN_STAGE_URN))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 0, "Non-versioned entity must produce no side effects");
  }

  @Test
  public void testNoLifecycleStageChange_noOp() {
    MockAspectRetriever aspectRetriever = buildAspectRetriever(v2IsLatest(), v1NotLatest());
    RetrieverContext ctx = buildContext(aspectRetriever, mock(SearchRetriever.class));

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            // Both prev and new have the same hidden stage
            .recordTemplate(new Status().setLifecycleStage(HIDDEN_STAGE_URN))
            .previousSystemAspect(
                buildPreviousAspect(
                    ENTITY_V2_URN, entitySpec, new Status().setLifecycleStage(HIDDEN_STAGE_URN)))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 0, "Unchanged lifecycle stage must produce no side effects");
  }

  @Test
  public void testNonLatestGoesHidden_noOp() {
    // ENTITY_V1 (isLatest=false) goes hidden — should not trigger anything
    MockAspectRetriever aspectRetriever = buildAspectRetriever(v2IsLatest(), v1NotLatest());
    RetrieverContext ctx = buildContext(aspectRetriever, mock(SearchRetriever.class));

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V1_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status().setLifecycleStage(HIDDEN_STAGE_URN))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 0, "Non-latest going hidden must produce no side effects");
  }

  // ---------------------------------------------------------------------------
  // Latest goes hidden
  // ---------------------------------------------------------------------------

  @Test
  public void testLatestGoesHidden_promotesNextVisibleSibling() {
    // ENTITY_V2 (isLatest=true) enters hidden stage; ENTITY_V1 is visible (no status aspect)
    // Expect: demote V2, promote V1, update VersionSetProperties
    MockAspectRetriever aspectRetriever = buildAspectRetriever(v2IsLatest(), v1NotLatest());
    SearchRetriever searchRetriever = stubSiblings(List.of(ENTITY_V2_URN, ENTITY_V1_URN));
    RetrieverContext ctx = buildContext(aspectRetriever, searchRetriever);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status().setLifecycleStage(HIDDEN_STAGE_URN))
            // No previous status → previous lifecycleStage is null (visible)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 3, "Expected demote V2 + promote V1 + VSP update");

    MCPItem demote = results.get(0);
    assertEquals(demote.getUrn(), ENTITY_V2_URN);
    assertEquals(demote.getAspectName(), VERSION_PROPERTIES_ASPECT_NAME);
    assertFalse(demote.getAspect(VersionProperties.class).isIsLatest());

    MCPItem promote = results.get(1);
    assertEquals(promote.getUrn(), ENTITY_V1_URN);
    assertEquals(promote.getAspectName(), VERSION_PROPERTIES_ASPECT_NAME);
    assertTrue(promote.getAspect(VersionProperties.class).isIsLatest());

    MCPItem vspUpdate = results.get(2);
    assertEquals(vspUpdate.getUrn(), VERSION_SET_URN);
    assertEquals(vspUpdate.getAspectName(), VERSION_SET_PROPERTIES_ASPECT_NAME);
    assertEquals(vspUpdate.getAspect(VersionSetProperties.class).getLatest(), ENTITY_V1_URN);
  }

  @Test
  public void testLatestGoesHidden_noVisibleSibling_onlyDemotes() {
    // ENTITY_V1 is also in a hidden stage → no visible sibling → only demote V2
    MockAspectRetriever aspectRetriever =
        buildAspectRetriever(
            v2IsLatest(),
            v1NotLatest(),
            // ENTITY_V1 status: hidden
            Map.entry(ENTITY_V1_URN, new Status().setLifecycleStage(HIDDEN_STAGE_URN)));

    SearchRetriever searchRetriever = stubSiblings(List.of(ENTITY_V2_URN, ENTITY_V1_URN));
    RetrieverContext ctx = buildContext(aspectRetriever, searchRetriever);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status().setLifecycleStage(HIDDEN_STAGE_URN))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 1, "Only demotion when all siblings are hidden");
    assertEquals(results.get(0).getUrn(), ENTITY_V2_URN);
    assertFalse(results.get(0).getAspect(VersionProperties.class).isIsLatest());
  }

  // ---------------------------------------------------------------------------
  // Returning from hidden
  // ---------------------------------------------------------------------------

  @Test
  public void testReturnsFromHidden_isBestVisible_noCurrentLatest_onlyPromotes() {
    // ENTITY_V2 (isLatest=false, was in DRAFT) returns to visible
    // ENTITY_V1 is also hidden → ENTITY_V2 is best visible, and no current effective-latest exists
    VersionProperties v2Hidden =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v2"))
            .setSortId("bbb")
            .setIsLatest(false);
    VersionProperties v1Hidden =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
            .setSortId("aaa")
            .setIsLatest(false);

    MockAspectRetriever aspectRetriever =
        buildAspectRetriever(
            Map.entry(ENTITY_V2_URN, v2Hidden),
            Map.entry(ENTITY_V1_URN, v1Hidden),
            Map.entry(ENTITY_V1_URN, new Status().setLifecycleStage(HIDDEN_STAGE_URN)));

    SearchRetriever searchRetriever = stubSiblings(List.of(ENTITY_V2_URN, ENTITY_V1_URN));
    RetrieverContext ctx = buildContext(aspectRetriever, searchRetriever);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status()) // clearing stage → visible
            .previousSystemAspect(
                buildPreviousAspect(
                    ENTITY_V2_URN, entitySpec, new Status().setLifecycleStage(HIDDEN_STAGE_URN)))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(
        results.size(), 2, "Promotion plus set-latest update when no current effective-latest");
    assertEquals(results.get(0).getUrn(), ENTITY_V2_URN);
    assertTrue(results.get(0).getAspect(VersionProperties.class).isIsLatest());
    assertVersionSetLatest(results.get(1), ENTITY_V2_URN);
  }

  @Test
  public void testReturnsFromHidden_isBestVisible_demotesCurrentLatest() {
    // ENTITY_V2 (isLatest=false) returns from DRAFT; ENTITY_V1 (isLatest=true, lower sortId)
    // is the current effective-latest → demote V1, promote V2
    VersionProperties v2Hidden =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v2"))
            .setSortId("bbb")
            .setIsLatest(false);
    VersionProperties v1AsLatest =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
            .setSortId("aaa")
            .setIsLatest(true);

    MockAspectRetriever aspectRetriever =
        buildAspectRetriever(
            Map.entry(ENTITY_V2_URN, v2Hidden), Map.entry(ENTITY_V1_URN, v1AsLatest));

    SearchRetriever searchRetriever = stubSiblings(List.of(ENTITY_V2_URN, ENTITY_V1_URN));
    RetrieverContext ctx = buildContext(aspectRetriever, searchRetriever);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V2_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status()) // returning to no stage → visible
            .previousSystemAspect(
                buildPreviousAspect(
                    ENTITY_V2_URN, entitySpec, new Status().setLifecycleStage(HIDDEN_STAGE_URN)))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 3, "Expected demote V1 + promote V2 + set-latest update");

    MCPItem demote = results.get(0);
    assertEquals(demote.getUrn(), ENTITY_V1_URN);
    assertFalse(demote.getAspect(VersionProperties.class).isIsLatest());

    MCPItem promote = results.get(1);
    assertEquals(promote.getUrn(), ENTITY_V2_URN);
    assertTrue(promote.getAspect(VersionProperties.class).isIsLatest());

    // The set must follow the published version, or the next version is created against a
    // stale latest.
    assertVersionSetLatest(results.get(2), ENTITY_V2_URN);
  }

  private static void assertVersionSetLatest(MCPItem item, Urn expectedLatest) {
    assertEquals(item.getUrn(), VERSION_SET_URN);
    assertEquals(item.getAspectName(), VERSION_SET_PROPERTIES_ASPECT_NAME);
    assertEquals(item.getAspect(VersionSetProperties.class).getLatest(), expectedLatest);
  }

  @Test
  public void testReturnsFromHidden_notBestVisible_noOp() {
    // ENTITY_V1 (lower sortId) returns from hidden, but ENTITY_V2 is visible with higher sortId
    // → V1 is not the best visible sibling, so nothing should happen
    VersionProperties v2Visible =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v2"))
            .setSortId("bbb")
            .setIsLatest(true);
    VersionProperties v1Hidden =
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
            .setSortId("aaa")
            .setIsLatest(false);

    MockAspectRetriever aspectRetriever =
        buildAspectRetriever(
            Map.entry(ENTITY_V2_URN, v2Visible), Map.entry(ENTITY_V1_URN, v1Hidden));

    SearchRetriever searchRetriever = stubSiblings(List.of(ENTITY_V2_URN, ENTITY_V1_URN));
    RetrieverContext ctx = buildContext(aspectRetriever, searchRetriever);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_V1_URN)
            .aspectName(STATUS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
            .recordTemplate(new Status()) // returning to visible
            .previousSystemAspect(
                buildPreviousAspect(
                    ENTITY_V1_URN, entitySpec, new Status().setLifecycleStage(HIDDEN_STAGE_URN)))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(aspectRetriever);

    List<ChangeMCP> results = runSideEffect(changeItem, ctx);

    assertEquals(results.size(), 0, "V1 is not the best visible sibling — expect no-op");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Builds a VersionProperties for ENTITY_V2 with isLatest=true. */
  private Map.Entry<Urn, VersionProperties> v2IsLatest() {
    return Map.entry(
        ENTITY_V2_URN,
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v2"))
            .setSortId("bbb")
            .setIsLatest(true));
  }

  /** Builds a VersionProperties for ENTITY_V1 with isLatest=false. */
  private Map.Entry<Urn, VersionProperties> v1NotLatest() {
    return Map.entry(
        ENTITY_V1_URN,
        new VersionProperties()
            .setVersionSet(VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
            .setSortId("aaa")
            .setIsLatest(false));
  }

  /**
   * Builds a MockAspectRetriever pre-loaded with: - The DRAFT (hidden) and PUBLISHED (visible)
   * stage type info - Any additional URN→RecordTemplate entries passed as entries
   */
  @SafeVarargs
  private MockAspectRetriever buildAspectRetriever(
      Map.Entry<Urn, ? extends RecordTemplate>... entries) {
    LifecycleStageTypeInfo hiddenInfo =
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setSettings(new LifecycleStageSettings().setHideInSearch(true))
            .setCreated(AuditStampUtils.createDefaultAuditStamp())
            .setLastModified(AuditStampUtils.createDefaultAuditStamp());

    LifecycleStageTypeInfo visibleInfo =
        new LifecycleStageTypeInfo()
            .setName("Published")
            .setSettings(new LifecycleStageSettings().setHideInSearch(false))
            .setCreated(AuditStampUtils.createDefaultAuditStamp())
            .setLastModified(AuditStampUtils.createDefaultAuditStamp());

    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(HIDDEN_STAGE_URN, List.of(hiddenInfo));
    data.put(VISIBLE_STAGE_URN, List.of(visibleInfo));

    for (Map.Entry<Urn, ? extends RecordTemplate> entry : entries) {
      data.computeIfAbsent(entry.getKey(), k -> new java.util.ArrayList<>()).add(entry.getValue());
    }

    MockAspectRetriever retriever = new MockAspectRetriever(data);
    retriever.setEntityRegistry(TEST_REGISTRY);
    return retriever;
  }

  private RetrieverContext buildContext(
      MockAspectRetriever aspectRetriever, SearchRetriever searchRetriever) {
    return RetrieverContext.builder()
        .searchRetriever(searchRetriever)
        .aspectRetriever(aspectRetriever)
        .graphRetriever(mock(GraphRetriever.class))
        .build();
  }

  private SearchRetriever stubSiblings(List<Urn> siblingUrns) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : siblingUrns) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(entities);
    scrollResult.setNumEntities(entities.size());

    SearchRetriever retriever = mock(SearchRetriever.class);
    when(retriever.scroll(anyList(), any(Filter.class), any(), anyInt(), anyList(), any()))
        .thenReturn(scrollResult);
    return retriever;
  }

  private TestSystemAspect buildPreviousAspect(Urn urn, EntitySpec entitySpec, Status status) {
    return TestSystemAspect.builder()
        .urn(urn)
        .version(0)
        .recordTemplate(status)
        .entitySpec(entitySpec)
        .aspectSpec(entitySpec.getAspectSpec(STATUS_ASPECT_NAME))
        .systemMetadata(new SystemMetadata().setVersion("1"))
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build();
  }

  private List<ChangeMCP> runSideEffect(ChangeItemImpl changeItem, RetrieverContext ctx) {
    LifecycleVersionSideEffect sideEffect = new LifecycleVersionSideEffect();
    sideEffect.setConfig(TEST_PLUGIN_CONFIG);
    return sideEffect
        .applyMCPSideEffect(
            mock(OperationFingerprint.class), Collections.singletonList(changeItem), ctx)
        .collect(Collectors.toList());
  }
}
