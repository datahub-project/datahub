package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class CatchUpBatchRebuilderTest {

  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,catchup.d1,PROD)");
  private static final String KEY = "datasetKey";
  private static final String PROPERTIES = "datasetProperties";

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
  private final CatchUpBatchRebuilder rebuilder = new CatchUpBatchRebuilder();

  @Test
  public void testBatchesWithoutTheCatchUpMarkAreReturnedAsTheyAre() {
    AspectRetriever retriever = retrieverHolding(Map.of());
    OperationContext opContext = context(retriever);
    Map<Urn, List<MCLItem>> live =
        Map.of(DATASET, List.of(event(PROPERTIES, new DatasetProperties().setName("live"), null)));

    assertSame(rebuilder.rebuild(opContext, live), live);
    verify(retriever, never()).getLatestSystemAspects(any(), any());
  }

  @Test
  public void testCatchUpBatchIsReplacedByTheEntitysStoredAspects() {
    DatasetProperties storedProperties = new DatasetProperties().setName("stored");
    AspectRetriever retriever =
        retrieverHolding(Map.of(KEY, stored(datasetKey()), PROPERTIES, stored(storedProperties)));
    OperationContext opContext = context(retriever);
    MCLItem replayed =
        event(PROPERTIES, new DatasetProperties().setName("stale"), BackfillHeaders.MODE_CATCH_UP);

    Map<Urn, List<MCLItem>> rebuilt =
        rebuilder.rebuild(opContext, Map.of(DATASET, List.of(replayed)));

    List<MCLItem> events = rebuilt.get(DATASET);
    assertEquals(
        events.stream().map(MCLItem::getAspectName).collect(Collectors.toList()),
        List.of(KEY, PROPERTIES));
    for (MCLItem event : events) {
      assertEquals(event.getChangeType(), ChangeType.RESTATE);
      assertEquals(
          event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.MODE_HEADER_NAME),
          BackfillHeaders.MODE_CATCH_UP,
          "the replay mark travels with the rebuilt events");
    }
    assertEquals(events.get(1).getRecordTemplate().data(), storedProperties.data());
    // Every aspect that can be in the document is asked for: the entity's non-timeseries ones.
    Set<String> requested =
        registry.getEntitySpec("dataset").getAspectSpecs().stream()
            .filter(spec -> !spec.isTimeseries())
            .map(AspectSpec::getName)
            .collect(Collectors.toSet());
    verify(retriever).getLatestSystemAspects(any(), eq(Map.of(DATASET, requested)));
  }

  @Test
  public void testEachEntityTypeIsReadOnItsOwn() {
    // The retriever resolves one entity type per call from the first URN it is given; a batch that
    // mixes types must not have its other types looked up under the wrong name and taken for gone.
    Urn container = UrnUtils.getUrn("urn:li:container:catchup-c1");
    EntitySpec containerSpec = registry.getEntitySpec("container");
    ContainerProperties containerProperties = new ContainerProperties().setName("stored");
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(registry);
    when(retriever.getLatestSystemAspects(any(), any()))
        .thenAnswer(
            invocation -> {
              Map<Urn, Set<String>> read = invocation.getArgument(1);
              assertEquals(
                  read.keySet().stream().map(Urn::getEntityType).collect(Collectors.toSet()).size(),
                  1,
                  "one entity type per read");
              return read.containsKey(DATASET)
                  ? Map.of(
                      DATASET,
                      Map.of(
                          KEY,
                          stored(datasetKey()),
                          PROPERTIES,
                          stored(new DatasetProperties().setName("stored"))))
                  : Map.of(
                      container,
                      Map.of(
                          "containerKey",
                          stored(
                              EntityKeyUtils.convertUrnToEntityKey(
                                  container, containerSpec.getKeyAspectSpec())),
                          "containerProperties",
                          stored(containerProperties)));
            });
    OperationContext opContext = context(retriever);
    MCLItem datasetRow =
        event(PROPERTIES, new DatasetProperties().setName("stale"), BackfillHeaders.MODE_CATCH_UP);
    MCLItem containerRow = mock(MCLItem.class);
    when(containerRow.getUrn()).thenReturn(container);
    when(containerRow.getEntitySpec()).thenReturn(containerSpec);
    when(containerRow.getAspectName()).thenReturn("containerProperties");
    when(containerRow.getAuditStamp()).thenReturn(auditStamp());
    when(containerRow.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("replay"));
    when(containerRow.getMetadataChangeLog())
        .thenReturn(
            new MetadataChangeLog()
                .setHeaders(
                    new StringMap(
                        Map.of(BackfillHeaders.MODE_HEADER_NAME, BackfillHeaders.MODE_CATCH_UP))));

    Map<Urn, List<MCLItem>> rebuilt =
        rebuilder.rebuild(
            opContext, Map.of(DATASET, List.of(datasetRow), container, List.of(containerRow)));

    verify(retriever, times(2)).getLatestSystemAspects(any(), any());
    for (Urn urn : List.of(DATASET, container)) {
      assertEquals(
          rebuilt.get(urn).stream().map(MCLItem::getChangeType).collect(Collectors.toSet()),
          Set.of(ChangeType.RESTATE),
          urn + " is rebuilt, not deleted");
    }
    assertEquals(
        rebuilt.get(container).get(1).getRecordTemplate().data(), containerProperties.data());
  }

  @Test
  public void testLiveEventAheadOfTheReplayedOneStillYieldsAMarkedRebuild() {
    // The consumer can group a live write and a replayed row of the same entity into one batch,
    // live event first; the rebuild still happens and still carries the replay mark.
    DatasetProperties storedProperties = new DatasetProperties().setName("stored");
    AspectRetriever retriever =
        retrieverHolding(Map.of(KEY, stored(datasetKey()), PROPERTIES, stored(storedProperties)));
    OperationContext opContext = context(retriever);
    MCLItem live = event(PROPERTIES, new DatasetProperties().setName("live"), null);
    MCLItem replayed =
        event(PROPERTIES, new DatasetProperties().setName("stale"), BackfillHeaders.MODE_CATCH_UP);

    Map<Urn, List<MCLItem>> rebuilt =
        rebuilder.rebuild(opContext, Map.of(DATASET, List.of(live, replayed)));

    List<MCLItem> events = rebuilt.get(DATASET);
    assertEquals(events.size(), 2);
    for (MCLItem event : events) {
      assertEquals(
          event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.MODE_HEADER_NAME),
          BackfillHeaders.MODE_CATCH_UP);
    }
    assertEquals(events.get(1).getRecordTemplate().data(), storedProperties.data());
  }

  @Test
  public void testEntityGoneFromStorageBecomesAKeyAspectDelete() {
    OperationContext opContext = context(retrieverHolding(Map.of()));
    MCLItem replayed =
        event(PROPERTIES, new DatasetProperties().setName("stale"), BackfillHeaders.MODE_CATCH_UP);

    Map<Urn, List<MCLItem>> rebuilt =
        rebuilder.rebuild(opContext, Map.of(DATASET, List.of(replayed)));

    List<MCLItem> events = rebuilt.get(DATASET);
    assertEquals(events.size(), 1);
    assertEquals(events.get(0).getChangeType(), ChangeType.DELETE);
    assertEquals(events.get(0).getAspectName(), KEY);
    assertEquals(
        events.get(0).getMetadataChangeLog().getHeaders().get(BackfillHeaders.MODE_HEADER_NAME),
        BackfillHeaders.MODE_CATCH_UP);
  }

  @Test
  public void testStoredStateIsReadFromPrimaryStorage() {
    AspectRetriever retriever =
        retrieverHolding(
            Map.of(KEY, stored(datasetKey()), PROPERTIES, stored(new DatasetProperties())));
    OperationContext replicaReads =
        OperationContext.withReadPreference(context(retriever), ReadPreference.READ);
    MCLItem replayed = event(PROPERTIES, new DatasetProperties(), BackfillHeaders.MODE_CATCH_UP);

    rebuilder.rebuild(replicaReads, Map.of(DATASET, List.of(replayed)));

    ArgumentCaptor<OperationContext> readWith = ArgumentCaptor.forClass(OperationContext.class);
    verify(retriever).getLatestSystemAspects(readWith.capture(), any());
    assertEquals(
        readWith.getValue().getPrimaryStorageContext().getReadPreference(), ReadPreference.PRIMARY);
  }

  private OperationContext context(AspectRetriever retriever) {
    return TestOperationContexts.systemContextNoSearchAuthorization(retriever);
  }

  private AspectRetriever retrieverHolding(Map<String, SystemAspect> storedAspects) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(registry);
    when(retriever.getLatestSystemAspects(any(), any()))
        .thenReturn(storedAspects.isEmpty() ? Map.of() : Map.of(DATASET, storedAspects));
    return retriever;
  }

  private RecordTemplate datasetKey() {
    return EntityKeyUtils.convertUrnToEntityKey(
        DATASET, registry.getEntitySpec("dataset").getKeyAspectSpec());
  }

  private MCLItem event(String aspectName, RecordTemplate record, String backfillMode) {
    EntitySpec datasetSpec = registry.getEntitySpec("dataset");
    MCLItem event = mock(MCLItem.class);
    when(event.getUrn()).thenReturn(DATASET);
    when(event.getEntitySpec()).thenReturn(datasetSpec);
    when(event.getAspectSpec()).thenReturn(datasetSpec.getAspectSpec(aspectName));
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getRecordTemplate()).thenReturn(record);
    when(event.getAuditStamp()).thenReturn(auditStamp());
    when(event.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("replay"));
    MetadataChangeLog changeLog = new MetadataChangeLog();
    if (backfillMode != null) {
      changeLog.setHeaders(new StringMap(Map.of(BackfillHeaders.MODE_HEADER_NAME, backfillMode)));
    }
    when(event.getMetadataChangeLog()).thenReturn(changeLog);
    return event;
  }

  private static SystemAspect stored(RecordTemplate record) {
    SystemAspect stored = mock(SystemAspect.class);
    when(stored.getRecordTemplate()).thenReturn(record);
    when(stored.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("stored"));
    when(stored.getAuditStamp()).thenReturn(auditStamp());
    return stored;
  }

  private static AuditStamp auditStamp() {
    return new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
  }
}
