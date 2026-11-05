package com.linkedin.metadata.search.siblings;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SiblingPrimaryNameIndexerTest {

  private static final Urn PRIMARY =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:glue,db.daily_kpi,PROD)");
  private static final Urn MEMBER =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:glue,db.hourly_kpi,PROD)");

  private ElasticSearchService search;
  private AspectRetriever retriever;
  private OperationContext opContext;
  private final Map<Urn, Map<String, Aspect>> store = new HashMap<>();

  @BeforeMethod
  public void setup() {
    store.clear();
    search = mock(ElasticSearchService.class);
    retriever = mock(AspectRetriever.class);
    opContext = mock(OperationContext.class, RETURNS_DEEP_STUBS);
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    when(opContext.getSearchContext().getIndexConvention().getEntityDocumentId(any()))
        .thenAnswer(invocation -> invocation.getArgument(0).toString());
    when(retriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> names = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> found = new HashMap<>();
                store.getOrDefault(urn, Map.of()).entrySet().stream()
                    .filter(e -> names.contains(e.getKey()))
                    .forEach(e -> found.put(e.getKey(), e.getValue()));
                result.put(urn, found);
              }
              return result;
            });
    when(retriever.getLatestAspectObject(any(), any(), anyString())).thenCallRealMethod();
    put(PRIMARY, "siblings", siblings(true, MEMBER));
    put(MEMBER, "siblings", siblings(false, PRIMARY));
    put(
        PRIMARY,
        "datasetProperties",
        new Aspect(new DatasetProperties().setName("daily_kpi").data()));
  }

  @Test
  public void testFlagOffDoesNothing() {
    new SiblingPrimaryNameIndexer(search, new SearchConfiguration())
        .onAspectIndexed(opContext, MEMBER, "siblings", siblings(false, PRIMARY));
    verifyNoInteractions(search, retriever);
  }

  @Test
  public void testNonPrimaryGetsItsPrimarysName() {
    indexer().onAspectIndexed(opContext, MEMBER, "siblings", siblings(false, PRIMARY));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"daily_kpi\"]}"),
            eq(MEMBER.toString()));
  }

  /** Covers a member indexed before its primary, and a primary that is renamed. */
  @Test
  public void testPrimaryPushesItsNameToNonPrimaries() {
    indexer().onAspectIndexed(opContext, PRIMARY, "siblings", siblings(true, MEMBER));
    put(PRIMARY, "datasetProperties", new Aspect(new DatasetProperties().setName("kpi_v2").data()));
    indexer()
        .onAspectIndexed(
            opContext, PRIMARY, "datasetProperties", new DatasetProperties().setName("kpi_v2"));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"daily_kpi\"]}"),
            eq(MEMBER.toString()));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"kpi_v2\"]}"),
            eq(MEMBER.toString()));
    verify(search, never()).upsertDocument(any(), any(), any(), eq(PRIMARY.toString()));
  }

  @Test
  public void testStoresBothThePropertiesAndKeyNames() {
    put(
        PRIMARY,
        "datasetKey",
        new Aspect(
            new DatasetKey()
                .setName("db.daily_kpi")
                .setOrigin(com.linkedin.common.FabricType.PROD)
                .setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:glue"))
                .data()));
    indexer().onAspectIndexed(opContext, MEMBER, "siblings", siblings(false, PRIMARY));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"daily_kpi\",\"db.daily_kpi\"]}"),
            eq(MEMBER.toString()));
  }

  /** A primary with more than one non-primary member pushes its name to each of them. */
  @Test
  public void testGroupLargerThanAPair() {
    Urn third = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:glue,db.weekly_kpi,PROD)");
    put(PRIMARY, "siblings", siblings(true, MEMBER, third));
    put(third, "siblings", siblings(false, PRIMARY));
    indexer().onAspectIndexed(opContext, PRIMARY, "siblings", siblings(true, MEMBER, third));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"daily_kpi\"]}"),
            eq(MEMBER.toString()));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"daily_kpi\"]}"),
            eq(third.toString()));
  }

  /** Nothing is written while the group has no primary yet; the primary pushes it later. */
  @Test
  public void testNoPrimaryWritesNothing() {
    put(PRIMARY, "siblings", siblings(false, MEMBER));
    indexer().onAspectIndexed(opContext, MEMBER, "siblings", siblings(false, PRIMARY));
    verifyNoInteractions(search);
  }

  /** A rename with a previous version still reaches the members. */
  @Test
  public void testChangedPrimaryNameIsPushed() {
    put(PRIMARY, "datasetProperties", new Aspect(new DatasetProperties().setName("kpi_v3").data()));
    indexer()
        .onAspectIndexed(
            opContext,
            PRIMARY,
            "datasetProperties",
            new DatasetProperties().setName("kpi_v3"),
            new DatasetProperties().setName("daily_kpi"));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"kpi_v3\"]}"),
            eq(MEMBER.toString()));
  }

  /** Re-ingesting a primary whose name did not change does no reads or writes. */
  @Test
  public void testUnchangedPrimaryNameIsSkipped() {
    DatasetProperties same = new DatasetProperties().setName("daily_kpi");
    indexer()
        .onAspectIndexed(
            opContext,
            PRIMARY,
            "datasetProperties",
            same,
            new DatasetProperties().setName("daily_kpi").setDescription("old"));
    verifyNoInteractions(search, retriever);
  }

  @Test
  public void testFallsBackToTheKeyName() {
    store.get(PRIMARY).remove("datasetProperties");
    put(
        PRIMARY,
        "datasetKey",
        new Aspect(
            new DatasetKey()
                .setName("db.daily_kpi")
                .setOrigin(com.linkedin.common.FabricType.PROD)
                .setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:glue"))
                .data()));
    indexer().onAspectIndexed(opContext, MEMBER, "siblings", siblings(false, PRIMARY));
    verify(search)
        .upsertDocument(
            any(),
            eq("dataset"),
            eq("{\"siblingPrimaryName\":[\"db.daily_kpi\"]}"),
            eq(MEMBER.toString()));
  }

  @Test
  public void testOtherEntityTypesAndAspectsAreIgnored() {
    indexer()
        .onAspectIndexed(
            opContext, UrnUtils.getUrn("urn:li:chart:(looker,c1)"), "siblings", siblings(false));
    indexer().onAspectIndexed(opContext, MEMBER, "ownership", null);
    verifyNoInteractions(search);
  }

  private SiblingPrimaryNameIndexer indexer() {
    return new SiblingPrimaryNameIndexer(
        search, new SearchConfiguration().setHideNonPrimarySiblings(true));
  }

  private void put(Urn urn, String aspectName, Object value) {
    Aspect aspect =
        value instanceof Siblings ? new Aspect(((Siblings) value).data()) : (Aspect) value;
    store.computeIfAbsent(urn, u -> new HashMap<>()).put(aspectName, aspect);
  }

  private static Siblings siblings(boolean primary, Urn... members) {
    return new Siblings()
        .setPrimary(primary)
        .setSiblings(new UrnArray(java.util.Arrays.asList(members)));
  }
}
