package com.linkedin.datahub.graphql.types.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestionArray;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for {@link UrnSearchResultsMapper}. */
public class UrnSearchResultsMapperTest {

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  private static SearchEntity hit(String urn) throws Exception {
    return new SearchEntity()
        .setEntity(Urn.createFromString(urn))
        .setMatchedFields(new MatchedFieldArray())
        .setFeatures(new DoubleMap());
  }

  private static SearchResult backendResult(SearchEntityArray entities) {
    return new SearchResult()
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(entities.size())
        .setEntities(entities)
        .setMetadata(
            new SearchResultMetadata()
                .setAggregations(new AggregationMetadataArray())
                .setSuggestions(new SearchSuggestionArray()));
  }

  @Test
  public void testUnmappableHitIsDroppedNotFailed() throws Exception {
    // A URN whose entity type UrnToEntityMapper does not model maps to a null entity. A single such
    // hit — e.g. an orphan or unmapped-type entry in the semantic index — must not null the whole
    // page (SearchResult.entity is non-null in GraphQL), so it is dropped instead.
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"),
            hit("urn:li:unmappableTestEntity:orphan-in-index"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/b,PROD)"));

    SearchResults mapped = UrnSearchResultsMapper.map(null, backendResult(entities));

    // The two mappable datasets survive; the unmappable hit is excluded, not fatal.
    assertEquals(mapped.getSearchResults().size(), 2);
    assertTrue(mapped.getSearchResults().stream().allMatch(r -> r.getEntity() != null));
    // total is left as the backend hit count.
    assertEquals(mapped.getTotal(), 3);
  }

  @Test
  public void testAllMappableHitsPassThroughUnchanged() throws Exception {
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/b,PROD)"));

    SearchResults mapped = UrnSearchResultsMapper.map(null, backendResult(entities));

    assertEquals(mapped.getSearchResults().size(), 2);
    assertEquals(mapped.getTotal(), 2);
  }

  @Test
  public void testHitReferencingUnknownEntityTypeIsDropped() throws Exception {
    // After a rollback, a schema field can belong to an entity type only the newer version
    // registered. The schema field itself is a known type, but its parent can't be represented, so
    // the hit is skipped like an unmappable one instead of failing the page.
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD),c1)"),
            hit("urn:li:schemaField:(urn:li:entityFromNewerBuild:x,c2)"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/b,PROD)"));

    SearchResults mapped = UrnSearchResultsMapper.map(context, backendResult(entities));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD),c1)",
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/b,PROD)"));
  }

  @Test
  public void testOnlyHitsUnknownToTheRegistryCountAsRollbackSkips() throws Exception {
    // telemetry is in the registry but GraphQL doesn't model it: dropped and logged, but not
    // counted in unknown_to_registry_skipped, which measures data from a newer version.
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext opContext = spy(TestOperationContexts.systemContextNoSearchAuthorization());
    when(opContext.getMetricUtils()).thenReturn(Optional.of(metrics));
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:telemetry:client"),
            hit("urn:li:entityFromNewerBuild:x"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"));

    SearchResults mapped = UrnSearchResultsMapper.map(context, backendResult(entities));

    assertEquals(mapped.getSearchResults().size(), 1);
    verify(metrics, times(1))
        .increment(any(Class.class), eq(UnknownDataGuard.SKIPPED_METRIC), anyDouble());
  }

  @Test
  public void testStrictDropsOnlyUnmappableHitsWithoutRegistryChecks() throws Exception {
    // Without tolerance, hits are dropped only when GraphQL doesn't model their type, as before: a
    // schema field of an entity type the registry doesn't know is kept and nothing is counted.
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext opContext = spy(TestOperationContexts.systemContextNoSearchAuthorization());
    when(opContext.getMetricUtils()).thenReturn(Optional.of(metrics));
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col_a)"),
            hit("urn:li:entityFromNewerBuild:x"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"));

    SearchResults mapped = UrnSearchResultsMapper.map(context, backendResult(entities));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(
            "urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col_a)",
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"));
    verify(metrics, never())
        .increment(any(Class.class), eq(UnknownDataGuard.SKIPPED_METRIC), anyDouble());
  }

  @Test
  public void testStrictScrollKeepsUnmappableHits() throws Exception {
    // Scroll results never filtered hits before tolerance; without it they still don't.
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    SearchEntityArray entities =
        new SearchEntityArray(
            hit("urn:li:entityFromNewerBuild:x"),
            hit("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)"));
    ScrollResult scrollResult =
        new ScrollResult()
            .setScrollId("next")
            .setPageSize(10)
            .setNumEntities(entities.size())
            .setEntities(entities)
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray())
                    .setSuggestions(new SearchSuggestionArray()));

    ScrollResults mapped = UrnScrollResultsMapper.map(null, scrollResult);

    assertEquals(mapped.getSearchResults().size(), 2);
    assertNull(mapped.getSearchResults().get(0).getEntity());
  }

  @Test
  public void testSchemaFieldWhoseParentGraphQLCannotMapIsLeftOut() throws Exception {
    // SchemaFieldEntity.parent is non-null: a field whose parent GraphQL doesn't map (telemetry is
    // in the registry but not modelled) would null its hit and the whole page, so it is left out.
    String dataset = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)";
    String onDataset = "urn:li:schemaField:(" + dataset + ",col_a)";
    String onTelemetry = "urn:li:schemaField:(urn:li:telemetry:client,col_b)";
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    SearchResults mapped =
        UrnSearchResultsMapper.map(
            context, backendResult(new SearchEntityArray(hit(onTelemetry), hit(onDataset))));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(onDataset));
    assertEquals(mapped.getTotal(), 2);
  }

  @Test
  public void testStrictKeepsSchemaFieldWhoseParentGraphQLCannotMap() throws Exception {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    String onTelemetry = "urn:li:schemaField:(urn:li:telemetry:client,col_b)";
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    SearchResults mapped =
        UrnSearchResultsMapper.map(context, backendResult(new SearchEntityArray(hit(onTelemetry))));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(onTelemetry));
  }

  @Test
  public void testStrictKeepsPoliciesWithoutReadingThem() throws Exception {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    AspectRetriever retriever = mock(AspectRetriever.class);
    OperationContext opContext = spy(TestOperationContexts.systemContextNoSearchAuthorization());
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    String policy = "urn:li:dataHubPolicy:unknown-condition";

    SearchResults mapped =
        UrnSearchResultsMapper.map(context, backendResult(new SearchEntityArray(hit(policy))));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(policy));
    verify(retriever, never()).getLatestAspectObjects(any(), anySet(), anySet());
  }

  @Test
  public void testPolicyInfoReadFailureLeavesThePolicyHitsOut() throws Exception {
    // Unchecked policies can't be kept: the loader would return an unrepresentable one as missing,
    // nulling the page. The other hits stay.
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenThrow(new RuntimeException("storage unavailable"));
    OperationContext opContext = spy(TestOperationContexts.systemContextNoSearchAuthorization());
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    String policy = "urn:li:dataHubPolicy:any";
    String dataset = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/a,PROD)";

    SearchResults mapped =
        UrnSearchResultsMapper.map(
            context, backendResult(new SearchEntityArray(hit(policy), hit(dataset))));

    assertEquals(
        mapped.getSearchResults().stream().map(r -> r.getEntity().getUrn()).toList(),
        List.of(dataset));
  }
}
