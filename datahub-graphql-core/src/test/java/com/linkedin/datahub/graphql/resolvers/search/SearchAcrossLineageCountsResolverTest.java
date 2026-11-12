package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.EntityTypeToPlatforms;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageFlags;
import com.linkedin.datahub.graphql.generated.SchemaFieldValidationMode;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCounts;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCountsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchAcrossLineageCountsResolverTest {

  private static final String SOURCE_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),order_id)";
  private static final String DBT_PLATFORM = "urn:li:dataPlatform:dbt";

  private EntityClient _entityClient;
  private DataFetchingEnvironment _environment;
  private SearchAcrossLineageCountsResolver _resolver;

  @BeforeMethod
  public void setUp() {
    _entityClient = mock(EntityClient.class);
    _environment = mock(DataFetchingEnvironment.class);
    _resolver = new SearchAcrossLineageCountsResolver(_entityClient);
  }

  /** The minimum a caller has to supply; every other field is left for the resolver to default. */
  private static SearchAcrossLineageCountsInput input() {
    final SearchAcrossLineageCountsInput input = new SearchAcrossLineageCountsInput();
    input.setUrn(SOURCE_URN);
    input.setDirection(LineageDirection.DOWNSTREAM);
    input.setTypes(Collections.singletonList(EntityType.SCHEMA_FIELD));
    return input;
  }

  private static AndFilterInput andFilter(FacetFilterInput... criteria) {
    final AndFilterInput and = new AndFilterInput();
    and.setAnd(List.of(criteria));
    return and;
  }

  private static FacetFilterInput facetFilter(String field, String... values) {
    final FacetFilterInput filter = new FacetFilterInput();
    filter.setField(field);
    filter.setValues(List.of(values));
    return filter;
  }

  /** Everything the resolver hands to the entity client, from one invocation. */
  private static class Call {
    int total;
    OperationContext opContext;
    Urn urn;
    com.linkedin.metadata.graph.LineageDirection direction;
    List<String> entityNames;
    String query;
    Integer maxHops;
    Filter filter;
    List<SortCriterion> sortCriteria;

    com.linkedin.metadata.query.LineageFlags lineageFlags() {
      return opContext.getSearchContext().getLineageFlags();
    }

    com.linkedin.metadata.query.SearchFlags searchFlags() {
      return opContext.getSearchContext().getSearchFlags();
    }

    /** The fields the forwarded filter constrains, which is all the resolver decides about it. */
    List<String> filterFields() {
      return filter.getOr().stream()
          .flatMap(branch -> branch.getAnd().stream())
          .map(Criterion::getField)
          .collect(Collectors.toList());
    }
  }

  private Call run(SearchAcrossLineageCountsInput input) throws Exception {
    // Tests that compare two inputs run this more than once
    clearInvocations(_entityClient);
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    when(_environment.getArgument(eq("input"))).thenReturn(input);
    when(_entityClient.searchAcrossLineage(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new LineageSearchResult()
                .setEntities(new LineageSearchEntityArray())
                .setNumEntities(7));

    final SearchAcrossLineageCounts counts = _resolver.get(_environment).join();

    final ArgumentCaptor<OperationContext> opContext =
        ArgumentCaptor.forClass(OperationContext.class);
    final ArgumentCaptor<Urn> urn = ArgumentCaptor.forClass(Urn.class);
    final ArgumentCaptor<com.linkedin.metadata.graph.LineageDirection> direction =
        ArgumentCaptor.forClass(com.linkedin.metadata.graph.LineageDirection.class);
    final ArgumentCaptor<List<String>> entityNames = ArgumentCaptor.forClass(List.class);
    final ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Integer> maxHops = ArgumentCaptor.forClass(Integer.class);
    final ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    final ArgumentCaptor<List<SortCriterion>> sortCriteria = ArgumentCaptor.forClass(List.class);
    verify(_entityClient)
        .searchAcrossLineage(
            opContext.capture(),
            urn.capture(),
            direction.capture(),
            entityNames.capture(),
            query.capture(),
            maxHops.capture(),
            filter.capture(),
            sortCriteria.capture(),
            anyInt(),
            anyInt());

    final Call call = new Call();
    call.total = counts.getTotal();
    call.opContext = opContext.getValue();
    call.urn = urn.getValue();
    call.direction = direction.getValue();
    call.entityNames = entityNames.getValue();
    call.query = query.getValue();
    call.maxHops = maxHops.getValue();
    call.filter = filter.getValue();
    call.sortCriteria = sortCriteria.getValue();
    return call;
  }

  @Test
  public void testForwardsWhatIsBeingCounted() throws Exception {
    final Call call = run(input());

    assertEquals(call.total, 7);
    assertEquals(call.urn.toString(), SOURCE_URN);
    assertEquals(call.direction, com.linkedin.metadata.graph.LineageDirection.DOWNSTREAM);
    assertEquals(call.entityNames, List.of("schemaField"));
    // No query and no sort, which the graph-only path could not serve anyway. The service treats a
    // null query as matching everything.
    assertNull(call.query);
    assertTrue(call.sortCriteria.isEmpty());
  }

  @Test
  public void testCountsUpstream() throws Exception {
    final SearchAcrossLineageCountsInput input = input();
    input.setDirection(LineageDirection.UPSTREAM);

    assertEquals(run(input).direction, com.linkedin.metadata.graph.LineageDirection.UPSTREAM);
  }

  @Test
  public void testNoTypesCountsEveryEntityType() throws Exception {
    final SearchAcrossLineageCountsInput input = input();
    input.setTypes(null);

    assertTrue(run(input).entityNames.isEmpty());
  }

  @Test
  public void testNothingIsFetchedToCount() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    when(_environment.getArgument(eq("input"))).thenReturn(input());
    when(_entityClient.searchAcrossLineage(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new LineageSearchResult()
                .setEntities(new LineageSearchEntityArray())
                .setNumEntities(3));

    _resolver.get(_environment).join();

    // Entities are never returned, which is what keeps existence checks from stripping the very
    // entities being counted back out of the result
    verify(_entityClient)
        .searchAcrossLineage(any(), any(), any(), any(), any(), any(), any(), any(), eq(0), eq(0));
  }

  @Test
  public void testMaxHopsAndFilterComeFromOrFilters() throws Exception {
    final SearchAcrossLineageCountsInput input = input();
    final FacetFilterInput notDbt = facetFilter("parent", DBT_PLATFORM);
    notDbt.setCondition(FilterOperator.CONTAIN);
    notDbt.setNegated(true);
    input.setOrFilters(List.of(andFilter(facetFilter("degree", "1"), notDbt)));

    final Call call = run(input);

    // A degree filter also caps the walk, so the graph is not traversed further than is counted
    assertEquals(call.maxHops, Integer.valueOf(1));
    assertEquals(call.filterFields(), List.of("degree", "parent"));
  }

  @Test
  public void testNoOrFiltersLeavesTheWalkUnbounded() throws Exception {
    final Call call = run(input());

    assertNull(call.maxHops);
    assertNull(call.filter);
  }

  @Test
  public void testCountsReadOffTheGraphWhenGhostEntitiesWanted() throws Exception {
    final SearchAcrossLineageCountsInput input = input();
    input.setIncludeGhostEntities(true);

    assertTrue(
        run(input).lineageFlags().isForceLightningMode(),
        "asking for ghost entities should switch the search service to the graph");
  }

  @Test
  public void testCountsComeFromTheIndexByDefault() throws Exception {
    // The graph-only path carries caveats, so it should not be the default
    assertFalse(Boolean.TRUE.equals(run(input()).lineageFlags().isForceLightningMode()));

    final SearchAcrossLineageCountsInput explicit = input();
    explicit.setIncludeGhostEntities(false);
    assertFalse(Boolean.TRUE.equals(run(explicit).lineageFlags().isForceLightningMode()));
  }

  @Test
  public void testValidateSchemaFieldsDefaultsToAuto() throws Exception {
    assertEquals(
        run(input()).lineageFlags().getValidateSchemaFields(),
        com.linkedin.metadata.query.SchemaFieldValidationMode.AUTO);

    final SearchAcrossLineageCountsInput off = input();
    off.setValidateSchemaFields(SchemaFieldValidationMode.NONE);
    assertEquals(
        run(off).lineageFlags().getValidateSchemaFields(),
        com.linkedin.metadata.query.SchemaFieldValidationMode.NONE);
  }

  @Test
  public void testLineageFlagsArePassedThrough() throws Exception {
    final EntityTypeToPlatforms dbtColumns = new EntityTypeToPlatforms();
    dbtColumns.setEntityType(EntityType.SCHEMA_FIELD);
    dbtColumns.setPlatforms(List.of(DBT_PLATFORM));
    final LineageFlags flags = new LineageFlags();
    flags.setIgnoreAsHops(List.of(dbtColumns));
    flags.setStartTimeMillis(100L);
    flags.setEndTimeMillis(200L);

    final SearchAcrossLineageCountsInput input = input();
    input.setLineageFlags(flags);
    final com.linkedin.metadata.query.LineageFlags mapped = run(input).lineageFlags();

    assertEquals(mapped.getIgnoreAsHops().get("schemaField").get(0).toString(), DBT_PLATFORM);
    assertEquals(mapped.getStartTimeMillis(), Long.valueOf(100L));
    assertEquals(mapped.getEndTimeMillis(), Long.valueOf(200L));
  }

  @Test
  public void testSearchFlagsAreFixedForCounting() throws Exception {
    final com.linkedin.metadata.query.SearchFlags flags = run(input()).searchFlags();

    // An empty grouping spec is what keeps a count of schema fields from being folded into a count
    // of the datasets holding them, which the service does by default
    assertFalse(flags.getGroupingSpec().hasGroupingCriteria());
    assertFalse(flags.isFulltext());
    assertTrue(flags.isSkipAggregates());
    assertTrue(flags.isSkipHighlighting());
    // Version filtering would drop relations the graph still draws
    assertFalse(flags.isFilterNonLatestVersions());
    assertFalse(flags.isIncludeSoftDeleted());
  }

  @Test
  public void testIncludeSoftDeletedIsForwarded() throws Exception {
    final SearchAcrossLineageCountsInput input = input();
    input.setIncludeSoftDeleted(true);

    assertTrue(run(input).searchFlags().isIncludeSoftDeleted());
  }

  @Test
  public void testInvalidUrnRejected() {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    final SearchAcrossLineageCountsInput bad = input();
    bad.setUrn("not-a-valid-urn");
    when(_environment.getArgument(eq("input"))).thenReturn(bad);

    assertThrows(IllegalArgumentException.class, () -> _resolver.get(_environment).join());
  }

  @Test
  public void testSearchFailureIsSurfaced() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    when(_environment.getArgument(eq("input"))).thenReturn(input());
    final IllegalStateException cause = new IllegalStateException("search unavailable");
    when(_entityClient.searchAcrossLineage(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenThrow(cause);

    // Never answered with a zero count, which would read as "this has no lineage"
    final RuntimeException thrown =
        expectThrows(RuntimeException.class, () -> _resolver.get(_environment).join());
    assertTrue(hasCause(thrown, cause));
  }

  private static boolean hasCause(Throwable thrown, Throwable cause) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t == cause) {
        return true;
      }
    }
    return false;
  }
}
