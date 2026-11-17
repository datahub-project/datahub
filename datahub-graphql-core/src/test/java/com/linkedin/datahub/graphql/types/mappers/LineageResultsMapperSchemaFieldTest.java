package com.linkedin.datahub.graphql.types.mappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * SearchAcrossLineageResult.entity is non-null, so lineage leaves out a hit GraphQL can't return,
 * as search does: a schema field whose parent it can't represent (telemetry is in the registry but
 * not modelled), an entity type it doesn't model (dataProcess has lineage but no GraphQL type), and
 * an entity type only a newer version registered.
 */
public class LineageResultsMapperSchemaFieldTest {

  private static final String ON_DATASET =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,linked,PROD),col_a)";
  private static final String ON_TELEMETRY = "urn:li:schemaField:(urn:li:telemetry:client,col_b)";
  private static final String DATA_PROCESS = "urn:li:dataProcess:(proc,airflow,PROD)";
  private static final String FROM_NEWER_BUILD = "urn:li:entityFromNewerBuild:x";
  private static final List<String> HITS =
      List.of(ON_TELEMETRY, DATA_PROCESS, ON_DATASET, FROM_NEWER_BUILD);

  private QueryContext context;

  @BeforeMethod
  public void setUp() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    context = mock(QueryContext.class);
    when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testSearchAcrossLineageLeavesOutHitsGraphQLCannotReturn() {
    assertEquals(
        UrnSearchAcrossLineageResultsMapper.map(context, searchResult(HITS))
            .getSearchResults()
            .stream()
            .map(r -> r.getEntity().getUrn())
            .toList(),
        List.of(ON_DATASET));
  }

  @Test
  public void testScrollAcrossLineageLeavesOutHitsGraphQLCannotReturn() {
    LineageScrollResult scroll =
        new LineageScrollResult()
            .setPageSize(10)
            .setNumEntities(HITS.size())
            .setEntities(entities(HITS))
            .setMetadata(metadata());

    assertEquals(
        UrnScrollAcrossLineageResultsMapper.map(context, scroll).getSearchResults().stream()
            .map(r -> r.getEntity().getUrn())
            .toList(),
        List.of(ON_DATASET));
  }

  @Test
  public void testStrictKeepsEveryHit() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);

    // As before: a hit GraphQL can't map keeps its place with no entity.
    assertEquals(
        UrnSearchAcrossLineageResultsMapper.map(context, searchResult(HITS))
            .getSearchResults()
            .stream()
            .map(r -> r.getEntity() == null ? null : r.getEntity().getUrn())
            .toList(),
        Arrays.asList(ON_TELEMETRY, null, ON_DATASET, null));
  }

  private static LineageSearchResult searchResult(List<String> urns) {
    return new LineageSearchResult()
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(urns.size())
        .setEntities(entities(urns))
        .setMetadata(metadata());
  }

  private static LineageSearchEntityArray entities(List<String> urns) {
    LineageSearchEntityArray entities = new LineageSearchEntityArray();
    for (String urn : urns) {
      entities.add(
          new LineageSearchEntity()
              .setEntity(UrnUtils.getUrn(urn))
              .setDegree(1)
              .setMatchedFields(new MatchedFieldArray())
              .setPaths(new UrnArrayArray()));
    }
    return entities;
  }

  private static SearchResultMetadata metadata() {
    return new SearchResultMetadata().setAggregations(new AggregationMetadataArray());
  }
}
