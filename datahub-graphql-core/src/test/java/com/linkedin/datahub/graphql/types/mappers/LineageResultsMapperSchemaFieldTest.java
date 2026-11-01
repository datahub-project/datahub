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
import com.linkedin.metadata.utils.UnknownDataHandlings;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * SearchAcrossLineageResult.entity is non-null, so lineage leaves out a schema field whose parent
 * GraphQL can't represent (telemetry is in the registry but not modelled), as search does.
 */
public class LineageResultsMapperSchemaFieldTest {

  private static final String ON_DATASET =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,linked,PROD),col_a)";
  private static final String ON_TELEMETRY = "urn:li:schemaField:(urn:li:telemetry:client,col_b)";

  private QueryContext context;

  @BeforeMethod
  public void setUp() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
    context = mock(QueryContext.class);
    when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testSearchAcrossLineageLeavesOutTheField() {
    assertEquals(
        UrnSearchAcrossLineageResultsMapper.map(context, searchResult()).getSearchResults().stream()
            .map(r -> r.getEntity().getUrn())
            .toList(),
        List.of(ON_DATASET));
  }

  @Test
  public void testScrollAcrossLineageLeavesOutTheField() {
    LineageScrollResult scroll =
        new LineageScrollResult()
            .setPageSize(10)
            .setNumEntities(2)
            .setEntities(entities())
            .setMetadata(metadata());

    assertEquals(
        UrnScrollAcrossLineageResultsMapper.map(context, scroll).getSearchResults().stream()
            .map(r -> r.getEntity().getUrn())
            .toList(),
        List.of(ON_DATASET));
  }

  @Test
  public void testStrictKeepsTheField() {
    UnknownDataHandlings.use(StrictHandling.INSTANCE);

    assertEquals(
        UrnSearchAcrossLineageResultsMapper.map(context, searchResult()).getSearchResults().stream()
            .map(r -> r.getEntity().getUrn())
            .toList(),
        List.of(ON_TELEMETRY, ON_DATASET));
  }

  private static LineageSearchResult searchResult() {
    return new LineageSearchResult()
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(2)
        .setEntities(entities())
        .setMetadata(metadata());
  }

  private static LineageSearchEntityArray entities() {
    LineageSearchEntityArray entities = new LineageSearchEntityArray();
    for (String urn : List.of(ON_TELEMETRY, ON_DATASET)) {
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
