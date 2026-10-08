package com.linkedin.datahub.graphql.types.mappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestionArray;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

/** Tests for {@link UrnSearchResultsMapper}. */
public class UrnSearchResultsMapperTest {

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
}
