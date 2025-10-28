package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants.DESTINATION;
import static com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants.SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

/**
 * Utility class for testing graph query functionality. Contains methods moved from ESGraphQueryDAO
 * for testing purposes.
 */
public class TestUtils {

  /**
   * Builds a lineage query for a specific entity type. This method is used for testing the query
   * building logic.
   */
  public static QueryBuilder getLineageQueryForEntityType(
      @Nonnull Set<Urn> urns, @Nonnull Set<LineageRegistry.EdgeInfo> lineageEdges) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    Map<RelationshipDirection, Set<LineageRegistry.EdgeInfo>> edgesByDirection =
        lineageEdges.stream()
            .collect(
                Collectors.groupingBy(LineageRegistry.EdgeInfo::getDirection, Collectors.toSet()));

    Set<LineageRegistry.EdgeInfo> outgoingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.OUTGOING, Collections.emptySet());
    if (!outgoingEdges.isEmpty()) {
      query.should(getOutGoingEdgeQuery(urns, outgoingEdges));
    }

    Set<LineageRegistry.EdgeInfo> incomingEdges =
        edgesByDirection.getOrDefault(RelationshipDirection.INCOMING, Collections.emptySet());
    if (!incomingEdges.isEmpty()) {
      query.should(getIncomingEdgeQuery(urns, incomingEdges));
    }

    if (!incomingEdges.isEmpty() || !outgoingEdges.isEmpty()) {
      query.minimumShouldMatch(1);
    }

    return query;
  }

  private static BoolQueryBuilder getOutGoingEdgeQuery(
      @Nonnull Set<Urn> urns, @Nonnull Set<LineageRegistry.EdgeInfo> outgoingEdges) {
    BoolQueryBuilder outgoingEdgeQuery = QueryBuilders.boolQuery();
    outgoingEdgeQuery.filter(buildUrnFilters(urns, SOURCE));
    outgoingEdgeQuery.filter(buildEdgeFilters(outgoingEdges));
    return outgoingEdgeQuery;
  }

  private static BoolQueryBuilder getIncomingEdgeQuery(
      @Nonnull Set<Urn> urns, Set<LineageRegistry.EdgeInfo> incomingEdges) {
    BoolQueryBuilder incomingEdgeQuery = QueryBuilders.boolQuery();
    incomingEdgeQuery.filter(buildUrnFilters(urns, DESTINATION));
    incomingEdgeQuery.filter(buildEdgeFilters(incomingEdges));
    return incomingEdgeQuery;
  }

  private static QueryBuilder buildUrnFilters(@Nonnull Set<Urn> urns, @Nonnull String prefix) {
    // dedup urns while preserving order
    LinkedHashSet<String> urnSet = new LinkedHashSet<>();
    urns.forEach(urn -> urnSet.add(urn.toString()));
    return QueryBuilders.termsQuery(prefix + ".urn", urnSet);
  }

  private static QueryBuilder buildEdgeFilters(@Nonnull Set<LineageRegistry.EdgeInfo> edgeInfos) {
    return QueryBuilders.termsQuery(
        "relationshipType",
        edgeInfos.stream()
            .map(LineageRegistry.EdgeInfo::getType)
            .distinct()
            .collect(Collectors.toList()));
  }

  /**
   * Helper method to recursively check if any exception in the chain contains the specified
   * message. This allows tests to find expected exception messages regardless of how deeply they're
   * nested. Handles circular references in exception chains to prevent infinite loops.
   */
  public static boolean hasMessageInChain(Throwable throwable, String expectedMessage) {
    return hasMessageInChain(throwable, expectedMessage, new HashSet<>());
  }

  /** Private helper method that tracks visited exceptions to prevent infinite loops. */
  private static boolean hasMessageInChain(
      Throwable throwable, String expectedMessage, Set<Throwable> visited) {
    if (throwable == null) {
      return false;
    }

    // Check if we've already visited this exception to prevent infinite loops
    if (visited.contains(throwable)) {
      return false;
    }

    // Mark this exception as visited
    visited.add(throwable);

    // Check current exception's message
    String message = throwable.getMessage();
    if (message != null && message.contains(expectedMessage)) {
      return true;
    }

    // Recursively check the cause
    return hasMessageInChain(throwable.getCause(), expectedMessage, visited);
  }

  /**
   * Utility method to mock slice-based search behavior for tests. This method handles the
   * concurrent execution of slices and pagination by: 1. Returning the provided searchResponse for
   * the first call of each slice 2. Returning empty responses for subsequent pagination calls of
   * each slice
   */
  public static void mockSliceBasedSearch(
      SearchClientShim<?> mockClient,
      List<SearchResponse> slice1Responses,
      List<SearchResponse> slice2Responses) {

    // Calculate total hits from the actual size of hits arrays across all responses
    long totalHits = 0;
    for (SearchResponse response : slice1Responses) {
      totalHits += response.getHits().getHits().length;
    }
    for (SearchResponse response : slice2Responses) {
      totalHits += response.getHits().getHits().length;
    }

    // Track calls per slice to handle pagination
    final Map<Integer, Integer> sliceCallCounts = new ConcurrentHashMap<>();
    final long finalTotalHits = totalHits;

    try {
      when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
          .thenAnswer(
              invocation -> {
                SearchRequest request = invocation.getArgument(0);

                // Check if this is a slice-based search
                if (request.source() != null && request.source().slice() != null) {
                  int sliceId = request.source().slice().getId();

                  // Get current call count for this slice
                  int callCount = sliceCallCounts.getOrDefault(sliceId, 0);
                  sliceCallCounts.put(sliceId, callCount + 1);

                  if (sliceId == 0) {
                    // First slice
                    if (callCount < slice1Responses.size()) {
                      return slice1Responses.get(callCount);
                    } else {
                      // Return empty response for pagination with correct total hits
                      return createEmptySearchResponse(finalTotalHits);
                    }
                  } else if (sliceId == 1) {
                    // Second slice
                    if (callCount < slice2Responses.size()) {
                      return slice2Responses.get(callCount);
                    } else {
                      // Return empty response for pagination with correct total hits
                      return createEmptySearchResponse(finalTotalHits);
                    }
                  }
                }

                // Non-slice search throws exception
                throw new UnsupportedOperationException("Non-slice search not supported in tests");
              });
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup mock", e);
    }
  }

  /** Helper method to create an empty search response for pagination with correct total hits */
  public static SearchResponse createEmptySearchResponse(long totalHits) {
    SearchResponse response = mock(SearchResponse.class);

    // Create a real SearchHits mock with proper behavior
    SearchHits searchHits = mock(SearchHits.class);

    // Create an actual TotalHits instance from Lucene
    TotalHits totalHitsObj = new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO);

    // Configure the SearchHits mock with realistic behavior
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);
    when(searchHits.getTotalHits()).thenReturn(totalHitsObj);

    // Configure the SearchResponse mock
    when(response.getHits()).thenReturn(searchHits);

    return response;
  }

  /**
   * Utility method to create fake SearchHit objects for testing lineage relationships. This
   * consolidates the repetitive mock creation code used throughout the test class.
   *
   * @param count Number of SearchHit objects to create
   * @param sourceUrn The source URN for the relationship
   * @param destinationPrefix Prefix for destination URNs (will be suffixed with index)
   * @param relationshipType Type of relationship (e.g., "DownstreamOf", "UpstreamOf")
   * @param entityType Entity type for both source and destination (defaults to "dataset")
   * @return Array of mocked SearchHit objects
   */
  private static SearchHit[] createFakeLineageHits(
      int count,
      String sourceUrn,
      String destinationPrefix,
      String relationshipType,
      String entityType) {

    if (entityType == null) {
      entityType = "dataset";
    }

    SearchHit[] hits = new SearchHit[count];
    for (int i = 0; i < count; i++) {
      SearchHit hit = mock(SearchHit.class);
      Map<String, Object> source = new HashMap<>();

      // For DOWNSTREAM direction, we need to model the relationship correctly
      // The source is the entity we're querying from (upstream)
      // The destination is the downstream entity
      // The relationship direction should be INCOMING from the destination's perspective
      source.put(
          "source",
          Map.of(
              "entityType", entityType,
              "urn", sourceUrn));

      // Destination entity (downstream of source)
      source.put(
          "destination",
          Map.of(
              "entityType",
              entityType,
              "urn",
              "urn:li:"
                  + entityType
                  + ":(urn:li:dataPlatform:test,"
                  + destinationPrefix
                  + i
                  + ",PROD)"));

      // Relationship metadata
      source.put("relationshipType", relationshipType);
      source.put("createdOn", System.currentTimeMillis());
      source.put("updatedOn", System.currentTimeMillis());
      source.put("createdActor", "urn:li:corpuser:test");
      source.put("updatedActor", "urn:li:corpuser:test");
      source.put("properties", Map.of("source", "API"));

      when(hit.getSourceAsMap()).thenReturn(source);
      when(hit.getSortValues()).thenReturn(new Object[] {"sort_" + i});

      hits[i] = hit;
    }
    return hits;
  }

  /** Convenience method to create fake lineage hits with default entity type "dataset". */
  public static SearchHit[] createFakeLineageHits(
      int count, String sourceUrn, String destinationPrefix, String relationshipType) {
    return createFakeLineageHits(count, sourceUrn, destinationPrefix, relationshipType, null);
  }

  /**
   * Utility method to create a complete SearchResponse from an array of SearchHit objects. This
   * consolidates the repetitive SearchResponse mock creation code.
   *
   * @param hits Array of SearchHit objects
   * @param totalHits Total number of hits to report
   * @param scrollId Optional scroll ID (can be null)
   * @return Mocked SearchResponse object
   */
  public static SearchResponse createFakeSearchResponse(
      SearchHit[] hits, long totalHits, String scrollId) {
    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits())
        .thenReturn(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO));

    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(searchHits);

    if (scrollId != null) {
      when(searchResponse.getScrollId()).thenReturn(scrollId);
    }

    return searchResponse;
  }

  /** Convenience method to create a SearchResponse without scroll ID. */
  public static SearchResponse createFakeSearchResponse(SearchHit[] hits, long totalHits) {
    return createFakeSearchResponse(hits, totalHits, null);
  }
}
