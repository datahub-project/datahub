package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface EntitySearchService {

  void configure();

  /** Clear all data within the service */
  void clear();

  /**
   * Get the number of documents corresponding to the entity
   *
   * @param entityName name of the entity
   */
  long docCount(@Nonnull String entityName);

  /**
   * Updates or inserts the given search document.
   *
   * @param entityName name of the entity
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId);

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param entityName name of the entity
   * @param docId the ID of the document to delete
   */
  void deleteDocument(@Nonnull String entityName, @Nonnull String docId);

  /**
   * Appends a run id to the list for a certain document
   *
   * @param entityName name of the entity
   * @param urn the urn of the user
   * @param runId the ID of the run
   */
  void appendRunId(@Nonnull String entityName, @Nonnull Urn urn, @Nullable String runId);

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * <p>Safe for non-structured, user input, queries with an attempt to provide some advanced
   * features <a
   * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html">Impl</a>
   *
   * @param entityNames names of the entities
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param searchFlags flags controlling search options
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  SearchResult search(
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size,
      @Nullable SearchFlags searchFlags);

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * <p>Safe for non-structured, user input, queries with an attempt to provide some advanced
   * features <a
   * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html">Impl</a>
   *
   * @param entityNames names of the entities
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param searchFlags flags controlling search options
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  SearchResult search(
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size,
      @Nullable SearchFlags searchFlags,
      @Nullable List<String> facets);

  /**
   * Gets a list of documents after applying the input filters.
   *
   * @param entityName name of the entity
   * @param filters the request map with fields and values to be applied as filters to the search
   *     query
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size number of search hits to return
   * @return a {@link SearchResult} that contains a list of filtered documents and related search
   *     result metadata
   */
  @Nonnull
  SearchResult filter(
      @Nonnull String entityName,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size);

  /**
   * Returns a list of suggestions given type ahead query.
   *
   * <p>The advanced auto complete can take filters and provides suggestions based on filtered
   * context.
   *
   * @param entityName name of the entity
   * @param query the type ahead query text
   * @param field the field name for the auto complete
   * @param requestParams specify the field to auto complete and the input text
   * @param limit the number of suggestions returned
   * @return A list of suggestions as string
   */
  @Nonnull
  AutoCompleteResult autoComplete(
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      int limit);

  /**
   * Returns number of documents per field value given the field and filters
   *
   * @param entityNames list of name of entities to aggregate across, if empty aggregate over all
   *     entities
   * @param field the field name for aggregate
   * @param requestParams filters to apply before aggregating
   * @param limit the number of aggregations to return
   * @return
   */
  @Nonnull
  Map<String, Long> aggregateByValue(
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      int limit);

  /**
   * Gets a list of groups/entities that match given browse request.
   *
   * @param entityName type of entity to query
   * @param path the path to be browsed
   * @param requestParams the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link BrowseResult} that contains a list of groups/entities
   */
  @Nonnull
  BrowseResult browse(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter requestParams,
      int from,
      int size);

  /**
   * Gets browse snapshot of a given path
   *
   * @param entityName entity being browsed
   * @param path path being browsed
   * @param filter browse filter
   * @param input search query
   * @param start start offset of first group
   * @param count max number of results requested
   */
  @Nonnull
  public BrowseResultV2 browseV2(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count);

  /**
   * Gets a list of paths for a given urn.
   *
   * @param entityName type of entity to query
   * @param urn urn of the entity
   * @return all paths related to a given urn
   */
  @Nonnull
  List<String> getBrowsePaths(@Nonnull String entityName, @Nonnull Urn urn);

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param entities name of the entities to scroll across
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll identifier to pass to search service
   * @param size the number of search hits to return
   * @param searchFlags flags controlling search options
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  ScrollResult fullTextScroll(
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size,
      @Nullable SearchFlags searchFlags);

  /**
   * Gets a list of documents that match given search request. The results are aggregated and
   * filters are applied to the search hits and not the aggregation results.
   *
   * @param entities name of the entities to scroll across
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll identifier to pass to search service
   * @param size the number of search hits to return
   * @param searchFlags flags controlling search options
   * @return a {@link ScrollResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  ScrollResult structuredScroll(
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size,
      @Nullable SearchFlags searchFlags);

  /** Max result size returned by the underlying search backend */
  int maxResultSize();
}
