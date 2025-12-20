package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.unit.TimeValue;

/**
 * Utility class for index deletion operations shared across ESWriteDAO and ESIndexBuilder. Provides
 * common logic for handling both aliases and concrete indices during deletion.
 */
@Slf4j
public class IndexDeletionUtils {

  private IndexDeletionUtils() {}

  /** Result of resolving an index name to its concrete indices for deletion. */
  public static class IndexResolutionResult {
    private final Collection<String> indicesToDelete;
    private final String nameToTrack;

    public IndexResolutionResult(Collection<String> indicesToDelete, String nameToTrack) {
      this.indicesToDelete = indicesToDelete;
      this.nameToTrack = nameToTrack;
    }

    /** The concrete index names to delete */
    public Collection<String> indicesToDelete() {
      return indicesToDelete;
    }

    /**
     * The name to track for recreation (alias name if it was an alias, otherwise the concrete index
     * name)
     */
    public String nameToTrack() {
      return nameToTrack;
    }
  }

  /**
   * Resolves an index name (which may be an alias or concrete index) to the concrete indices that
   * should be deleted, and determines what name to track for recreation.
   *
   * @param searchClient The search client to use for API calls
   * @param indexName The index name to resolve (may be an alias or concrete index)
   * @return IndexResolutionResult containing the concrete indices to delete and name to track, or
   *     null if the index doesn't exist
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public static IndexResolutionResult resolveIndexForDeletion(
      @Nonnull SearchClientShim<?> searchClient, @Nonnull String indexName) throws IOException {

    // Check if it's an alias
    GetAliasesRequest getAliasesRequest = new GetAliasesRequest(indexName);
    GetAliasesResponse aliasesResponse;
    try {
      aliasesResponse = searchClient.getIndexAliases(getAliasesRequest, RequestOptions.DEFAULT);
    } catch (IOException | RuntimeException e) {
      // If getIndexAliases throws, check if it's because index/alias doesn't exist
      if (e.getMessage() != null && e.getMessage().contains("index_not_found_exception")) {
        // Check if it's an actual index
        boolean indexExists =
            searchClient.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        if (!indexExists) {
          log.debug("Index {} does not exist, skipping", indexName);
          return null;
        }
        // Index exists but getIndexAliases failed, treat as concrete index (not an alias)
        aliasesResponse = null;
      } else {
        throw new IOException("Failed to get index aliases for " + indexName, e);
      }
    }

    Collection<String> indicesToDelete;
    String nameToTrack;

    if (aliasesResponse == null || aliasesResponse.getAliases().isEmpty()) {
      // Not an alias, must be a concrete index - verify it exists
      boolean indexExists =
          searchClient.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
      if (!indexExists) {
        log.debug("Index {} does not exist, skipping", indexName);
        return null;
      }

      // Check if this concrete index has any aliases pointing to it
      // If so, track the alias name for recreation, not the concrete index name
      GetAliasesRequest aliasesForIndexRequest = new GetAliasesRequest();
      aliasesForIndexRequest.indices(indexName);
      GetAliasesResponse aliasesForIndex =
          searchClient.getIndexAliases(aliasesForIndexRequest, RequestOptions.DEFAULT);

      nameToTrack = indexName;
      if (!aliasesForIndex.getAliases().isEmpty()
          && aliasesForIndex.getAliases().containsKey(indexName)) {
        // Get the alias names that point to this concrete index
        Set<AliasMetadata> aliases = aliasesForIndex.getAliases().get(indexName);
        if (aliases != null && !aliases.isEmpty()) {
          // Use the first alias name (typically there's only one)
          nameToTrack = aliases.iterator().next().alias();
          log.info(
              "Concrete index {} has alias {}, tracking alias for recreation",
              indexName,
              nameToTrack);
        }
      }

      indicesToDelete = List.of(indexName);
      log.info("Resolved concrete index {} for deletion, tracking as {}", indexName, nameToTrack);
    } else {
      // It's an alias, delete the concrete indices behind it
      indicesToDelete = aliasesResponse.getAliases().keySet();
      nameToTrack = indexName;
      log.info("Resolved alias {} to concrete indices {} for deletion", indexName, indicesToDelete);
    }

    return new IndexResolutionResult(indicesToDelete, nameToTrack);
  }

  /**
   * Deletes a concrete index with a timeout.
   *
   * @param searchClient The search client to use for API calls
   * @param concreteIndexName The concrete index name to delete (not an alias)
   * @throws IOException If there's an error deleting the index
   */
  public static void deleteConcreteIndex(
      @Nonnull SearchClientShim<?> searchClient, @Nonnull String concreteIndexName)
      throws IOException {
    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(concreteIndexName);
    deleteRequest.timeout(TimeValue.timeValueSeconds(30));
    searchClient.deleteIndex(deleteRequest, RequestOptions.DEFAULT);
    log.info("Successfully deleted index {}", concreteIndexName);
  }

  /**
   * Deletes an index (handling both aliases and concrete indices).
   *
   * @param searchClient The search client to use for API calls
   * @param indexName The index name to delete (may be an alias or concrete index)
   * @return The name to track for recreation (alias name if applicable), or null if index didn't
   *     exist
   * @throws IOException If there's an error deleting the index
   */
  public static String deleteIndex(
      @Nonnull SearchClientShim<?> searchClient, @Nonnull String indexName) throws IOException {
    IndexResolutionResult resolution = resolveIndexForDeletion(searchClient, indexName);
    if (resolution == null) {
      return null;
    }

    for (String concreteIndex : resolution.indicesToDelete()) {
      deleteConcreteIndex(searchClient, concreteIndex);
    }

    return resolution.nameToTrack();
  }
}
