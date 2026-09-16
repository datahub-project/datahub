package com.linkedin.metadata.search.semantic;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Interface for semantic entity search implementations. */
public interface SemanticEntitySearch {

  /**
   * Execute semantic search against semantic indices for the provided entities.
   *
   * <p>Implementations should generate an embedding from {@code input}, perform a kNN search over
   * the semantic indices corresponding to {@code entityNames}, apply any {@code postFilters}, and
   * return a paginated {@link com.linkedin.metadata.search.SearchResult} honoring {@code from} and
   * {@code pageSize}.
   *
   * @param opContext operation context carrying session, auth, and search flags
   * @param entityNames list of entity types to search (assumed pre-validated/non-empty by callers)
   * @param input the raw user query to embed and search semantically
   * @param postFilters optional document-level filters to apply
   * @param sortCriterion optional sort criterion; semantic v1 typically ignores custom sort and
   *     ranks strictly by semantic score
   * @param from zero-based offset for pagination into the ranked candidate set
   * @param pageSize requested page size; when null, implementations should normalize to a sensible
   *     default
   * @return {@link com.linkedin.metadata.search.SearchResult} containing entities, metadata, and
   *     pagination fields populated
   */
  @Nonnull
  SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      @Nullable Integer pageSize);
}
