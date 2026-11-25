package com.linkedin.metadata.search.embedding;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Where a search template vector rung finds the nearest neighbors of an embedding profile. The
 * DataHub index is the native store. An external store is an adapter named in {@code
 * semanticSearch.stores}, which also sets the capabilities it has. The service embeds the query and
 * sends the vector. Every hit from an external store is hydrated on the DataHub index, so a
 * deleted, hidden, or unauthorized entity never surfaces.
 */
public interface VectorStore {
  /** The store applies the caller's filter and the system filters inside the vector search. */
  String PRE_FILTER = "preFilter";

  /** The store finds an entity by URN and returns its query vectors. Similarity needs it. */
  String FIND_BY_ID = "findById";

  /**
   * Neighbors of the request's vector, best first, at most {@code k}, within {@code timeout}. An
   * entity may appear once per chunk; the service keeps its best cosine.
   */
  @Nonnull
  List<Hit> search(
      @Nonnull OperationContext opContext, @Nonnull Request request, @Nonnull Duration timeout)
      throws IOException;

  /**
   * The query vectors the store holds for {@code urn} in {@code collection}, one per chunk, or
   * empty when it does not hold the entity. Called only on a store with {@link #FIND_BY_ID}. Other
   * entities' vectors are never returned.
   */
  @Nonnull
  List<float[]> findById(
      @Nonnull OperationContext opContext,
      @Nonnull String collection,
      @Nonnull Urn urn,
      @Nonnull Duration timeout)
      throws IOException;

  /**
   * @param collection the embedding profile id
   * @param vector the query vector the service embedded
   * @param entityTypes entity types to search
   * @param filter the caller's filter, sent only to a store with {@link #PRE_FILTER}. That store
   *     also applies the system filters the search flags set.
   */
  record Request(
      @Nonnull String collection,
      @Nonnull float[] vector,
      int k,
      @Nonnull List<String> entityTypes,
      @Nullable Filter filter) {}

  /**
   * A neighbor: its URN and raw cosine, from -1 to 1. The service derives the result score from the
   * cosine, so a store cannot raise a hit past a score floor.
   */
  record Hit(@Nonnull Urn urn, double similarity) {}
}
