package com.linkedin.metadata.search.hybrid;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Composes query embeddings, V3 kNN, and lexical-backed hybrid result reordering.
 *
 * <p>Only rows of the entity types that have vectors (documents by default) are reordered, and only
 * among the positions those rows already hold. Other entity types have no vector to compare, so
 * their rows keep their lexical positions and keyword search keeps ranking across entity types.
 * When every requested entity type has vectors, all rows are reordered.
 */
public class HybridSearchResultReranker {

  private final HybridQueryEmbeddingService queryEmbeddingService;
  private final V3HybridKnnRequestBuilder knnRequestBuilder;
  private final HybridScoreMapBuilder scoreMapBuilder;
  private final HybridCandidateMerger candidateMerger;

  public HybridSearchResultReranker(
      @Nonnull final HybridQueryEmbeddingService queryEmbeddingService,
      @Nonnull final V3HybridKnnRequestBuilder knnRequestBuilder,
      @Nonnull final HybridScoreMapBuilder scoreMapBuilder,
      @Nonnull final HybridCandidateMerger candidateMerger) {
    this.queryEmbeddingService = queryEmbeddingService;
    this.knnRequestBuilder = knnRequestBuilder;
    this.scoreMapBuilder = scoreMapBuilder;
    this.candidateMerger = candidateMerger;
  }

  /**
   * Returns {@code lexicalRows} with the rows of entity types that have vectors reordered by
   * combined lexical and vector score, each moved into a position such a row held before.
   */
  @Nonnull
  public List<SearchEntity> rerank(
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<String> entityNames,
      @Nonnull final String query,
      @Nonnull final List<SearchEntity> lexicalRows,
      final int k,
      @Nonnull final Collection<String> fieldsToFetch,
      @Nullable final Map<String, Object> rootFilter)
      throws IOException {
    return reorder(
        lexicalRows,
        candidates(opContext, entityNames, query, lexicalRows, k, fieldsToFetch, rootFilter));
  }

  /**
   * Scores the rows of entity types that have vectors, highest combined score first. Empty when the
   * query is a wildcard or no row has an entity type with vectors, in which case no embedding or
   * kNN request is made.
   */
  @Nonnull
  public List<HybridCandidate> candidates(
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<String> entityNames,
      @Nonnull final String query,
      @Nonnull final List<SearchEntity> lexicalRows,
      final int k,
      @Nonnull final Collection<String> fieldsToFetch,
      @Nullable final Map<String, Object> rootFilter)
      throws IOException {
    if (query.isBlank() || "*".equals(query) || lexicalRows.isEmpty()) {
      return List.of();
    }
    final Set<String> vectorEntityNames =
        knnRequestBuilder.vectorEntityNames(opContext, entityNames);
    final Map<Urn, Double> lexicalScores = scoreMapBuilder.lexicalScores(lexicalRows);
    lexicalScores.keySet().removeIf(urn -> !vectorEntityNames.contains(urn.getEntityType()));
    if (lexicalScores.isEmpty()) {
      return List.of();
    }

    final HybridQueryEmbeddingService.QueryEmbedding queryEmbedding =
        queryEmbeddingService.embed(query);
    final Optional<KnnSearchRequest> knnRequest =
        knnRequestBuilder.build(
            opContext,
            entityNames,
            queryEmbedding.modelEmbeddingKey(),
            queryEmbedding.vector(),
            k,
            fieldsToFetch,
            rootFilter);
    if (knnRequest.isEmpty()) {
      return List.of();
    }

    final KnnSearchResponse knnResponse =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3)
            .searchKnn(opContext, knnRequest.get());
    return candidateMerger.merge(lexicalScores, scoreMapBuilder.vectorScores(knnResponse));
  }

  /**
   * Fills the positions of the rows that have a candidate with those rows in candidate order; every
   * other row keeps its position. A URN listed twice keeps its later rows where they are.
   */
  @Nonnull
  static List<SearchEntity> reorder(
      @Nonnull final List<SearchEntity> rows, @Nonnull final List<HybridCandidate> candidates) {
    if (candidates.isEmpty()) {
      return rows;
    }
    final Set<Urn> candidateUrns = new HashSet<>();
    candidates.forEach(candidate -> candidateUrns.add(candidate.entity()));
    final Map<Urn, SearchEntity> candidateRows = new LinkedHashMap<>();
    final List<Integer> slots = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      final Urn urn = rows.get(i).getEntity();
      if (urn != null && candidateUrns.contains(urn) && !candidateRows.containsKey(urn)) {
        candidateRows.put(urn, rows.get(i));
        slots.add(i);
      }
    }
    final List<SearchEntity> reordered = new ArrayList<>(rows);
    final Iterator<Integer> slot = slots.iterator();
    for (HybridCandidate candidate : candidates) {
      final SearchEntity row = candidateRows.get(candidate.entity());
      if (row != null) {
        reordered.set(slot.next(), row);
      }
    }
    return reordered;
  }
}
