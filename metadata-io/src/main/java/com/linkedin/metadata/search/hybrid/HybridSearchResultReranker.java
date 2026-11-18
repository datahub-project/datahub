package com.linkedin.metadata.search.hybrid;

import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
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

/**
 * Composes query embeddings, V3 kNN, and lexical-backed hybrid result reordering.
 *
 * <p>Only rows of the plan's eligible entity types (by default those that have vectors) are
 * reordered, and only among the positions those rows already hold. Other rows keep their lexical
 * positions, so keyword search keeps ranking across entity types. When every requested entity type
 * is eligible, all rows are reordered.
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

  /** Canonical names of the requested entity types whose rows can be reranked. */
  @Nonnull
  public Set<String> vectorEntityNames(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<String> entityNames) {
    return knnRequestBuilder.vectorEntityNames(opContext, entityNames);
  }

  /** The embedding profile id the query embedding service embeds for. */
  @Nonnull
  public String modelEmbeddingKey() {
    return queryEmbeddingService.getModelEmbeddingKey();
  }

  /** Whether the embedding provider answered in time at or after {@code nanos}, a nanoTime. */
  public boolean providerSucceededSince(final long nanos) {
    return queryEmbeddingService.providerSucceededSince(nanos);
  }

  /**
   * Returns {@code lexicalRows} with the rows of the plan's eligible entity types reordered by
   * combined lexical and vector score, each moved into a position such a row held before. Empty
   * when fewer than two rows got a vector score that passes the plan's floor, so the rows keep the
   * keyword ranking.
   *
   * @param deadlineNanos {@link System#nanoTime()} by which the embedding and kNN calls end
   */
  @Nonnull
  public Optional<List<SearchEntity>> rerank(
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<String> entityNames,
      @Nonnull final String query,
      @Nonnull final List<SearchEntity> lexicalRows,
      @Nonnull final Collection<String> fieldsToFetch,
      final long deadlineNanos,
      @Nonnull final HybridRerankPlan plan)
      throws IOException {
    final List<HybridCandidate> candidates =
        candidates(opContext, entityNames, query, lexicalRows, fieldsToFetch, deadlineNanos, plan);
    return candidates.isEmpty() ? Optional.empty() : Optional.of(reorder(lexicalRows, candidates));
  }

  /**
   * Scores the plan's eligible rows that have vectors, highest combined score first. The kNN query
   * scores only these rows. Empty when the query is a wildcard or fewer than two rows are eligible,
   * in which case no embedding or kNN request is made, and when fewer than two of them have a
   * vector that passes the plan's floor.
   *
   * @throws UncheckedTimeoutException when the deadline passes before the kNN call
   * @throws IOException when the kNN call fails or its response may be missing hits
   */
  @Nonnull
  public List<HybridCandidate> candidates(
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<String> entityNames,
      @Nonnull final String query,
      @Nonnull final List<SearchEntity> lexicalRows,
      @Nonnull final Collection<String> fieldsToFetch,
      final long deadlineNanos,
      @Nonnull final HybridRerankPlan plan)
      throws IOException {
    if (query.isBlank() || "*".equals(query) || lexicalRows.isEmpty()) {
      return List.of();
    }
    final Set<String> eligible = plan.eligibleEntityNames();
    final Map<Urn, Double> lexicalScores = scoreMapBuilder.lexicalScores(lexicalRows);
    lexicalScores.keySet().removeIf(urn -> !eligible.contains(urn.getEntityType()));
    // A single row has no other position to move into
    if (lexicalScores.size() < 2) {
      return List.of();
    }

    final HybridQueryEmbeddingService.QueryEmbedding queryEmbedding =
        queryEmbeddingService.embed(query, deadlineNanos);
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new UncheckedTimeoutException("The hybrid deadline passed before the kNN call");
    }
    // Only eligible types' indices are read. A type the plan leaves out, e.g. one a template's
    // entity gates exclude, may have no vector mapping, and a failed shard fails the kNN read
    final Optional<KnnSearchRequest> knnRequest =
        knnRequestBuilder.build(
            opContext,
            eligible,
            queryEmbedding.modelEmbeddingKey(),
            queryEmbedding.vector(),
            lexicalScores.keySet(),
            fieldsToFetch,
            Duration.ofNanos(remainingNanos));
    if (knnRequest.isEmpty()) {
      return List.of();
    }

    final KnnSearchResponse knnResponse =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3)
            .searchKnn(opContext, knnRequest.get());
    if (knnResponse.partial()) {
      // Timed-out or failed shards may omit hits, and a row without one would be taken for a row
      // without vectors, so the search counts the response as a failure
      count(opContext, "hybridReadPartial");
      throw new IOException("The kNN response reported a timed-out or failed shard");
    }
    final Map<Urn, Double> vectorScores = scoreMapBuilder.vectorScores(knnResponse);
    // A row without vectors, e.g. not embedded yet, has nothing to compare and keeps its position
    lexicalScores.keySet().retainAll(vectorScores.keySet());
    final Double floor = plan.blendFloor();
    if (floor != null) {
      // A row under the floor is not blended and keeps its position; the floor never drops a row
      lexicalScores.keySet().removeIf(urn -> vectorScores.get(urn) < floor);
    }
    if (lexicalScores.size() < 2) {
      // e.g. the V3 document index has few embeddings yet: the embedding and kNN calls bought
      // nothing
      count(opContext, "hybridReadNoVectors");
      return List.of();
    }
    return (plan.merger() == null ? candidateMerger : plan.merger())
        .merge(lexicalScores, vectorScores);
  }

  private static void count(@Nonnull final OperationContext opContext, @Nonnull String metric) {
    opContext
        .getMetricUtils()
        .ifPresent(m -> m.increment(HybridSearchResultReranker.class, metric, 1));
  }

  /**
   * Fills the positions of the rows that have a candidate with those rows in candidate order; every
   * other row keeps its position. A moved row takes the score of the position it moves into, so the
   * rows stay in score order for callers that sort by score. A URN listed twice keeps its later
   * rows where they are.
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
    final List<Double> slotScores = new ArrayList<>(slots.size());
    slots.forEach(index -> slotScores.add(rows.get(index).getScore()));
    final List<SearchEntity> reordered = new ArrayList<>(rows);
    final Iterator<Integer> slot = slots.iterator();
    final Iterator<Double> slotScore = slotScores.iterator();
    for (HybridCandidate candidate : candidates) {
      final SearchEntity row = candidateRows.get(candidate.entity());
      if (row != null) {
        final Double score = slotScore.next();
        if (score != null) {
          row.setScore(score);
        }
        reordered.set(slot.next(), row);
      }
    }
    return reordered;
  }
}
