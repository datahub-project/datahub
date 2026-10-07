package com.linkedin.metadata.search.hybrid;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Converts lexical and kNN search results into score maps for hybrid candidate merging. */
@Slf4j
public class HybridScoreMapBuilder {

  private final HybridVectorScoreNormalizer vectorScoreNormalizer;

  public HybridScoreMapBuilder(@Nonnull final HybridVectorScoreNormalizer vectorScoreNormalizer) {
    this.vectorScoreNormalizer = vectorScoreNormalizer;
  }

  @Nonnull
  public Map<Urn, Double> lexicalScores(@Nonnull final List<SearchEntity> lexicalRows) {
    final Map<Urn, Double> lexicalScores = new LinkedHashMap<>();
    for (SearchEntity entity : lexicalRows) {
      if (entity.getEntity() != null) {
        lexicalScores.put(entity.getEntity(), entity.getScore());
      }
    }
    return lexicalScores;
  }

  @Nonnull
  public Map<Urn, Double> vectorScores(@Nonnull final KnnSearchResponse knnResponse) {
    final Map<Urn, Double> vectorScores = new LinkedHashMap<>();
    for (KnnSearchResponse.Hit hit : knnResponse.hits()) {
      urn(hit)
          .ifPresent(
              urn ->
                  vectorScores.merge(
                      urn,
                      vectorScoreNormalizer.normalize(hit.score()),
                      (existing, candidate) -> Math.max(existing, candidate)));
    }
    return vectorScores;
  }

  @Nonnull
  private static Optional<Urn> urn(@Nonnull final KnnSearchResponse.Hit hit) {
    final Object sourceUrn = hit.source().get("urn");
    if (sourceUrn instanceof String && !((String) sourceUrn).isBlank()) {
      return urn((String) sourceUrn);
    }
    return urn(hit.id()).or(() -> decode(hit.id()).flatMap(HybridScoreMapBuilder::urn));
  }

  @Nonnull
  private static Optional<String> decode(@Nonnull final String id) {
    try {
      return Optional.of(URLDecoder.decode(id, StandardCharsets.UTF_8));
    } catch (IllegalArgumentException e) {
      log.warn("Skipping hybrid vector hit with an undecodable id: {}", id);
      return Optional.empty();
    }
  }

  @Nonnull
  private static Optional<Urn> urn(@Nonnull final String rawUrn) {
    try {
      return Optional.of(Urn.createFromString(rawUrn));
    } catch (URISyntaxException e) {
      log.warn("Skipping malformed hybrid vector hit id: {}", rawUrn);
      return Optional.empty();
    }
  }
}
