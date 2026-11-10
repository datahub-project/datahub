package com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Tier 1: Focused query for identity lookups (URN, S3/GCS/HDFS paths) and FQN navigation. Targets
 * only urn, qualifiedName, name, and id fields — 4 fields instead of ~47. Expected latency: ~5ms.
 */
public class IdentityQueryStrategy implements QueryStrategy {

  private static final float URN_EXACT_BOOST = 100.0f;
  private static final float URN_PHRASE_BOOST = 50.0f;
  private static final float QUALIFIED_NAME_BOOST = 30.0f;
  private static final float NAME_KEYWORD_BOOST = 20.0f;
  private static final float ID_KEYWORD_BOOST = 10.0f;

  @Override
  public int tier() {
    return 1;
  }

  @Nonnull
  @Override
  public String name() {
    return "IDENTITY";
  }

  @Nullable
  @Override
  public QueryBuilder buildQuery(
      @Nonnull final String query, @Nullable final Map<String, Set<String>> synonymMap) {
    String trimmed = query.trim();
    if (trimmed.isEmpty()) {
      return null;
    }

    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

    // Exact URN match — term query on keyword field (highest priority)
    boolQuery.should(QueryBuilders.termQuery("urn", trimmed).boost(URN_EXACT_BOOST));

    // Phrase match on urn (analyzed) — catches partial URN / path prefix matches
    boolQuery.should(QueryBuilders.matchPhraseQuery("urn", trimmed).boost(URN_PHRASE_BOOST));

    // Whole qualifiedName match — for FQN and S3 path queries. The V3 root keeps the stored casing.
    boolQuery.should(
        QueryBuilders.termQuery("qualifiedName", trimmed)
            .caseInsensitive(true)
            .boost(QUALIFIED_NAME_BOOST));

    // Exact name match — for FQN last segment (e.g., "plan" in "dwh_core.billing.plan")
    String lastSegment =
        trimmed.contains(".") ? trimmed.substring(trimmed.lastIndexOf('.') + 1) : trimmed;
    if (!lastSegment.isEmpty() && !lastSegment.equals(trimmed)) {
      boolQuery.should(
          QueryBuilders.termQuery("name", lastSegment)
              .caseInsensitive(true)
              .boost(NAME_KEYWORD_BOOST));
    }

    // Exact ID match
    boolQuery.should(
        QueryBuilders.termQuery("id", trimmed).caseInsensitive(true).boost(ID_KEYWORD_BOOST));

    boolQuery.minimumShouldMatch(1);
    return boolQuery;
  }
}
