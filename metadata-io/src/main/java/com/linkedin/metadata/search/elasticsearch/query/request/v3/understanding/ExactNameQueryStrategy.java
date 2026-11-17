package com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Tier 2: Name-focused query for short, exact name searches. Targets name, title, qualifiedName,
 * id, displayName (~5 fields) with exact keyword boosts and synonym expansion. Expected latency:
 * ~20ms.
 */
public class ExactNameQueryStrategy implements QueryStrategy {

  private static final String[] MATCH_FIELDS = {
    "name", "title", "qualifiedName", "id", "displayName"
  };

  private static final String[] KEYWORD_FIELDS = {"name.keyword", "title.keyword", "id.keyword"};

  private static final float EXACT_BOOST = 100.0f;
  private static final float MATCH_BOOST = 10.0f;
  private static final float SYNONYM_BOOST = 8.0f;

  @Override
  public int tier() {
    return 2;
  }

  @Nonnull
  @Override
  public String name() {
    return "EXACT_NAME";
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

    // Exact keyword matches (highest priority)
    for (String field : KEYWORD_FIELDS) {
      boolQuery.should(QueryBuilders.termQuery(field, trimmed).boost(EXACT_BOOST));
    }

    // Match queries on name-related fields
    for (String field : MATCH_FIELDS) {
      boolQuery.should(QueryBuilders.matchQuery(field, trimmed).boost(MATCH_BOOST));
    }

    // Synonym expansion
    if (synonymMap != null) {
      Set<String> synonyms = synonymMap.get(trimmed.toLowerCase());
      if (synonyms != null) {
        for (String synonym : synonyms) {
          if (!synonym.equalsIgnoreCase(trimmed)) {
            for (String field : MATCH_FIELDS) {
              boolQuery.should(QueryBuilders.matchQuery(field, synonym).boost(SYNONYM_BOOST));
            }
            for (String field : KEYWORD_FIELDS) {
              boolQuery.should(QueryBuilders.termQuery(field, synonym).boost(SYNONYM_BOOST));
            }
          }
        }
      }
    }

    boolQuery.minimumShouldMatch(1);
    return boolQuery;
  }
}
