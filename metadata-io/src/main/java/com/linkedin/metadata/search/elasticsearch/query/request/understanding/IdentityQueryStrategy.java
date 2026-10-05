package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Tier 1: focused clauses for identity lookups (URNs and S3/GCS/HDFS paths) on the urn,
 * qualifiedName, name and id fields. SearchQueryBuilder ORs them with the all-terms query, which
 * matches URNs that differ in case or are cut short.
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

    // Phrase match on urn. The V3 urn field is a keyword, so this only matches the whole URN and
    // adds to the term match's score
    boolQuery.should(QueryBuilders.matchPhraseQuery("urn", trimmed).boost(URN_PHRASE_BOOST));

    // Phrase match on qualifiedName — for FQN and S3 path queries
    boolQuery.should(
        QueryBuilders.matchPhraseQuery("qualifiedName", trimmed).boost(QUALIFIED_NAME_BOOST));

    // Exact name match on the last dotted segment, e.g. "orders" for a URN cut off after
    // "my_db.orders". FQN queries take the general query instead
    String lastSegment =
        trimmed.contains(".") ? trimmed.substring(trimmed.lastIndexOf('.') + 1) : trimmed;
    if (!lastSegment.isEmpty() && !lastSegment.equals(trimmed)) {
      boolQuery.should(
          QueryBuilders.termQuery("name.keyword", lastSegment)
              .caseInsensitive(true)
              .boost(NAME_KEYWORD_BOOST));
    }

    // Exact ID match
    boolQuery.should(
        QueryBuilders.termQuery("id.keyword", trimmed)
            .caseInsensitive(true)
            .boost(ID_KEYWORD_BOOST));

    boolQuery.minimumShouldMatch(1);
    return boolQuery;
  }
}
