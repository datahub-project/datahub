package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * V2.5 Query Understanding layer. Analyzes the raw query string to determine user intent and
 * normalize synonym variants to canonical forms.
 *
 * <p>Classification and normalization are cheap — string prefix checks, regex, and map lookups —
 * and run once per query. Rules are evaluated in order; first match wins.
 */
public final class QueryUnderstanding {

  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  /** FQN pattern: dot-separated segments, each segment is word chars (plus optional wildcards). */
  private static final Pattern FQN_PATTERN = Pattern.compile("^[\\w.*]+\\.[\\w.*]+$");

  /** EXACT_NAME pattern: word chars and hyphens, no spaces, no dots, no query operators. */
  private static final Pattern EXACT_NAME_PATTERN = Pattern.compile("^[\\w][\\w-]*$");

  /** Result of query analysis: intent classification + normalized query text. */
  public static final class Result {
    private final QueryIntent intent;
    private final String normalizedQuery;

    Result(@Nonnull final QueryIntent intent, @Nonnull final String normalizedQuery) {
      this.intent = intent;
      this.normalizedQuery = normalizedQuery;
    }

    @Nonnull
    public QueryIntent intent() {
      return intent;
    }

    /** Query with synonym variants replaced by canonical forms. */
    @Nonnull
    public String normalizedQuery() {
      return normalizedQuery;
    }
  }

  private QueryUnderstanding() {}

  /**
   * Full query analysis: classify intent and normalize synonym variants.
   *
   * @param query the raw user query string
   * @param synonymMap bidirectional synonym map (term → set of equivalents), may be null
   * @return analysis result with intent and normalized query
   */
  @Nonnull
  public static Result analyze(
      @Nonnull final String query, @Nullable final Map<String, Set<String>> synonymMap) {
    QueryIntent intent = understand(query);
    String normalized = normalizeSynonyms(query, synonymMap);
    return new Result(intent, normalized);
  }

  /**
   * Normalize synonym variants in the query to canonical forms. Each whitespace-separated token is
   * looked up in the synonym map; if found, it's replaced with the canonical form (alphabetically
   * first synonym in the equivalence group). This ensures "monetisation" and "monetization" produce
   * identical ES queries and identical scoring.
   *
   * <p>Tokens not in the synonym map are left unchanged. The canonical form is deterministic
   * (sorted first) so normalization is idempotent.
   */
  @Nonnull
  public static String normalizeSynonyms(
      @Nonnull final String query, @Nullable final Map<String, Set<String>> synonymMap) {
    if (synonymMap == null || synonymMap.isEmpty() || query.trim().isEmpty()) {
      return query;
    }
    String[] tokens = WHITESPACE.split(query);
    boolean changed = false;
    for (int i = 0; i < tokens.length; i++) {
      Set<String> synonyms = synonymMap.get(tokens[i].toLowerCase());
      if (synonyms != null && synonyms.size() > 1) {
        // Only normalize spelling variants (e.g., "monetisation"/"monetization"), not semantic
        // synonyms such as abbreviations, expansions or related names ("glue"/"athena"): every
        // synonym in the group must be a single word within two edits of the canonical form, the
        // alphabetically first synonym. Multi-word synonyms are always semantic.
        String canonical = null;
        boolean allSingleWord = true;
        for (String s : synonyms) {
          if (s.contains(" ")) {
            allSingleWord = false;
            break;
          }
          if (canonical == null || s.compareTo(canonical) < 0) {
            canonical = s;
          }
        }
        final String canonicalForm = canonical;
        boolean isSpellingVariant =
            allSingleWord
                && canonicalForm != null
                && synonyms.stream()
                    .allMatch(s -> StringUtils.getLevenshteinDistance(s, canonicalForm, 2) >= 0);

        if (isSpellingVariant && canonical != null) {
          if (!tokens[i].equalsIgnoreCase(canonical)) {
            tokens[i] = canonical;
            changed = true;
          }
        }
      }
    }
    return changed ? String.join(" ", tokens) : query;
  }

  /**
   * Understand the intent behind a search query.
   *
   * @param query the raw user query string
   * @return the classified intent
   */
  @Nonnull
  public static QueryIntent understand(@Nonnull final String query) {
    String trimmed = query.trim();

    // Empty or wildcard — broad browsing intent
    if (trimmed.isEmpty() || "*".equals(trimmed)) {
      return QueryIntent.KEYWORD;
    }

    // Rule 1: Identity lookups — URN or cloud storage paths
    if (trimmed.startsWith("urn:li:")
        || trimmed.startsWith("s3://")
        || trimmed.startsWith("gs://")
        || trimmed.startsWith("hdfs://")) {
      return QueryIntent.IDENTITY;
    }

    // Rule 2: Quoted exact lookup — user wants a specific entity by name
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
        || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
      return QueryIntent.EXACT_NAME;
    }

    // Rule 3: FQN — dot-separated, no spaces (e.g., my_db.sales.orders)
    if (!trimmed.contains(" ") && FQN_PATTERN.matcher(trimmed).matches()) {
      return QueryIntent.FQN;
    }

    // Rule 4: Exact name — no spaces, no dots, alphanumeric/underscore/hyphen
    if (!trimmed.contains(" ") && EXACT_NAME_PATTERN.matcher(trimmed).matches()) {
      return QueryIntent.EXACT_NAME;
    }

    // Rule 5: Default — multi-word or complex queries
    return QueryIntent.KEYWORD;
  }
}
