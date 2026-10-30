package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;

import java.util.Arrays;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Clauses one index may add for the per-term queries. Lucene counts every term, and every term a
 * fuzzy term expands to, against {@code indices.query.bool.max_clause_count} (1024 by default on
 * OpenSearch). A query keeps the words that fit and its fuzzy terms share what is left.
 */
@Slf4j
public final class SearchClauseBudget {
  public static final int CLAUSE_BUDGET = 850;

  /** Clauses per term outside the per-field queries (all-terms, description, fully qualified). */
  public static final int PER_TERM_EXTRA_CLAUSES = 6;

  public static final int MAX_FUZZY_EXPANSIONS = 10;

  /**
   * Terms a phrase prefix expands to per field when the query has several terms. A single term
   * keeps Elasticsearch's default of 50: for a word too short for fuzziness, the prefix is its only
   * partial match.
   */
  public static final int MAX_PREFIX_EXPANSIONS = 10;

  public static final int DEFAULT_PREFIX_EXPANSIONS = 50;

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
  private static final Pattern LETTER_DIGIT_BOUNDARY = Pattern.compile("(?<=[a-zA-Z])(?=\\d)");
  private static final Pattern DIGIT_LETTER_BOUNDARY = Pattern.compile("(?<=\\d)(?=[a-zA-Z])");
  private static final Pattern NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^\\p{L}\\p{N}]+");
  private static final Pattern FULLY_QUOTED_PATTERN = Pattern.compile("^[\"'].+[\"']$");
  private static final Pattern TERM_CHAR_PATTERN = Pattern.compile("[\\p{L}\\p{N}]");

  private SearchClauseBudget() {}

  /** The fields one index queries by default, outside and inside the word gram analyzers. */
  public record FieldCounts(int fields, int wordGramFields) {}

  /**
   * The leading words of {@code query} whose per-term clauses fit {@link #CLAUSE_BUDGET}. A first
   * word that does not fit alone keeps its leading terms. A fully quoted query keeps its quotes.
   */
  @Nonnull
  public static String firstTermsWithinBudget(@Nonnull String query, @Nonnull FieldCounts counts) {
    String trimmed = query.trim();
    boolean quoted = FULLY_QUOTED_PATTERN.matcher(trimmed).matches();
    String[] words =
        WHITESPACE_PATTERN.split(quoted ? trimmed.substring(1, trimmed.length() - 1) : trimmed);
    int terms = 0;
    int unsplitWords = 0;
    boolean runs = false;
    int kept = 0;
    for (String word : words) {
      String escaped = V3SearchQueryBuilder.escapeSimpleQueryStringOperators(word);
      int wordTerms = termCount(escaped);
      int wordUnsplit = runCount(escaped);
      boolean withRuns = runs || splitsLetterDigitRun(escaped);
      if (termClauses(terms + wordTerms, unsplitWords + wordUnsplit, withRuns, counts)
          > CLAUSE_BUDGET) {
        break;
      }
      terms += wordTerms;
      unsplitWords += wordUnsplit;
      runs = withRuns;
      kept++;
    }
    if (kept == words.length) {
      return query;
    }
    log.debug("Kept {} of {} words of the query within the clause budget", kept, words.length);
    String keptWords =
        kept == 0
            ? leadingTermsWithinBudget(words[0], counts)
            : String.join(" ", Arrays.copyOf(words, kept));
    return quoted
        ? trimmed.charAt(0) + keptWords + trimmed.charAt(trimmed.length() - 1)
        : keptWords;
  }

  /**
   * Expansions per fuzzy term that stay within {@link #CLAUSE_BUDGET}, or 0 for no fuzziness: a
   * quoted query, no term long enough to be fuzzy, or no room for one expansion each.
   */
  public static int fuzzyExpansions(
      @Nonnull String sanitizedQuery,
      @Nonnull String operatorEscaped,
      @Nonnull FieldCounts counts) {
    return fuzzyExpansions(
        sanitizedQuery,
        operatorEscaped,
        counts,
        V3SearchQueryBuilder.makeFuzzyQuery(sanitizedQuery));
  }

  /**
   * Expansions per fuzzy term in {@code fuzzyQuery}. The template path passes the fuzzy text it
   * already built from its normalization settings.
   */
  public static int fuzzyExpansions(
      @Nonnull String sanitizedQuery,
      @Nonnull String operatorEscaped,
      @Nonnull FieldCounts counts,
      @Nonnull String fuzzyQuery) {
    long fuzzyTerms = fuzzyQuery.chars().filter(c -> c == '~').count();
    if (isQuoted(sanitizedQuery) || fuzzyTerms == 0 || counts.fields() == 0) {
      return 0;
    }
    long room =
        CLAUSE_BUDGET
            - termClauses(operatorEscaped, counts)
            - (long) termCount(operatorEscaped) * counts.fields();
    if (room <= 0) {
      return 0;
    }
    return (int) Math.min(MAX_FUZZY_EXPANSIONS, 1 + room / (fuzzyTerms * counts.fields()));
  }

  /**
   * Synonym expansions that fit {@link #CLAUSE_BUDGET} after {@code operatorEscaped} is counted.
   * {@code requested} is already limited to the template maximum.
   */
  public static int clampedSynonymCap(
      int requested, @Nonnull String operatorEscaped, @Nonnull FieldCounts counts) {
    int bounded = Math.max(0, requested);
    if (counts.fields() <= 0) {
      return 0;
    }
    long room =
        CLAUSE_BUDGET
            - termClauses(operatorEscaped, counts)
            - (long) termCount(operatorEscaped) * counts.fields();
    if (room <= 0) {
      return 0;
    }
    return (int) Math.min(bounded, room / counts.fields());
  }

  /**
   * Clauses {@code operatorEscaped} is estimated to add for one index, using the same per-term
   * accounting as {@link #firstTermsWithinBudget}.
   */
  public static int estimatedTermClauses(
      @Nonnull String operatorEscaped, @Nonnull FieldCounts counts) {
    return termClauses(operatorEscaped, counts);
  }

  /** Phrase-prefix expansions: 10 once the query has more than one term, otherwise 50. */
  public static int prefixExpansions(@Nonnull String operatorEscaped) {
    return termCount(operatorEscaped) > 1 ? MAX_PREFIX_EXPANSIONS : DEFAULT_PREFIX_EXPANSIONS;
  }

  private static int runCount(@Nonnull String text) {
    return (int)
        Arrays.stream(NON_ALPHANUMERIC_PATTERN.split(text)).filter(term -> !term.isEmpty()).count();
  }

  private static int termCount(@Nonnull String text) {
    return runCount(V3SearchQueryBuilder.splitAlphanumericTokens(text));
  }

  private static int termClauses(
      int terms, int unsplitWords, boolean runs, @Nonnull FieldCounts counts) {
    return terms * (counts.fields() + counts.wordGramFields() + PER_TERM_EXTRA_CLAUSES)
        + (runs ? unsplitWords * counts.fields() : 0);
  }

  private static int termClauses(@Nonnull String operatorEscaped, @Nonnull FieldCounts counts) {
    return termClauses(
        termCount(operatorEscaped),
        runCount(operatorEscaped),
        splitsLetterDigitRun(operatorEscaped),
        counts);
  }

  @Nonnull
  private static String leadingTermsWithinBudget(
      @Nonnull String word, @Nonnull FieldCounts counts) {
    String kept = null;
    for (int i = word.offsetByCodePoints(0, 1);
        i < word.length();
        i = word.offsetByCodePoints(i, 1)) {
      String previous = Character.toString(word.codePointBefore(i));
      String next = Character.toString(word.codePointAt(i));
      boolean termEnds =
          TERM_CHAR_PATTERN.matcher(previous).matches()
              && (!TERM_CHAR_PATTERN.matcher(next).matches()
                  || splitsLetterDigitRun(previous + next));
      if (termEnds) {
        String prefix = word.substring(0, i);
        if (kept != null
            && termClauses(V3SearchQueryBuilder.escapeSimpleQueryStringOperators(prefix), counts)
                > CLAUSE_BUDGET) {
          break;
        }
        kept = prefix;
      }
    }
    return kept == null ? word : kept;
  }

  private static boolean splitsLetterDigitRun(@Nonnull String query) {
    return LETTER_DIGIT_BOUNDARY.matcher(query).find()
        || DIGIT_LETTER_BOUNDARY.matcher(query).find();
  }
}
