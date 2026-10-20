package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryIntent;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryUnderstanding;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Per-query values the template body branches on. The body does not compute them. */
public final class SearchTemplateQueryShape {
  private static final Pattern STORAGE =
      Pattern.compile("^(s3|gs|hdfs):\\\\?/\\\\?/", Pattern.CASE_INSENSITIVE);
  private static final Pattern DATAHUB_URN =
      Pattern.compile("^urn:li:[a-z][a-z0-9]*[:()].*", Pattern.CASE_INSENSITIVE);
  private static final Pattern LONG_DIGITS = Pattern.compile("\\d{6,}");
  private static final Pattern DELIMITER = Pattern.compile("[_\\-./:]+");

  public final String raw;
  public final String escaped;

  /** Escaped text before letter/digit splitting. */
  public final String unsplit;

  /** Escaped text with a space at each letter/digit boundary. */
  public final String normalized;

  public final String fuzzyQuery;
  public final String lightQueryText;
  public final String shortened;
  public final String lastSegment;
  public final boolean multiWord;
  public final boolean longQuery;
  public final boolean dotted;
  public final boolean fullyQualifiedName;
  public final boolean deepPath;
  public final boolean longExactName;
  public final boolean operatorEscaped;
  public final boolean alphanumericSplit;
  public final boolean nameOnly;
  public final String wildcardTerm;
  public final String wholeIdentifier;
  public final String letterDigitQuery;
  public final String fqnQuery;
  public final List<String> synonyms;

  private SearchTemplateQueryShape(
      String raw,
      String escaped,
      String unsplit,
      String normalized,
      String fuzzyQuery,
      String lightQueryText,
      String shortened,
      String lastSegment,
      boolean multiWord,
      boolean longQuery,
      boolean dotted,
      boolean fullyQualifiedName,
      boolean deepPath,
      boolean longExactName,
      boolean operatorEscaped,
      boolean alphanumericSplit,
      boolean nameOnly,
      String wildcardTerm,
      String wholeIdentifier,
      String letterDigitQuery,
      String fqnQuery,
      List<String> synonyms) {
    this.raw = raw;
    this.escaped = escaped;
    this.unsplit = unsplit;
    this.normalized = normalized;
    this.fuzzyQuery = fuzzyQuery;
    this.lightQueryText = lightQueryText;
    this.shortened = shortened;
    this.lastSegment = lastSegment;
    this.multiWord = multiWord;
    this.longQuery = longQuery;
    this.dotted = dotted;
    this.fullyQualifiedName = fullyQualifiedName;
    this.deepPath = deepPath;
    this.longExactName = longExactName;
    this.operatorEscaped = operatorEscaped;
    this.alphanumericSplit = alphanumericSplit;
    this.nameOnly = nameOnly;
    this.wildcardTerm = wildcardTerm;
    this.wholeIdentifier = wholeIdentifier;
    this.letterDigitQuery = letterDigitQuery;
    this.fqnQuery = fqnQuery;
    this.synonyms = synonyms;
  }

  @Nonnull
  public static SearchTemplateQueryShape of(@Nullable String query) {
    return of(query, 5);
  }

  @Nonnull
  public static SearchTemplateQueryShape of(@Nullable String query, int wildcardMinimumLength) {
    String raw = query == null ? "" : query.trim();
    String plain = escapeOperators(raw);
    String unsplit = plain.replaceAll("\\s+", " ").trim();
    String normalized =
        V3SearchQueryBuilder.splitAlphanumericTokens(plain).replaceAll("\\s+", " ").trim();
    String[] tokens = normalized.isEmpty() ? new String[0] : normalized.split(" ");
    String fuzzy = normalized.isEmpty() ? "" : V3SearchQueryBuilder.makeFuzzyQuery(normalized);
    boolean letterDigit = !normalized.equals(unsplit);
    int dots = 0;
    for (int i = 0; i < raw.length(); i++) {
      if (raw.charAt(i) == '.') {
        dots++;
      }
    }
    String[] words = raw.isEmpty() ? new String[0] : raw.split("\\s+");
    String wildcard = null;
    if (words.length == 1
        && words[0].length() >= Math.max(wildcardMinimumLength, 1)
        && !raw.startsWith("\"")) {
      wildcard = escapeWildcard(words[0].toLowerCase(Locale.ROOT));
    }
    String fqnQuery =
        dots >= 2 ? raw.trim().replace('.', ' ').replaceAll("\\s+", " ").trim() : null;
    boolean delimitedIdentifier =
        !raw.contains(" ") && (raw.indexOf('_') >= 0 || raw.indexOf('-') >= 0);
    return new SearchTemplateQueryShape(
        raw,
        plain,
        unsplit,
        normalized,
        fuzzy,
        cutAtWord(normalized, 80),
        cutAtWord(unsplit, 80),
        lastSegment(raw),
        tokens.length >= 2,
        tokens.length >= 4,
        raw.indexOf('.') >= 0,
        dots >= 2,
        dots >= 3,
        !raw.contains(" ") && delimiterParts(raw) >= 5,
        !plain.equals(raw),
        letterDigit,
        words.length <= 1,
        wildcard,
        letterDigit || delimitedIdentifier ? raw.trim() : null,
        letterDigit ? normalized : null,
        fqnQuery,
        SearchTemplateSynonyms.expansions(raw));
  }

  private static int delimiterParts(@Nonnull String text) {
    int parts = 1;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '.' || c == '_' || c == '-' || c == '/') {
        parts++;
      }
    }
    return parts;
  }

  /**
   * A DataHub URN ({@code urn:li:} plus an entity type) or a storage path. {@code urn:} alone is
   * not an identity lookup.
   */
  public static boolean isIdentity(@Nullable String query) {
    if (query == null) {
      return false;
    }
    String text = query.trim();
    return DATAHUB_URN.matcher(text).matches() || STORAGE.matcher(text).find();
  }

  /**
   * A single exact-name token that splits at a letter/digit boundary, such as {@code orders2017}. A
   * multi-word query that contains such a token is not this shape.
   */
  public boolean exactNameLetterDigit() {
    return alphanumericSplit && QueryUnderstanding.understand(raw) == QueryIntent.EXACT_NAME;
  }

  /** A spaceless id or a long delimited name stops the light cascade on an empty total. */
  public static boolean stopOnEmptyLight(@Nullable String query) {
    if (query == null) {
      return false;
    }
    String trimmed = query.trim();
    if (trimmed.chars().anyMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c))) {
      return false;
    }
    if (LONG_DIGITS.matcher(trimmed).find()) {
      return true;
    }
    return Arrays.stream(DELIMITER.split(trimmed)).filter(part -> !part.isEmpty()).count() >= 4;
  }

  @Nonnull
  private static String escapeWildcard(@Nonnull String value) {
    StringBuilder out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' || c == '*' || c == '?') {
        out.append('\\');
      }
      out.append(c);
    }
    return out.toString();
  }

  /**
   * Backslash first, then the operator. A copied {@code \} would consume the escape added to the
   * next character, so {@code foo\*} would reach simple_query_string as a literal backslash and a
   * live {@code *}.
   */
  @Nonnull
  private static String escapeOperators(@Nonnull String value) {
    StringBuilder out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' || c == '+' || c == '|' || c == '-' || c == '*' || c == '(' || c == ')'
          || c == '~' || c == '"') {
        out.append('\\');
      }
      out.append(c);
    }
    return out.toString();
  }

  @Nonnull
  private static String cutAtWord(@Nonnull String value, int limit) {
    if (value.length() <= limit) {
      return value;
    }
    int cut = value.lastIndexOf(' ', limit);
    if (cut <= 0) {
      return value.substring(0, limit);
    }
    return value.substring(0, cut);
  }

  @Nonnull
  private static String lastSegment(@Nonnull String value) {
    if (!value.regionMatches(true, 0, "urn:", 0, 4)) {
      int slash = value.lastIndexOf('/');
      if (slash >= 0 && slash < value.length() - 1) {
        return value.substring(slash + 1);
      }
    }
    int open = value.lastIndexOf('(');
    int close = value.lastIndexOf(')');
    if (open >= 0 && close > open) {
      String[] parts = value.substring(open + 1, close).split(",");
      if (parts.length >= 2) {
        return parts[1].trim();
      }
    }
    int colon = value.lastIndexOf(':');
    if (colon < 0 || colon == value.length() - 1) {
      return value;
    }
    return value.substring(colon + 1);
  }
}
