package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Per-query values the template body branches on. The body does not compute them. */
public final class SearchTemplateQueryShape {
  private static final Pattern LETTER_DIGIT =
      Pattern.compile("(?<=\\p{L})(?=\\p{N})|(?<=\\p{N})(?=\\p{L})");
  private static final Pattern STORAGE =
      Pattern.compile("^(s3|gs|hdfs)://", Pattern.CASE_INSENSITIVE);
  private static final Pattern LONG_DIGITS = Pattern.compile("\\d{6,}");

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
    String raw = query == null ? "" : query.trim();
    String plain = escapeOperators(raw);
    String unsplit = plain.replaceAll("\\s+", " ").trim();
    String normalized = splitLetterDigit(plain).replaceAll("\\s+", " ").trim();
    String[] tokens = normalized.isEmpty() ? new String[0] : normalized.split(" ");
    StringBuilder fuzzy = new StringBuilder();
    for (int i = 0; i < tokens.length; i++) {
      if (i > 0) {
        fuzzy.append(' ');
      }
      fuzzy.append(tokens[i]).append(fuzzySuffix(tokens[i]));
    }
    boolean letterDigit = !normalized.equals(unsplit);
    int dots = 0;
    for (int i = 0; i < raw.length(); i++) {
      if (raw.charAt(i) == '.') {
        dots++;
      }
    }
    String[] words = raw.isEmpty() ? new String[0] : raw.split("\\s+");
    String wildcard = null;
    if (words.length == 1 && words[0].length() >= 5 && !raw.startsWith("\"")) {
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
        fuzzy.toString(),
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

  public static boolean isIdentity(@Nullable String query) {
    if (query == null) {
      return false;
    }
    String text = query.trim();
    return text.regionMatches(true, 0, "urn:", 0, 4) || STORAGE.matcher(text).find();
  }

  /** A spaceless id or a long delimited name stops the light cascade on an empty total. */
  public static boolean stopOnEmptyLight(@Nullable String query) {
    if (query == null || query.indexOf(' ') >= 0) {
      return false;
    }
    String text = query.trim();
    if (LONG_DIGITS.matcher(text).find()) {
      return true;
    }
    int parts = 1;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '.' || c == '_' || c == '-' || c == '/') {
        parts++;
      }
    }
    return parts >= 4;
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

  @Nonnull
  private static String escapeOperators(@Nonnull String value) {
    StringBuilder out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '+' || c == '|' || c == '-' || c == '*' || c == '(' || c == ')' || c == '~'
          || c == '"') {
        out.append('\\');
      }
      out.append(c);
    }
    return out.toString();
  }

  @Nonnull
  private static String splitLetterDigit(@Nonnull String value) {
    return LETTER_DIGIT.matcher(value).replaceAll(" ");
  }

  @Nonnull
  private static String fuzzySuffix(@Nonnull String token) {
    int length = token.length();
    if (length <= 4) {
      return "";
    }
    if (length <= 6) {
      return "~1";
    }
    return "~2";
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
