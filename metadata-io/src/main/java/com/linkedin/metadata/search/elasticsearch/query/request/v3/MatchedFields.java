package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DATAHUB_STOP_WORDS_LIST;

import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.search.MatchedField;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.tartarus.snowball.ext.EnglishStemmer;

/**
 * The fields a Search V3 hit matched on ("Matched on" in the UI), found after the query from the
 * document's own root values. The engine cannot highlight the shared {@code _search} fields the
 * query reads: {@code copy_to} fills them, so they are not in {@code _source}, and storing them
 * would cost every write. A field matches when one of its words stems to the same word as a word of
 * the query, as the shared fields' {@code stemmed} subfields do, or starts with the last word of
 * the query, as their phrase prefix does; an identifier such as {@code customer_id} also matches by
 * its parts, as the analyzers index them. This is best effort: synonyms are not applied, words are
 * split on what is not a letter, a digit or an underscore even when a main tokenizer is configured,
 * and as on V2 only the first matching value of a field is reported.
 *
 * <p>One instance serves one query, for the hits of one response, from one thread.
 */
public final class MatchedFields {

  // Letters and digits in any script, as the shared fields' tokenizer splits them, the accents of
  // decomposed letters, which belong to their word, and underscores, which join an identifier
  private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}\\p{M}_]+");
  private static final Pattern UNDERSCORES = Pattern.compile("_+");
  // Longer values are cut to this many characters around the first match, about what the V2
  // highlighter returns
  private static final int MAX_VALUE_LENGTH = 200;
  // The rules of the stem_override filter, which the stemmed analyzers apply before snowball
  private static final String STEM_OVERRIDE_RULES = "elasticsearch/stem_override.txt";
  private static final Map<String, String> STEM_OVERRIDES = stemOverrides();

  private final String query;
  private final int minWordLength;
  private final EnglishStemmer stemmer = new EnglishStemmer();
  private final Set<String> queryStems;
  // The forms of the query's last word, which match as a prefix
  private final List<String> lastQueryForms;

  /**
   * @param minWordLength words shorter than this are dropped, as the analyzers' {@code min_length}
   *     filter drops them
   */
  public MatchedFields(@Nonnull final String query, final int minWordLength) {
    this.query = query;
    this.minWordLength = minWordLength;
    final List<List<String>> queryWords = new ArrayList<>();
    final Matcher matcher = WORD.matcher(query);
    while (matcher.find()) {
      final List<String> forms = forms(matcher.group());
      if (!forms.isEmpty()) {
        queryWords.add(forms);
      }
    }
    this.queryStems =
        queryWords.stream().flatMap(List::stream).map(this::stem).collect(Collectors.toSet());
    this.lastQueryForms = queryWords.isEmpty() ? List.of() : queryWords.get(queryWords.size() - 1);
  }

  /** The fields among {@code fields}, in that order, whose value in {@code source} matches. */
  @Nonnull
  public List<MatchedField> find(
      @Nonnull final Map<String, Object> source, @Nonnull final Collection<String> fields) {
    final List<MatchedField> matchedFields = new ArrayList<>();
    if (lastQueryForms.isEmpty()) {
      return matchedFields;
    }
    for (String field : fields) {
      final List<?> fieldValues = values(at(source, field));
      if (StructuredPropertyUtils.isStructuredPropertyField(field)) {
        addStrongest(matchedFields, field, fieldValues);
        continue;
      }
      for (Object value : fieldValues) {
        final String text = String.valueOf(value);
        final int match = firstMatch(text);
        if (match >= 0) {
          matchedFields.add(new MatchedField().setName(field).setValue(fragment(text, match)));
          break;
        }
      }
    }
    return matchedFields;
  }

  /**
   * The array value that matches most closely. {@code copy_to} indexes every value, and the stored
   * destination field does not hold them, so the evidence is chosen here.
   */
  private void addStrongest(
      @Nonnull final List<MatchedField> matchedFields,
      @Nonnull final String field,
      @Nonnull final List<?> fieldValues) {
    String bestText = null;
    int bestMatch = -1;
    int bestRank = Integer.MAX_VALUE;
    for (Object value : fieldValues) {
      final String text = String.valueOf(value);
      final int match = firstMatch(text);
      if (match < 0) {
        continue;
      }
      final int rank = matchRank(text, match);
      if (rank < bestRank) {
        bestRank = rank;
        bestMatch = match;
        bestText = text;
      }
    }
    if (bestText != null) {
      matchedFields.add(new MatchedField().setName(field).setValue(fragment(bestText, bestMatch)));
    }
  }

  /** Exact, then a value that starts with the query, then the earliest word match. */
  private int matchRank(@Nonnull final String text, final int match) {
    final String trimmed = query.trim();
    if (text.equalsIgnoreCase(trimmed)) {
      return 0;
    }
    if (!trimmed.isEmpty() && text.regionMatches(true, 0, trimmed, 0, trimmed.length())) {
      return 1;
    }
    return 2 + match;
  }

  /** A dotted field is either a flat key or a path through nested objects. */
  @Nullable
  private static Object at(@Nonnull final Map<String, Object> source, @Nonnull final String field) {
    if (source.containsKey(field)) {
      return source.get(field);
    }
    Object current = source;
    for (String part : field.split("\\.", -1)) {
      if (!(current instanceof Map<?, ?> map) || !map.containsKey(part)) {
        return null;
      }
      current = map.get(part);
    }
    return current;
  }

  /** Whether the query has a word a field could match. */
  public boolean hasQueryWords() {
    return !lastQueryForms.isEmpty();
  }

  /**
   * Where the first word of a value that matches the query starts, or -1, read word by word so that
   * a long value is scanned only up to its first match.
   */
  private int firstMatch(@Nonnull final String text) {
    final Matcher matcher = WORD.matcher(text);
    while (matcher.find()) {
      for (String form : forms(matcher.group())) {
        if (lastQueryForms.stream().anyMatch(form::startsWith) || queryStems.contains(stem(form))) {
          return matcher.start();
        }
      }
    }
    return -1;
  }

  /**
   * The forms the analyzers keep of one word: the word, with case and accents folded, and for an
   * identifier its parts around the underscores, without short words and stop words.
   */
  @Nonnull
  private List<String> forms(@Nonnull final String word) {
    final String folded = fold(word);
    final List<String> forms = new ArrayList<>();
    if (isKept(folded)) {
      forms.add(folded);
    }
    if (folded.indexOf('_') >= 0) {
      for (String part : UNDERSCORES.split(folded)) {
        if (isKept(part) && !forms.contains(part)) {
          forms.add(part);
        }
      }
    }
    return forms;
  }

  /**
   * Folds a word as the analyzers do: ASCII folding, which keeps combining marks, then lower case
   * one code point at a time.
   */
  @Nonnull
  private static String fold(@Nonnull final String word) {
    final char[] chars = word.toCharArray();
    // A character folds to at most four
    final char[] folded = new char[chars.length * 4];
    final int length = ASCIIFoldingFilter.foldToASCII(chars, 0, folded, 0, chars.length);
    return new String(folded, 0, length)
        .codePoints()
        .map(Character::toLowerCase)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  private boolean isKept(@Nonnull final String word) {
    return word.length() >= Math.max(1, minWordLength)
        && !DATAHUB_STOP_WORDS_LIST.contains(word)
        && !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(word);
  }

  @Nonnull
  private String stem(@Nonnull final String word) {
    final String override = STEM_OVERRIDES.get(word);
    if (override != null) {
      return override;
    }
    stemmer.setCurrent(word);
    stemmer.stem();
    return stemmer.getCurrent();
  }

  /**
   * The stem of each word a stem_override rule such as {@code customers, customer => customer}
   * names.
   */
  @Nonnull
  private static Map<String, String> stemOverrides() {
    // Shipped with this class; the index analyzers cannot be built without it either
    final InputStream rules =
        Objects.requireNonNull(
            MatchedFields.class.getClassLoader().getResourceAsStream(STEM_OVERRIDE_RULES),
            STEM_OVERRIDE_RULES);
    final Map<String, String> overrides = new HashMap<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(rules, StandardCharsets.UTF_8))) {
      reader
          .lines()
          .map(line -> line.trim().toLowerCase(Locale.ROOT))
          .filter(line -> !line.startsWith("#") && line.contains("=>"))
          .forEach(
              line -> {
                final int arrow = line.indexOf("=>");
                final String stem = line.substring(arrow + 2).trim();
                for (String word : line.substring(0, arrow).split(",")) {
                  // The first rule naming a word wins, as in the engine's filter
                  overrides.putIfAbsent(word.trim(), stem);
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Map.copyOf(overrides);
  }

  @Nonnull
  private static List<?> values(@Nullable final Object value) {
    if (value == null) {
      return List.of();
    }
    return value instanceof List<?> list ? list : List.of(value);
  }

  @Nonnull
  private static String fragment(@Nonnull final String text, final int match) {
    if (text.length() <= MAX_VALUE_LENGTH) {
      return text;
    }
    final int start =
        Math.max(0, Math.min(match - MAX_VALUE_LENGTH / 4, text.length() - MAX_VALUE_LENGTH));
    return text.substring(start, start + MAX_VALUE_LENGTH);
  }
}
