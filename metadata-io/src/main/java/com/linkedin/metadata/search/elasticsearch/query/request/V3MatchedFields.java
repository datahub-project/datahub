package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DATAHUB_STOP_WORDS_LIST;

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
 * and as on V2 only the first matching value of a field is reported. {@link #forAutocomplete} reads
 * words as the autocomplete analyzer does instead.
 *
 * <p>One instance serves one query, for the hits of one response, from one thread.
 */
final class V3MatchedFields {

  // Letters and digits in any script, as the shared fields' tokenizer splits them, the accents of
  // decomposed letters, which belong to their word, and underscores, which join an identifier
  private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}\\p{M}_]+");
  // The words of the autocomplete analyzer (V2's partial analyzer), whose word delimiter keeps
  // hyphens inside a word too (V2LegacySettingsBuilder.WORD_DELIMITER_TYPE_TABLE)
  private static final Pattern AUTOCOMPLETE_WORD = Pattern.compile("[\\p{L}\\p{N}\\p{M}_-]+");
  private static final Pattern UNDERSCORES = Pattern.compile("_+");
  // Longer values are cut to this many characters around the first match, about what the V2
  // highlighter returns
  private static final int MAX_VALUE_LENGTH = 200;
  // The rules of the stem_override filter, which the stemmed analyzers apply before snowball
  private static final String STEM_OVERRIDE_RULES = "elasticsearch/stem_override.txt";
  private static final Map<String, String> STEM_OVERRIDES = stemOverrides();

  private final int minWordLength;
  // Words are read as the autocomplete analyzer reads them: whole, stop words kept, not stemmed
  private final boolean autocomplete;
  private final Pattern wordPattern;
  private final EnglishStemmer stemmer = new EnglishStemmer();
  private final Set<String> queryStems;
  // The forms of the query's last word, which match as a prefix
  private final List<String> lastQueryForms;

  /**
   * @param minWordLength words shorter than this are dropped, as the analyzers' {@code min_length}
   *     filter drops them
   */
  V3MatchedFields(@Nonnull final String query, final int minWordLength) {
    this(query, minWordLength, false);
  }

  private V3MatchedFields(
      @Nonnull final String query, final int minWordLength, final boolean autocomplete) {
    this.minWordLength = minWordLength;
    this.autocomplete = autocomplete;
    this.wordPattern = autocomplete ? AUTOCOMPLETE_WORD : WORD;
    final List<List<String>> queryWords = new ArrayList<>();
    final Matcher matcher = wordPattern.matcher(query);
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

  /**
   * For an autocomplete input, a prefix being typed, matched as the autocomplete analyzer reads
   * words: none is too short and no stop word is dropped, nothing is stemmed, and an identifier
   * such as {@code order_i} or {@code order-i} stays whole, in the input and in the values, since
   * its parts would be prefixes of unrelated words. This is best effort too: unlike the analyzer,
   * it reads the {@code s} of a possessive {@code 's} as a word of its own, which the analyzer
   * drops, it splits at a character that only folds to a hyphen or an underscore, such as an en
   * dash, and it keeps a hyphenated word whole even when a main tokenizer is configured.
   */
  @Nonnull
  static V3MatchedFields forAutocomplete(@Nonnull final String input) {
    return new V3MatchedFields(input, 0, true);
  }

  /** The fields among {@code fields}, in that order, whose value in {@code source} matches. */
  @Nonnull
  List<MatchedField> find(
      @Nonnull final Map<String, Object> source, @Nonnull final Collection<String> fields) {
    final List<MatchedField> matchedFields = new ArrayList<>();
    if (lastQueryForms.isEmpty()) {
      return matchedFields;
    }
    for (String field : fields) {
      for (Object value : values(source.get(field))) {
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

  /** Whether the query has a word a field could match. */
  boolean hasQueryWords() {
    return !lastQueryForms.isEmpty();
  }

  /**
   * Where the first word of a value that matches the query starts, or -1, read word by word so that
   * a long value is scanned only up to its first match.
   */
  private int firstMatch(@Nonnull final String text) {
    final Matcher matcher = wordPattern.matcher(text);
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
   * identifier its parts around the underscores, without short words and stop words. Autocomplete
   * keeps an identifier whole and keeps stop words.
   */
  @Nonnull
  private List<String> forms(@Nonnull final String word) {
    final String folded = fold(word);
    final List<String> forms = new ArrayList<>();
    if (isKept(folded)) {
      forms.add(folded);
    }
    if (!autocomplete && folded.indexOf('_') >= 0) {
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
        && (autocomplete
            || (!DATAHUB_STOP_WORDS_LIST.contains(word)
                && !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(word)));
  }

  @Nonnull
  private String stem(@Nonnull final String word) {
    if (autocomplete) {
      return word;
    }
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
            V3MatchedFields.class.getClassLoader().getResourceAsStream(STEM_OVERRIDE_RULES),
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
