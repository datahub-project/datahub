package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DATAHUB_STOP_WORDS_LIST;

import com.linkedin.metadata.search.MatchedField;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
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
final class V3MatchedFields {

  // Letters and digits in any script, as the shared fields' tokenizer splits them, the accents of
  // decomposed letters, which belong to their word, and underscores, which join an identifier
  private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}\\p{M}_]+");
  private static final Pattern UNDERSCORES = Pattern.compile("_+");
  private static final Pattern COMBINING_MARKS = Pattern.compile("\\p{M}+");
  // A word the query excludes with a leading minus
  private static final Pattern EXCLUDED_WORD = Pattern.compile("(^|\\s)-\\S+");
  // Longer values are cut to this many characters around the first match, about what the V2
  // highlighter returns
  private static final int MAX_VALUE_LENGTH = 200;

  private final int minWordLength;
  private final EnglishStemmer stemmer = new EnglishStemmer();
  private final Set<String> queryStems;
  // The forms of the query's last word, which match as a prefix
  private final List<String> lastQueryForms;

  /**
   * @param minWordLength words shorter than this are dropped, as the analyzers' {@code min_length}
   *     filter drops them
   */
  V3MatchedFields(@Nonnull final String query, final int minWordLength) {
    this.minWordLength = minWordLength;
    final List<List<String>> queryWords = new ArrayList<>();
    final Matcher matcher = WORD.matcher(EXCLUDED_WORD.matcher(query).replaceAll(" "));
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
   * The forms the analyzers keep of one word: the word and, for an identifier, the parts around its
   * underscores, with case and accents folded and stop words and short words dropped.
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

  @Nonnull
  private static String fold(@Nonnull final String word) {
    return COMBINING_MARKS
        .matcher(Normalizer.normalize(word, Normalizer.Form.NFD))
        .replaceAll("")
        .toLowerCase(Locale.ROOT);
  }

  private boolean isKept(@Nonnull final String word) {
    return word.length() >= Math.max(1, minWordLength)
        && !DATAHUB_STOP_WORDS_LIST.contains(word)
        && !EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.contains(word);
  }

  @Nonnull
  private String stem(@Nonnull final String word) {
    stemmer.setCurrent(word);
    stemmer.stem();
    return stemmer.getCurrent();
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
