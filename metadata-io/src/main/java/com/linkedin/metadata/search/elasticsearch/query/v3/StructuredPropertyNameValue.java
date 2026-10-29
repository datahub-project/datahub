package com.linkedin.metadata.search.elasticsearch.query.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.StructuredPropertySearchCatalog;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMatchReport;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertySubfields;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Expands a name-and-value weight into one {@code dis_max} of remainder terms. A display name
 * matches a whole-word span. A qualified name matches one span with its dots kept. The longest span
 * wins, and a shorter span that overlaps it is not also applied.
 */
public final class StructuredPropertyNameValue {
  private StructuredPropertyNameValue() {}

  @Nullable
  public static QueryBuilder disMax(
      @Nonnull String query,
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      float weight) {
    String text = query.trim();
    if (text.isEmpty() || !(weight > 0f)) {
      return null;
    }
    Set<String> queriedPaths = new HashSet<>();
    for (SearchFieldConfig field : queried) {
      String base = propertyBase(field.fieldName());
      if (base != null) {
        queriedPaths.add(base);
      }
    }
    List<Span> spans = new ArrayList<>();
    for (StructuredPropertySearchCatalog.IndexedProperty property : properties) {
      if (property.valueType() != LogicalValueType.STRING
          && property.valueType() != LogicalValueType.RICH_TEXT) {
        continue;
      }
      if (!queriedPaths.contains(property.fieldPath())) {
        continue;
      }
      addDisplaySpans(text, property, spans);
      addQualifiedSpans(text, property, spans);
    }
    spans.sort(
        Comparator.comparingInt((Span span) -> span.end - span.start)
            .reversed()
            .thenComparingInt(span -> span.start));
    List<Span> chosen = new ArrayList<>();
    for (Span span : spans) {
      if (chosen.stream().noneMatch(other -> overlaps(other, span))) {
        chosen.add(span);
      }
    }
    DisMaxQueryBuilder disMax = QueryBuilders.disMaxQuery().tieBreaker(0f);
    Set<String> seen = new HashSet<>();
    int added = 0;
    for (Span span : chosen) {
      String remainder = remainder(text, span.start, span.end);
      if (remainder.isEmpty()) {
        continue;
      }
      if (!seen.add(span.definition.fieldPath() + "\0" + remainder)) {
        continue;
      }
      disMax.add(
          QueryBuilders.termQuery(span.definition.fieldPath(), remainder)
              .boost(weight)
              .queryName(SearchTemplateMatchReport.NAME_AND_VALUE));
      added++;
    }
    return added == 0 ? null : disMax;
  }

  private static void addDisplaySpans(
      @Nonnull String text,
      @Nonnull StructuredPropertySearchCatalog.IndexedProperty property,
      @Nonnull List<Span> spans) {
    if (property.displayName() == null || property.displayName().isBlank()) {
      return;
    }
    String[] nameWords = property.displayName().trim().split("\\s+");
    if (nameWords.length == 0) {
      return;
    }
    List<int[]> words = words(text);
    for (int i = 0; i + nameWords.length <= words.size(); i++) {
      boolean match = true;
      for (int j = 0; j < nameWords.length; j++) {
        int[] word = words.get(i + j);
        if (!text.regionMatches(true, word[0], nameWords[j], 0, nameWords[j].length())
            || word[1] - word[0] != nameWords[j].length()) {
          match = false;
          break;
        }
      }
      if (match) {
        spans.add(new Span(words.get(i)[0], words.get(i + nameWords.length - 1)[1], property));
      }
    }
  }

  private static void addQualifiedSpans(
      @Nonnull String text,
      @Nonnull StructuredPropertySearchCatalog.IndexedProperty property,
      @Nonnull List<Span> spans) {
    if (property.qualifiedName().isBlank()) {
      return;
    }
    String haystack = text.toLowerCase(Locale.ROOT);
    String needle = property.qualifiedName().toLowerCase(Locale.ROOT);
    int from = 0;
    while (from <= haystack.length() - needle.length()) {
      int index = haystack.indexOf(needle, from);
      if (index < 0) {
        return;
      }
      int end = index + needle.length();
      if (bounded(text, index, end)) {
        spans.add(new Span(index, end, property));
      }
      from = index + 1;
    }
  }

  private static boolean bounded(@Nonnull String text, int start, int end) {
    boolean left = start == 0 || Character.isWhitespace(text.charAt(start - 1));
    boolean right = end == text.length() || Character.isWhitespace(text.charAt(end));
    return left && right;
  }

  @Nonnull
  private static List<int[]> words(@Nonnull String text) {
    List<int[]> words = new ArrayList<>();
    int index = 0;
    while (index < text.length()) {
      while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
        index++;
      }
      if (index >= text.length()) {
        break;
      }
      int start = index;
      while (index < text.length() && !Character.isWhitespace(text.charAt(index))) {
        index++;
      }
      words.add(new int[] {start, index});
    }
    return words;
  }

  @Nonnull
  private static String remainder(@Nonnull String text, int start, int end) {
    String left = text.substring(0, start);
    String right = text.substring(end);
    return (left + " " + right).trim().replaceAll("\\s+", " ");
  }

  private static boolean overlaps(@Nonnull Span left, @Nonnull Span right) {
    return left.start < right.end && right.start < left.end;
  }

  @Nullable
  private static String propertyBase(@Nonnull String fieldName) {
    String prefix = STRUCTURED_PROPERTY_MAPPING_FIELD + ".";
    if (!fieldName.startsWith(prefix)) {
      return null;
    }
    for (String suffix : StructuredPropertySubfields.SUFFIXES) {
      if (fieldName.endsWith(suffix)) {
        return fieldName.substring(0, fieldName.length() - suffix.length());
      }
    }
    return fieldName;
  }

  private record Span(
      int start, int end, @Nonnull StructuredPropertySearchCatalog.IndexedProperty definition) {}
}
