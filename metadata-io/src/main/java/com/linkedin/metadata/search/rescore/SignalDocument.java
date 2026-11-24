package com.linkedin.metadata.search.rescore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;

/**
 * One candidate the rescore reads. V2.5 builds it from a {@link SearchHit}. The template scorer
 * builds it from the source map the caller already fetched.
 */
public record SignalDocument(
    @Nonnull Map<String, Object> source,
    @Nullable String[] matchedQueries,
    @Nullable String indexName,
    double score) {

  public SignalDocument {
    // A partial update writes JSON null into _source to clear a field. Every signal reads that
    // as missing, and Map.copyOf rejects the null entry.
    source = withoutNulls(source);
  }

  @Nonnull
  private static Map<String, Object> withoutNulls(@Nullable Map<String, Object> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    Map<String, Object> copy = new HashMap<>();
    for (Map.Entry<String, Object> entry : source.entrySet()) {
      if (entry.getKey() != null && entry.getValue() != null) {
        copy.put(entry.getKey(), entry.getValue());
      }
    }
    return Map.copyOf(copy);
  }

  @Nonnull
  public static SignalDocument from(@Nonnull SearchHit hit, double score) {
    Map<String, Object> source = new HashMap<>();
    Map<String, Object> stored = hit.getSourceAsMap();
    if (stored != null) {
      source.putAll(stored);
    }
    Map<String, DocumentField> fields = hit.getFields();
    if (fields != null) {
      fields.forEach(
          (name, field) -> {
            if (name == null || field == null || source.get(name) != null) {
              return;
            }
            // A stage-required copy_to field is absent from _source. Doc values carry the
            // indexed term for whatever type the field has. Several boolean aspects fold to
            // one flag. Every other type is left for the stage to interpret.
            Object value = docValue(field);
            if (value != null) {
              source.put(name, value);
            }
          });
    }
    return new SignalDocument(source, hit.getMatchedQueries(), hit.getIndex(), score);
  }

  /**
   * One doc value is stored as itself. Several values stay a list so a stage can apply its own
   * aggregation or keyword match. Boolean terms still fold to one flag: several aspects copy the
   * same has-values field, and any true wins. A count, date, keyword, or URN is not rewritten.
   */
  @Nullable
  private static Object docValue(@Nonnull DocumentField field) {
    List<Object> values = field.getValues();
    if (values == null || values.isEmpty()) {
      return field.getValue();
    }
    if (values.stream().allMatch(value -> value instanceof Boolean)) {
      return booleanDocValue(field);
    }
    if (values.size() == 1) {
      return values.get(0);
    }
    return List.copyOf(values);
  }

  /**
   * True when any doc value is true. A false-only field stays false. A value that is not a boolean
   * is missing. {@code datasetProperties} and {@code editableDatasetProperties} both copy {@code
   * hasDescription}, and the same pattern exists for owners, tags, and glossary terms. This is the
   * only any-true fold: callers already know every value is a boolean.
   */
  @Nullable
  private static Boolean booleanDocValue(@Nonnull DocumentField field) {
    List<Object> values = field.getValues();
    if (values == null || values.isEmpty()) {
      return field.getValue() instanceof Boolean bool ? bool : null;
    }
    Boolean resolved = null;
    for (Object value : values) {
      boolean parsed = (Boolean) value;
      if (parsed) {
        return Boolean.TRUE;
      }
      resolved = Boolean.FALSE;
    }
    return resolved;
  }

  /** Same source and matched queries, with the score the rescorer selected for this hit. */
  @Nonnull
  public SignalDocument withScore(double score) {
    return new SignalDocument(source, matchedQueries, indexName, score);
  }
}
