package com.linkedin.metadata.models.extractor;

import com.datahub.util.RecordUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Extracts fields from a RecordTemplate based on the appropriate {@link FieldSpec}. */
public class FieldExtractor {

  private static final String ARRAY_WILDCARD = "*";
  private static final int MAX_VALUE_LENGTH = 200;

  private FieldExtractor() {}

  private static long getNumArrayWildcards(PathSpec pathSpec) {
    return pathSpec.getPathComponents().stream().filter(ARRAY_WILDCARD::equals).count();
  }

  // Extract the value of each field in the field specs from the input record
  public static <T extends FieldSpec> Map<T, List<Object>> extractFields(
      @Nonnull RecordTemplate record, List<T> fieldSpecs) {
    return extractFields(record, fieldSpecs, MAX_VALUE_LENGTH);
  }

  public static <T extends FieldSpec> Map<T, List<Object>> extractFields(
      @Nonnull RecordTemplate record, List<T> fieldSpecs, int maxValueLength) {
    final Map<T, List<Object>> extractedFields = new HashMap<>();
    for (T fieldSpec : fieldSpecs) {
      Optional<Object> value = RecordUtils.getFieldValue(record, fieldSpec.getPath());
      if (!value.isPresent()) {
        extractedFields.put(fieldSpec, Collections.emptyList());
      } else {
        long numArrayWildcards = getNumArrayWildcards(fieldSpec.getPath());
        // Not an array field
        if (numArrayWildcards == 0) {
          // For maps, convert it into a list of the form key=value (Filter out long values)
          if (value.get() instanceof Map) {
            extractedFields.put(
                fieldSpec,
                ((Map<?, ?>) value.get())
                    .entrySet().stream()
                        .map(
                            entry ->
                                new Pair<>(entry.getKey().toString(), entry.getValue().toString()))
                        .filter(entry -> entry.getValue().length() < maxValueLength)
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.toList()));
          } else {
            extractedFields.put(fieldSpec, Collections.singletonList(value.get()));
          }
        } else {
          List<Object> valueList = (List<Object>) value.get();
          // If the field is a nested list of values, flatten it
          for (int i = 0; i < numArrayWildcards - 1; i++) {
            valueList =
                valueList.stream()
                    .flatMap(v -> ((List<Object>) v).stream())
                    .collect(Collectors.toList());
          }
          extractedFields.put(fieldSpec, valueList);
        }
      }
    }
    return extractedFields;
  }

  public static <T extends FieldSpec> Map<T, List<Object>> extractFieldsFromSnapshot(
      RecordTemplate snapshot,
      EntitySpec entitySpec,
      Function<AspectSpec, List<T>> getFieldSpecsFunc,
      int maxValueLength) {
    final Map<String, RecordTemplate> aspects = AspectExtractor.extractAspectRecords(snapshot);
    final Map<T, List<Object>> extractedFields = new HashMap<>();
    aspects.keySet().stream()
        .map(
            aspectName ->
                FieldExtractor.extractFields(
                    aspects.get(aspectName),
                    getFieldSpecsFunc.apply(entitySpec.getAspectSpec(aspectName)),
                    maxValueLength))
        .forEach(extractedFields::putAll);
    return extractedFields;
  }
}
