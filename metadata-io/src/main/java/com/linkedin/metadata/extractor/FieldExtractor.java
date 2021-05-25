package com.linkedin.metadata.extractor;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


/**
 * Extracts fields from a RecordTemplate based on the appropriate {@link FieldSpec}.
 */
public class FieldExtractor {
  private FieldExtractor() {
  }

  private static <T extends FieldSpec> Map<String, T> getPathToFieldSpecMap(Map<String, List<T>> fieldSpecsPerAspect) {
    return fieldSpecsPerAspect.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue()
            .stream()
            .map(fieldSpec -> Pair.of(entry.getKey() + fieldSpec.getPath().toString(), fieldSpec)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Function to extract the fields that match the input fieldSpecs
   */
  public static <T extends FieldSpec> Map<T, List<Object>> extractFields(RecordTemplate snapshot,
      Map<String, List<T>> fieldSpecsPerAspect) {

    final ObjectIterator iterator = new ObjectIterator(snapshot.data(), snapshot.schema(), IterationOrder.PRE_ORDER);
    final Map<String, T> pathToFieldSpec = getPathToFieldSpecMap(fieldSpecsPerAspect);
    final Set<String> aspectsInSnapshot = new HashSet<>();
    final Map<T, List<Object>> fieldSpecToValues = new HashMap<>();
    for (DataElement dataElement = iterator.next(); dataElement != null; dataElement = iterator.next()) {
      final PathSpec pathSpec = dataElement.getSchemaPathSpec();
      List<String> pathComponents = pathSpec.getPathComponents();
      if (pathComponents.size() < 4) {
        continue;
      }
      String aspectName = PegasusUtils.getAspectNameFromFullyQualifiedName(pathComponents.get(2));
      aspectsInSnapshot.add(aspectName);
      final String path = aspectName + "/" + StringUtils.join(pathComponents.subList(3, pathComponents.size()), "/");
      final Optional<T> matchingSpec = Optional.ofNullable(pathToFieldSpec.get(path));
      if (matchingSpec.isPresent()) {
        List<Object> originalValues = fieldSpecToValues.computeIfAbsent(matchingSpec.get(), key -> new ArrayList<>());
        originalValues.add(dataElement.getValue());
        fieldSpecToValues.put(matchingSpec.get(), originalValues);
      }
    }

    // For the field specs in aspects set in the snapshot that did not match any fields in the snapshot,
    // set empty value to indicate the field is missing
    Set<T> requiredFieldSpec = fieldSpecsPerAspect.entrySet()
        .stream()
        .filter(entry -> aspectsInSnapshot.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toSet());
    pathToFieldSpec.values()
        .stream()
        .filter(spec -> !fieldSpecToValues.containsKey(spec) && requiredFieldSpec.contains(spec))
        .forEach(spec -> fieldSpecToValues.put(spec, ImmutableList.of()));
    return fieldSpecToValues;
  }
}
