package com.linkedin.metadata.extractor;

import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


public class FieldExtractor {
  private FieldExtractor() {
  }

  private static <T extends FieldSpec> Map<String, T> getPathToFieldSpecMap(Map<String, List<T>> fieldSpecsPerAspect) {
    return fieldSpecsPerAspect.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue()
            .stream()
            .map(fieldSpec -> Pair.of(entry.getKey() + "/" + fieldSpec.getPath().toString(), fieldSpec)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Function to extract the fields that match the input fieldSpecs
   */
  public static <T extends FieldSpec> Map<T, Object> extractFields(RecordTemplate snapshot,
      Map<String, List<T>> fieldSpecsPerAspect) {
    final EntitySpec entitySpec = EntitySpecBuilder.buildEntitySpec(snapshot.schema());
    ObjectIterator iterator = new ObjectIterator(snapshot.data(), snapshot.schema(), IterationOrder.PRE_ORDER);
    Map<String, T> pathToFieldSpec = getPathToFieldSpecMap(fieldSpecsPerAspect);
    final Map<T, Object> result = new HashMap<>();
    for (DataElement dataElement = iterator.next(); dataElement != null; dataElement = iterator.next()) {
      final PathSpec pathSpec = dataElement.getSchemaPathSpec();
      List<String> pathComponents = pathSpec.getPathComponents();
      if (pathComponents.size() < 4) {
        continue;
      }
      final String path = StringUtils.join(pathComponents.subList(2, pathComponents.size()), "/");
      final Optional<T> matchingSpec = Optional.ofNullable(pathToFieldSpec.get(path));
      if (matchingSpec.isPresent()) {
        result.put(matchingSpec.get(), dataElement.getValue());
      }
    }
    return result;
  }
}
