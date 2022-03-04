package com.linkedin.metadata.models.extractor;

import com.datahub.util.ModelUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.FieldSpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * Extracts fields from a RecordTemplate based on the appropriate {@link FieldSpec}.
 */
@Slf4j
public class AspectExtractor {

  private AspectExtractor() {
  }

  public static Map<String, RecordTemplate> extractAspectRecords(RecordTemplate snapshot) {
    return ModelUtils.getAspectsFromSnapshot(snapshot)
        .stream()
        .collect(Collectors.toMap(record -> getAspectNameFromSchema(record.schema()), Function.identity()));
  }

  private static String getAspectNameFromSchema(final RecordDataSchema aspectSchema) {
    final Object aspectAnnotationObj = aspectSchema.getProperties().get(AspectAnnotation.ANNOTATION_NAME);
    if (aspectAnnotationObj != null) {
      return AspectAnnotation.fromSchemaProperty(aspectAnnotationObj, aspectSchema.getFullName()).getName();
    }
    log.error(String.format("Failed to extract aspect name from provided schema %s", aspectSchema.getName()));
    throw new IllegalArgumentException(
        String.format("Failed to extract aspect name from provided schema %s", aspectSchema.getName()));
  }
}
