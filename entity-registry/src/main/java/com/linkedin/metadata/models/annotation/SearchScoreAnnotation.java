package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;


/**
 * Annotation indicating how the search results should be ranked by the underlying search service
 */
@Value
public class SearchScoreAnnotation {

  public static final String ANNOTATION_NAME = "SearchScore";

  // Name of the field in the search index. Defaults to the field name in the schema
  String fieldName;
  // Weight to apply to the score
  double weight;
  // Value to use when field is missing
  double defaultValue;
  // Modifier to apply to the value. None if empty.
  Optional<Modifier> modifier;

  public enum Modifier {
    LOG, // log_10(value + 1)
    LN, // ln(value + 1)
    SQRT, // sqrt(value)
    SQUARE, // value^2
    RECIPROCAL // 1/value
  }

  @Nonnull
  public static SearchScoreAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName, @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format("Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    final Optional<Double> weight = AnnotationUtils.getField(map, "weight", Double.class);
    final Optional<Double> defaultValue = AnnotationUtils.getField(map, "defaultValue", Double.class);
    final Optional<String> modifierStr = AnnotationUtils.getField(map, "modifier", String.class);
    if (modifierStr.isPresent() && !EnumUtils.isValidEnum(Modifier.class, modifierStr.get())) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid field 'modifier'. Invalid modifier provided. Valid modifiers are %s",
          ANNOTATION_NAME, context, Arrays.toString(Modifier.values())));
    }
    final Optional<Modifier> modifier = modifierStr.map(Modifier::valueOf);
    return new SearchScoreAnnotation(fieldName.orElse(schemaFieldName), weight.orElse(1.0), defaultValue.orElse(0.0),
        modifier);
  }
}
