package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

/** Simple object representation of the @SearchableRefAnnotation annotation metadata. */
@Value
public class SearchableRefAnnotation {

  public static final String FIELD_NAME_ALIASES = "fieldNameAliases";
  public static final String ANNOTATION_NAME = "SearchableRef";
  private static final Set<SearchableAnnotation.FieldType> DEFAULT_QUERY_FIELD_TYPES =
      ImmutableSet.of(
          SearchableAnnotation.FieldType.TEXT,
          SearchableAnnotation.FieldType.OBJECT,
          SearchableAnnotation.FieldType.TEXT_PARTIAL,
          SearchableAnnotation.FieldType.WORD_GRAM,
          SearchableAnnotation.FieldType.URN,
          SearchableAnnotation.FieldType.URN_PARTIAL);

  // Name of the field in the search index. Defaults to the field name in the schema
  String fieldName;
  // Type of the field. Defines how the field is indexed and matched
  SearchableAnnotation.FieldType fieldType;
  // Whether we should match the field for the default search query
  boolean queryByDefault;
  // Boost multiplier to the match score. Matches on fields with higher boost score ranks higher
  double boostScore;
  // defines what depth should be explored of reference object
  int depth;
  // defines entity type of URN
  String refType;
  // (Optional) Aliases for this given field that can be used for sorting etc.
  List<String> fieldNameAliases;

  @Nonnull
  public static SearchableRefAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final DataSchema.Type schemaDataType,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    final Optional<String> fieldType = AnnotationUtils.getField(map, "fieldType", String.class);
    if (fieldType.isPresent()
        && !EnumUtils.isValidEnum(SearchableAnnotation.FieldType.class, fieldType.get())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field 'fieldType'. Invalid fieldType provided. Valid types are %s",
              ANNOTATION_NAME, context, Arrays.toString(SearchableAnnotation.FieldType.values())));
    }
    final Optional<String> refType = AnnotationUtils.getField(map, "refType", String.class);
    if (!refType.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: "
                  + "Mandatory input field refType defining the Entity Type is not provided",
              ANNOTATION_NAME, context));
    }
    final Optional<Boolean> queryByDefault =
        AnnotationUtils.getField(map, "queryByDefault", Boolean.class);
    final Optional<Integer> depth = AnnotationUtils.getField(map, "depth", Integer.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final List<String> fieldNameAliases = getFieldNameAliases(map);
    final SearchableAnnotation.FieldType resolvedFieldType =
        getFieldType(fieldType, schemaDataType);

    return new SearchableRefAnnotation(
        fieldName.orElse(schemaFieldName),
        resolvedFieldType,
        getQueryByDefault(queryByDefault, resolvedFieldType),
        boostScore.orElse(1.0),
        depth.orElse(2),
        refType.get(),
        fieldNameAliases);
  }

  private static Boolean getQueryByDefault(
      Optional<Boolean> maybeQueryByDefault, SearchableAnnotation.FieldType fieldType) {
    if (!maybeQueryByDefault.isPresent()) {
      if (DEFAULT_QUERY_FIELD_TYPES.contains(fieldType)) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    return maybeQueryByDefault.get();
  }

  private static SearchableAnnotation.FieldType getFieldType(
      Optional<String> maybeFieldType, DataSchema.Type schemaDataType) {
    if (!maybeFieldType.isPresent()) {
      return getDefaultFieldType(schemaDataType);
    }
    return SearchableAnnotation.FieldType.valueOf(maybeFieldType.get());
  }

  private static SearchableAnnotation.FieldType getDefaultFieldType(
      DataSchema.Type schemaDataType) {
    switch (schemaDataType) {
      case INT:
        return SearchableAnnotation.FieldType.COUNT;
      case MAP:
        return SearchableAnnotation.FieldType.KEYWORD;
      case FLOAT:
      case DOUBLE:
        return SearchableAnnotation.FieldType.DOUBLE;
      default:
        return SearchableAnnotation.FieldType.TEXT;
    }
  }

  private static List<String> getFieldNameAliases(Map map) {
    final List<String> aliases = new ArrayList<>();
    final Optional<List> fieldNameAliases =
        AnnotationUtils.getField(map, FIELD_NAME_ALIASES, List.class);
    if (fieldNameAliases.isPresent()) {
      for (Object alias : fieldNameAliases.get()) {
        aliases.add((String) alias);
      }
    }
    return aliases;
  }
}
