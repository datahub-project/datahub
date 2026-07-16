package com.linkedin.metadata.models;

import static com.linkedin.metadata.models.FieldSpecUtils.isNullAnnotation;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ComplexDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotationValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link SearchableFieldSpec}s
 * from an aspect schema.
 */
@Slf4j
public class SearchableFieldSpecExtractor implements SchemaVisitor {

  private final List<SearchableFieldSpec> _specs = new ArrayList<>();
  private final Map<String, String> _searchFieldNamesToPatch = new HashMap<>();
  private final List<SearchableAnnotationValidator.AnnotatedField> _annotatedFields =
      new ArrayList<>();

  private static final String MAP = "map";

  public static final Map<String, Object> PRIMARY_URN_SEARCH_PROPERTIES;

  static {
    PRIMARY_URN_SEARCH_PROPERTIES = new DataMap();
    PRIMARY_URN_SEARCH_PROPERTIES.put("enableAutocomplete", "true");
    PRIMARY_URN_SEARCH_PROPERTIES.put("fieldType", "URN");
    PRIMARY_URN_SEARCH_PROPERTIES.put("boostScore", "10.0");
  }

  private static final float SECONDARY_URN_FACTOR = 0.1f;
  private static final Set<String> SECONDARY_URN_FIELD_TYPES =
      ImmutableSet.<String>builder().add("URN").add("URN_PARTIAL").build();

  public List<SearchableFieldSpec> getSpecs() {
    // Perform cross-annotation validation before returning specs
    SearchableAnnotationValidator.validateCrossAnnotationCompatibility(_annotatedFields);

    // Filter out specs with problematic field names (containing double underscores)
    // These are generated from null values in path-based @Searchable annotations
    return _specs.stream()
        .filter(
            spec -> {
              String fieldName = spec.getSearchableAnnotation().getFieldName();
              if (fieldName != null && fieldName.contains("__")) {
                log.warn(
                    "Filtering out searchable field spec with problematic field name: {}",
                    fieldName);
                return false;
              }
              return true;
            })
        .collect(java.util.stream.Collectors.toList());
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      final Object annotationObj = getAnnotationObj(context);

      if (!isNullAnnotation(annotationObj)) {

        if (currentSchema.getDereferencedDataSchema().isComplex()) {
          final ComplexDataSchema complexSchema = (ComplexDataSchema) currentSchema;
          if (isValidComplexType(complexSchema)) {
            extractSearchableAnnotation(annotationObj, currentSchema, context);
          }
        } else if (isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {
          extractSearchableAnnotation(annotationObj, currentSchema, context);
        } else {
          throw new ModelValidationException(
              String.format(
                  "Invalid @Searchable Annotation at %s", context.getSchemaPathSpec().toString()));
        }
      }
    }
  }

  private Object getAnnotationObj(TraverserContext context) {
    final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

    // First, check properties for primary annotation definition.
    final Map<String, Object> properties = context.getEnclosingField().getProperties();

    // Validate the primary annotation on the field - only applies to override case.
    final Object primaryAnnotationObj = properties.get(SearchableAnnotation.ANNOTATION_NAME);
    if (primaryAnnotationObj != null) {
      validatePropertiesAnnotation(
          currentSchema, primaryAnnotationObj, context.getTraversePath().toString());
    }

    // Specific handling for MAP fields: Extract the annotation from the values of the map.
    // Note that this only works for maps of type primitive.
    if (currentSchema.getDereferencedType() == DataSchema.Type.MAP) {
      MapDataSchema mapSchema = ((MapDataSchema) currentSchema);
      Object maybeKeyAnnotation =
          mapSchema.getKey().getResolvedProperties().get(SearchableAnnotation.ANNOTATION_NAME);
      Object maybeValueAnnotation =
          mapSchema.getValues().getResolvedProperties().get(SearchableAnnotation.ANNOTATION_NAME);
      return maybeKeyAnnotation != null ? maybeKeyAnnotation : maybeValueAnnotation;
    }

    // Skip processing any field paths inside a map, since we already catch and process it above.
    // If we do not do this, the indices update process may be effected.
    if (context.getTraversePath().contains(MAP)) {
      return null;
    }

    final Map<String, Object> resolvedProperties =
        FieldSpecUtils.getResolvedProperties(currentSchema, Collections.emptyMap());

    final boolean isUrn =
        ((DataMap) context.getParentSchema().getProperties().getOrDefault("java", new DataMap()))
            .getOrDefault("class", "")
            .equals("com.linkedin.common.urn.Urn");

    // Adjust URN annotations before returning.
    if (isUrn) {
      final Object resolvedAnnotationObj =
          resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);

      if (isNullAnnotation(resolvedAnnotationObj)) {
        return null;
      }

      final DataMap annotationMap = (DataMap) resolvedAnnotationObj;
      Map<String, Object> result = new HashMap<>(annotationMap);

      // Override boostScore for secondary urn
      if (SECONDARY_URN_FIELD_TYPES.contains(
          annotationMap.getOrDefault("fieldType", "URN").toString())) {
        result.put(
            "boostScore",
            Float.parseFloat(String.valueOf(annotationMap.getOrDefault("boostScore", "1.0")))
                * SECONDARY_URN_FACTOR);
      }

      return result;
    } else {
      // Next, check resolved properties for annotations on primitives.
      return resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);
    }
  }

  private void extractSearchableAnnotation(
      final Object annotationObj, final DataSchema currentSchema, final TraverserContext context) {
    final PathSpec path = new PathSpec(context.getSchemaPathSpec());
    final Optional<PathSpec> fullPath = FieldSpecUtils.getPathSpecWithAspectName(context);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationObj,
            FieldSpecUtils.getSchemaFieldName(path),
            currentSchema.getDereferencedType(),
            path.toString());

    String schemaPathSpec = context.getSchemaPathSpec().toString();
    if (_searchFieldNamesToPatch.containsKey(annotation.getFieldName())
        && !_searchFieldNamesToPatch.get(annotation.getFieldName()).equals(schemaPathSpec)) {
      // Try to use path
      String pathName = path.toString().replace('/', '_').replace("*", "");
      if (pathName.startsWith("_")) {
        pathName = pathName.replaceFirst("_", "");
      }

      if (_searchFieldNamesToPatch.containsKey(pathName)
          && !_searchFieldNamesToPatch.get(pathName).equals(schemaPathSpec)) {
        throw new ModelValidationException(
            String.format(
                "Entity has multiple searchable fields with the same field name %s, path: %s",
                annotation.getFieldName(), fullPath.orElse(path)));
      } else {
        annotation =
            new SearchableAnnotation(
                pathName,
                annotation.getFieldType(),
                annotation.isQueryByDefault(),
                annotation.isEnableAutocomplete(),
                annotation.isAddToFilters(),
                annotation.isAddHasValuesToFilters(),
                annotation.getFilterNameOverride(),
                annotation.getHasValuesFilterNameOverride(),
                annotation.getBoostScore(),
                annotation.getHasValuesFieldName(),
                annotation.getNumValuesFieldName(),
                annotation.getWeightsPerFieldValue(),
                annotation.getFieldNameAliases(),
                annotation.isIncludeQueryEmptyAggregation(),
                annotation.isIncludeSystemModifiedAt(),
                annotation.getSystemModifiedAtFieldName(),
                annotation.getSearchTier(),
                annotation.getSearchLabel(),
                annotation.getSearchIndexed(),
                annotation.getEntityFieldName(),
                annotation.getEagerGlobalOrdinals(),
                annotation.isSanitizeRichText());
      }
    }
    log.debug("Searchable annotation for field: {} : {}", schemaPathSpec, annotation);
    final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
    _specs.add(fieldSpec);
    _searchFieldNamesToPatch.put(annotation.getFieldName(), context.getSchemaPathSpec().toString());

    // Collect annotated field for cross-annotation validation
    _annotatedFields.add(
        new SearchableAnnotationValidator.AnnotatedField(
            annotation, currentSchema.getDereferencedType(), path.toString()));
  }

  @Override
  public VisitorContext getInitialVisitorContext() {
    return null;
  }

  @Override
  public SchemaVisitorTraversalResult getSchemaVisitorTraversalResult() {
    return new SchemaVisitorTraversalResult();
  }

  private Boolean isValidComplexType(final ComplexDataSchema schema) {
    return DataSchema.Type.ENUM.equals(schema.getDereferencedDataSchema().getDereferencedType())
        || DataSchema.Type.MAP.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return true;
  }

  private void validatePropertiesAnnotation(
      DataSchema currentSchema, Object annotationObj, String pathStr) {

    // If primitive, assume the annotation is well formed until resolvedProperties reflects it.
    if (currentSchema.isPrimitive()
        || currentSchema.getDereferencedType().equals(DataSchema.Type.ENUM)
        || currentSchema.getDereferencedType().equals(DataSchema.Type.MAP)) {
      return;
    }

    // Required override case. If the annotation keys are not overrides, they are incorrect.
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared inside %s: Invalid value type provided (Expected Map)",
              SearchableAnnotation.ANNOTATION_NAME, pathStr));
    }

    Map<String, Object> annotationMap = (Map<String, Object>) annotationObj;

    if (annotationMap.size() == 0) {
      throw new ModelValidationException(
          String.format(
              "Invalid @Searchable Annotation at %s. Annotation placed on invalid field of type %s. Must be placed on primitive field.",
              pathStr, currentSchema.getType()));
    }

    for (String key : annotationMap.keySet()) {
      if (!key.startsWith(Character.toString(PathSpec.SEPARATOR))) {
        throw new ModelValidationException(
            String.format(
                "Invalid @Searchable Annotation at %s. Annotation placed on invalid field of type %s. Must be placed on primitive field.",
                pathStr, currentSchema.getType()));
      }
    }
  }
}
