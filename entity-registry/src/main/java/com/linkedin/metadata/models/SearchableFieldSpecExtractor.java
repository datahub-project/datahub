package com.linkedin.metadata.models;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ComplexDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.ArrayList;
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
    return _specs;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      final Object annotationObj = getAnnotationObj(context);

      if (annotationObj != null) {
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
    final Object primaryAnnotationObj = properties.get(SearchableAnnotation.ANNOTATION_NAME);

    if (primaryAnnotationObj != null) {
      validatePropertiesAnnotation(
          currentSchema, primaryAnnotationObj, context.getTraversePath().toString());
      // Unfortunately, annotations on collections always need to be a nested map (byproduct of
      // making overrides work)
      // As such, for annotation maps, we make it a single entry map, where the key has no meaning
      if (currentSchema.getDereferencedType() == DataSchema.Type.MAP
          && primaryAnnotationObj instanceof Map
          && !((Map) primaryAnnotationObj).isEmpty()) {
        return ((Map<?, ?>) primaryAnnotationObj).entrySet().stream().findFirst().get().getValue();
      }
    }

    // Check if the path has map in it. Individual values of the maps (actual maps are caught above)
    // can be ignored
    if (context.getTraversePath().contains(MAP)) {
      return null;
    }

    final boolean isUrn =
        ((DataMap) context.getParentSchema().getProperties().getOrDefault("java", new DataMap()))
            .getOrDefault("class", "")
            .equals("com.linkedin.common.urn.Urn");

    final Map<String, Object> resolvedProperties =
        FieldSpecUtils.getResolvedProperties(currentSchema);

    // if primary doesn't have an annotation, then ignore secondary urns
    if (isUrn && primaryAnnotationObj != null) {
      DataMap annotationMap =
          (DataMap) resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);
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
                annotation.getFieldNameAliases());
      }
    }
    log.debug("Searchable annotation for field: {} : {}", schemaPathSpec, annotation);
    final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
    _specs.add(fieldSpec);
    _searchFieldNamesToPatch.put(annotation.getFieldName(), context.getSchemaPathSpec().toString());
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
