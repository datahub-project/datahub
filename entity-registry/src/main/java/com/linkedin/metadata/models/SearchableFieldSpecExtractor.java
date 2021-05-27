package com.linkedin.metadata.models;

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


/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link SearchableFieldSpec}s
 * from an aspect schema.
 */
public class SearchableFieldSpecExtractor implements SchemaVisitor {

  private final List<SearchableFieldSpec> _specs = new ArrayList<>();
  private final Map<String, String> _searchFieldNamesToPatch = new HashMap<>();

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
      final Map<String, Object> resolvedProperties = getResolvedProperties(currentSchema);

      if (currentSchema.getDereferencedDataSchema().isComplex()) {
        final ComplexDataSchema complexSchema = (ComplexDataSchema) currentSchema;
        if (isValidComplexType(complexSchema)) {
          extractSearchableAnnotation(currentSchema, resolvedProperties, context);
        }
      } else if (isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {
        extractSearchableAnnotation(currentSchema, resolvedProperties, context);
      }
    }
  }

  private void extractSearchableAnnotation(
      final DataSchema currentSchema,
      final Map<String, Object> resolvedProperties,
      final TraverserContext context) {
    final Object annotationObj = resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);
    if (annotationObj != null) {
      final PathSpec path = new PathSpec(context.getSchemaPathSpec());
      final SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationObj,
              getSchemaFieldName(path),
              currentSchema.getDereferencedType(), path.toString());
      if (_searchFieldNamesToPatch.containsKey(annotation.getFieldName())
          && !_searchFieldNamesToPatch.get(annotation.getFieldName()).equals(context.getSchemaPathSpec().toString())) {
        throw new ModelValidationException(
            String.format("Entity has multiple searchable fields with the same field name %s",
                annotation.getFieldName()));
      }
      final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
      _specs.add(fieldSpec);
      _searchFieldNamesToPatch.put(annotation.getFieldName(), context.getSchemaPathSpec().toString());
    }
  }

  @Override
  public VisitorContext getInitialVisitorContext() {
    return null;
  }

  @Override
  public SchemaVisitorTraversalResult getSchemaVisitorTraversalResult() {
    return new SchemaVisitorTraversalResult();
  }

  private String getSchemaFieldName(PathSpec pathSpec) {
    List<String> components = pathSpec.getPathComponents();
    String lastComponent = components.get(components.size() - 1);
    if (lastComponent.equals("*")) {
      return components.get(components.size() - 2);
    }
    return lastComponent;
  }

  private Map<String, Object> getResolvedProperties(final DataSchema schema) {
    return !schema.getResolvedProperties().isEmpty() ? schema.getResolvedProperties() : schema.getProperties();
  }

  private Boolean isValidComplexType(final ComplexDataSchema schema) {
    return DataSchema.Type.ENUM.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return true;
  }
}