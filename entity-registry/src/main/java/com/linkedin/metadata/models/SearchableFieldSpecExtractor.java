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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link SearchableFieldSpec}s
 * from an aspect schema.
 */
public class SearchableFieldSpecExtractor implements SchemaVisitor {

  private final List<SearchableFieldSpec> _specs = new ArrayList<>();
  private final Set<String> _searchFieldNames = new HashSet<>();

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
            final Object annotationObj = resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);
            if (annotationObj != null) {
              final PathSpec path = new PathSpec(context.getSchemaPathSpec());
              final SearchableAnnotation annotation = SearchableAnnotation.fromPegasusAnnotationObject(
                  annotationObj,
                  path.toString());
              if (_searchFieldNames.contains(annotation.getFieldName())) {
                throw new ModelValidationException(
                    String.format("Entity has multiple searchable fields with the same field name %s",
                        annotation.getFieldName()));
              }
              final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
              _specs.add(fieldSpec);
          }
        }
      } else if (isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {
        final Object annotationObj = resolvedProperties.get(SearchableAnnotation.ANNOTATION_NAME);

        if (annotationObj != null) {
          final PathSpec path = new PathSpec(context.getSchemaPathSpec());
          final SearchableAnnotation annotation = SearchableAnnotation.fromPegasusAnnotationObject(
              annotationObj,
              path.toString()
          );
          final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
          _specs.add(fieldSpec);
        }
      }
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

  private  Map<String, Object> getResolvedProperties(final DataSchema schema) {
    return !schema.getResolvedProperties().isEmpty() ? schema.getResolvedProperties() : schema.getProperties();
  }

  private Boolean isValidComplexType(final ComplexDataSchema schema) {
    return DataSchema.Type.ENUM.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return true;
  }
}