package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link RelationshipFieldSpec}s
 * from an aspect schema.
 */
public class RelationshipFieldSpecExtractor implements SchemaVisitor {

  private final List<RelationshipFieldSpec> _specs = new ArrayList<>();

  public List<RelationshipFieldSpec> getSpecs() {
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

      if (currentSchema.isPrimitive() && isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {
        final Object annotationObj = resolvedProperties.get(RelationshipAnnotation.ANNOTATION_NAME);

        if (annotationObj != null) {
          final PathSpec path = new PathSpec(context.getSchemaPathSpec());
          final RelationshipAnnotation annotation = RelationshipAnnotation.fromPegasusAnnotationObject(
              annotationObj,
              path.toString()
          );
          final RelationshipFieldSpec fieldSpec = new RelationshipFieldSpec(path, annotation, currentSchema);
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

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return DataSchema.Type.STRING.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }
}
