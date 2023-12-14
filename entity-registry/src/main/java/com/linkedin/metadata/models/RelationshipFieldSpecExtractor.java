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

      // First, check properties for primary annotation definition.
      final Map<String, Object> properties = context.getEnclosingField().getProperties();
      final Object primaryAnnotationObj = properties.get(RelationshipAnnotation.ANNOTATION_NAME);

      if (primaryAnnotationObj != null) {
        validatePropertiesAnnotation(
            currentSchema, primaryAnnotationObj, context.getTraversePath().toString());
      }

      // Next, check resolved properties for annotations on primitives.
      final Map<String, Object> resolvedProperties =
          FieldSpecUtils.getResolvedProperties(currentSchema);
      final Object resolvedAnnotationObj =
          resolvedProperties.get(RelationshipAnnotation.ANNOTATION_NAME);

      if (resolvedAnnotationObj != null) {
        if (currentSchema.isPrimitive()
            && isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {
          final PathSpec path = new PathSpec(context.getSchemaPathSpec());
          final RelationshipAnnotation annotation =
              RelationshipAnnotation.fromPegasusAnnotationObject(
                  resolvedAnnotationObj, path.toString());
          final RelationshipFieldSpec fieldSpec =
              new RelationshipFieldSpec(path, annotation, currentSchema);
          _specs.add(fieldSpec);
          return;
        }
        throw new ModelValidationException(
            String.format(
                "Invalid @Relationship Annotation at %s", context.getSchemaPathSpec().toString()));
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

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return DataSchema.Type.STRING.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private void validatePropertiesAnnotation(
      DataSchema currentSchema, Object annotationObj, String pathStr) {

    // If primitive, assume the annotation is well formed until resolvedProperties reflects it.
    if (currentSchema.isPrimitive()) {
      return;
    }

    // Required override case. If the annotation keys are not overrides, they are incorrect.
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared inside %s: Invalid value type provided (Expected Map)",
              RelationshipAnnotation.ANNOTATION_NAME, pathStr));
    }

    Map<String, Object> annotationMap = (Map<String, Object>) annotationObj;
    for (String key : annotationMap.keySet()) {
      if (!key.startsWith(Character.toString(PathSpec.SEPARATOR))) {
        throw new ModelValidationException(
            String.format(
                "Invalid @Relationship Annotation at %s. Annotation placed on invalid field of type %s. Must be placed on primitive field.",
                pathStr, currentSchema.getType()));
      }
    }
  }
}
