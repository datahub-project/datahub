package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;


public class RelationshipFieldSpecExtractor implements SchemaVisitor {

  private static final String RELATIONSHIP_ANNOTATION_NAME = "Relationship";

  private final List<RelationshipFieldSpec> _specs = new ArrayList<>();

  public List<RelationshipFieldSpec> getSpecs() {
    return _specs;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      if (currentSchema.isComplex()) {
        // Case 1: Relationship Annotation on BaseRelationship Object.
        final Object annotationObj = currentSchema.getProperties().get(RELATIONSHIP_ANNOTATION_NAME);
        if (annotationObj != null) {
          final ArrayDeque<String> modifiedPath = new ArrayDeque<>(context.getSchemaPathSpec());
          modifiedPath.add("entity"); // TODO: Validate that this field exists!
          final PathSpec path = new PathSpec(modifiedPath);
          final RelationshipAnnotation annotation = RelationshipAnnotation.fromPegasusAnnotationObject(annotationObj);
          final RelationshipFieldSpec fieldSpec = new RelationshipFieldSpec(path, annotation, currentSchema);
          _specs.add(fieldSpec);
        }
      } else {
        // Case 2: Relationship Annotation on Primitive or Array of Primitives
        final RecordDataSchema.Field enclosingField = context.getEnclosingField();
        final Object annotationObj = enclosingField.getProperties().get(RELATIONSHIP_ANNOTATION_NAME);

        if (annotationObj != null) {
          // TOOD: Validate that we are looking at a primitive / array of primitives.
          final PathSpec path = new PathSpec(context.getSchemaPathSpec());
          final RelationshipAnnotation annotation = RelationshipAnnotation.fromPegasusAnnotationObject(annotationObj);
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
}