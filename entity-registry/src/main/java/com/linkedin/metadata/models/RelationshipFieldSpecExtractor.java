package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RelationshipFieldSpecExtractor implements SchemaVisitor {

  private static final String RELATIONSHIP_ANNOTATION_NAME = "Relationship";

  private final List<RelationshipFieldSpec> _specs = new ArrayList<>();

  public List<RelationshipFieldSpec> getSpecs() {
    return _specs;
  }

  SchemaAnnotationProcessor.SchemaAnnotationProcessResult _processedSchema;

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (DataSchemaTraverse.Order.POST_ORDER.equals(order)) {
      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      if (currentSchema.isComplex()) {
        // Case 1: Relationship Annotation on BaseRelationship Object.
        Map<String, Object> resolvedPropertiesByPath = new HashMap<>();

        ArrayDeque<String> clonedPath = context.getSchemaPathSpec().clone();
        try {
          if (context.getSchemaPathSpec().size() != 0) {
            if (context.getSchemaPathSpec().getLast().equals("string")) {
              clonedPath.removeLast();
            }
            resolvedPropertiesByPath = SchemaAnnotationProcessor.getResolvedPropertiesByPath(
                new PathSpec(clonedPath).toString(),
                _processedSchema.getResultSchema());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        final Object annotationObj = resolvedPropertiesByPath.get(RELATIONSHIP_ANNOTATION_NAME); //enclosingField.getProperties().get(SEARCHABLE_ANNOTATION_NAME);

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

        Map<String, Object> resolvedPropertiesByPath = new HashMap<>();

        ArrayDeque<String> clonedPath = context.getSchemaPathSpec().clone();
        try {
          if (context.getSchemaPathSpec().size() != 0) {
            if (context.getSchemaPathSpec().getLast().equals("string")) {
              clonedPath.removeLast();
            }
            resolvedPropertiesByPath = SchemaAnnotationProcessor.getResolvedPropertiesByPath(new PathSpec(clonedPath).toString(),
                _processedSchema.getResultSchema());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        final Object annotationObj = resolvedPropertiesByPath.get(RELATIONSHIP_ANNOTATION_NAME); //enclosingField.getProperties().get(SEARCHABLE_ANNOTATION_NAME);

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

  public RelationshipFieldSpecExtractor(SchemaAnnotationProcessor.SchemaAnnotationProcessResult processedSchema) {
    _processedSchema = processedSchema;
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
