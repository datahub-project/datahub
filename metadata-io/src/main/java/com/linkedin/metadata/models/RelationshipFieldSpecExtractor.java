package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;

import java.util.ArrayList;
import java.util.List;

public class RelationshipFieldSpecExtractor implements SchemaVisitor {

    private static final String RELATIONSHIP_ANNOTATION_NAME = "Relationship";

    private List<RelationshipFieldSpec> _specs;

    public RelationshipFieldSpecExtractor() {
        _specs = new ArrayList<>();
    }

    public List<RelationshipFieldSpec> getSpecs() {
        return _specs;
    }

    @Override
    public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
        if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
            final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();
            if (currentSchema.isPrimitive()) {

                final RecordDataSchema.Field enclosingField = context.getEnclosingField();
                final Object annotationObj = enclosingField.getProperties().get(RELATIONSHIP_ANNOTATION_NAME);

                if (annotationObj != null) {
                    final PathSpec path = new PathSpec(context.getSchemaPathSpec());
                    final RelationshipFieldSpec.RelationshipAnnotation annotation = RelationshipFieldSpec.RelationshipAnnotation
                            .fromPegasusAnnotationObject(annotationObj);
                    final RelationshipFieldSpec fieldSpec = new RelationshipFieldSpec(path, currentSchema, annotation);
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