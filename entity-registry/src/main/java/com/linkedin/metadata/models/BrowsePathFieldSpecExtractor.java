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

public class BrowsePathFieldSpecExtractor implements SchemaVisitor {

    private static final String BROWSE_PATH_ANNOTATION_NAME = "BrowsePath";

    private final List<BrowsePathFieldSpec> _specs = new ArrayList<>();

    public List<BrowsePathFieldSpec> getSpecs() {
        return _specs;
    }

    @Override
    public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
        if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

            final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

            if (currentSchema.isComplex()) {
                // Case 1: BrowsePath Annotation on Array of string
                // TODO: Validate that it is array[string]
                final RecordDataSchema.Field enclosingField = context.getEnclosingField();
                if (enclosingField != null) {
                    final Object annotationObj = enclosingField.getProperties().get(BROWSE_PATH_ANNOTATION_NAME);

                    if (annotationObj != null) {
                        // TOOD: Validate that we are looking at a primitive / array of primitives.
                        final PathSpec path = new PathSpec(context.getSchemaPathSpec());
                        final BrowsePathFieldSpec fieldSpec = new BrowsePathFieldSpec(path);
                        _specs.add(fieldSpec);
                    }
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