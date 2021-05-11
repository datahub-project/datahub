package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;

import java.util.ArrayList;
import java.util.List;

public class SearchableFieldSpecExtractor implements SchemaVisitor {

    private static final String SEARCHABLE_ANNOTATION_NAME = "Searchable";

    private final List<SearchableFieldSpec> _specs = new ArrayList<>();

    public List<SearchableFieldSpec> getSpecs() {
        return _specs;
    }

    @Override
    public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
        if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
            final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();
            if (currentSchema.isPrimitive()) {
                final RecordDataSchema.Field enclosingField = context.getEnclosingField();
                final Object annotationObj = enclosingField.getProperties().get(SEARCHABLE_ANNOTATION_NAME);

                if (annotationObj != null) {
                    final PathSpec path = new PathSpec(context.getSchemaPathSpec());
                    final SearchableAnnotation annotation = SearchableAnnotation
                            .fromPegasusAnnotationObject(annotationObj);
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
}
