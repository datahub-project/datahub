package com.linkedin.experimental;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class PathSpecToAnnotationTraverser<T> implements SchemaVisitor {

    private Map<List<String>, T> _index;
    private String _targetAnnotationName;
    private Function<Object, T> _annotationTransformer;


    public PathSpecToAnnotationTraverser(final String targetAnnotationName,
                                         final Function<Object, T> annotationTransformer) {
        _index = new HashMap<>();
        _targetAnnotationName = targetAnnotationName;
        _annotationTransformer = annotationTransformer;
    }

    public Map<List<String>, T> getIndex() {
        return _index;
    }

    @Override
    public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
        if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

            final DataSchema currentSchema = context.getCurrentSchema();

            if (currentSchema.isPrimitive()) {
                // If a primitive, then the
                // Only allow Indexing on primitives / array of primitives.
                final RecordDataSchema.Field enclosingField = context.getEnclosingField();
                final Object annotationObj = enclosingField.getProperties().get(_targetAnnotationName);

                if (annotationObj != null) {
                    final ArrayDeque<String> path = context.getSchemaPathSpec();
                    final T processedAnnotationObj = _annotationTransformer.apply(annotationObj);
                    _index.put(new ArrayList<>(path), processedAnnotationObj);
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
