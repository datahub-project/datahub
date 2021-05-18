package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SearchableFieldSpecExtractor implements SchemaVisitor {

  private static final String SEARCHABLE_ANNOTATION_NAME = "Searchable";

  private final List<SearchableFieldSpec> _specs = new ArrayList<>();

  public List<SearchableFieldSpec> getSpecs() {
    return _specs;
  }

  private SchemaAnnotationProcessor.SchemaAnnotationProcessResult _processedSchema;

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();
      if (currentSchema.isPrimitive()) {
        Map<String, Object> resolvedPropertiesByPath = new HashMap<>();

        try {
          resolvedPropertiesByPath = SchemaAnnotationProcessor.getResolvedPropertiesByPath(new PathSpec(context.getSchemaPathSpec()).toString(),
              _processedSchema.getResultSchema());
        } catch (Exception e) {
          e.printStackTrace();
        }

        final Object annotationObj = resolvedPropertiesByPath.get(SEARCHABLE_ANNOTATION_NAME); //enclosingField.getProperties().get(SEARCHABLE_ANNOTATION_NAME);

        if (annotationObj != null) {
          // TOOD: Validate that we are looking at a primitive / array of primitives.
          final PathSpec path = new PathSpec(context.getSchemaPathSpec());
          final SearchableAnnotation annotation = SearchableAnnotation.fromPegasusAnnotationObject(annotationObj);
          final SearchableFieldSpec fieldSpec = new SearchableFieldSpec(path, annotation, currentSchema);
          _specs.add(fieldSpec);
        }
      }
    }
  }

  public SearchableFieldSpecExtractor(SchemaAnnotationProcessor.SchemaAnnotationProcessResult processedSchema) {
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
