package com.linkedin.metadata.models;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link SearchScoreFieldSpec}s
 * from an aspect schema.
 */
public class SearchScoreFieldSpecExtractor implements SchemaVisitor {

  private final List<SearchScoreFieldSpec> _specs = new ArrayList<>();
  private static final Set<DataSchema.Type> NUMERIC_TYPES =
      ImmutableSet.of(DataSchema.Type.INT, DataSchema.Type.LONG, DataSchema.Type.FLOAT, DataSchema.Type.DOUBLE);

  public List<SearchScoreFieldSpec> getSpecs() {
    return _specs;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      final Object annotationObj = getAnnotationObj(context);

      if (annotationObj != null) {
        if (currentSchema.isPrimitive() && isNumericType((PrimitiveDataSchema) currentSchema)) {
          extractAnnotation(annotationObj, currentSchema, context);
        } else {
          throw new ModelValidationException(String.format(
              "Invalid @SearchScore Annotation at %s. This annotation can only be put in on a numeric singular (non-array) field",
              context.getSchemaPathSpec().toString()));
        }
      }
    }
  }

  private Object getAnnotationObj(TraverserContext context) {
    final Map<String, Object> properties = context.getEnclosingField().getProperties();
    return properties.get(SearchScoreAnnotation.ANNOTATION_NAME);
  }

  private void extractAnnotation(final Object annotationObj, final DataSchema currentSchema,
      final TraverserContext context) {
    final PathSpec path = new PathSpec(context.getSchemaPathSpec());
    final Optional<PathSpec> fullPath = FieldSpecUtils.getPathSpecWithAspectName(context);
    if (context.getSchemaPathSpec().contains(PathSpec.WILDCARD)) {
      throw new ModelValidationException(
          String.format("SearchScore annotation can only be put on singular fields (non-arrays): path %s",
              fullPath.orElse(path)));
    }
    final SearchScoreAnnotation annotation =
        SearchScoreAnnotation.fromPegasusAnnotationObject(annotationObj, FieldSpecUtils.getSchemaFieldName(path),
            path.toString());
    final SearchScoreFieldSpec fieldSpec = new SearchScoreFieldSpec(path, annotation, currentSchema);
    _specs.add(fieldSpec);
  }

  @Override
  public VisitorContext getInitialVisitorContext() {
    return null;
  }

  @Override
  public SchemaVisitorTraversalResult getSchemaVisitorTraversalResult() {
    return new SchemaVisitorTraversalResult();
  }

  private Boolean isNumericType(final PrimitiveDataSchema schema) {
    return NUMERIC_TYPES.contains(schema.getDereferencedType());
  }
}
