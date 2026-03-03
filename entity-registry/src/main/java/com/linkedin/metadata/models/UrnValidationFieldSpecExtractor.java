package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class UrnValidationFieldSpecExtractor implements SchemaVisitor {
  private final List<UrnValidationFieldSpec> urnValidationFieldSpecs = new ArrayList<>();

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {
    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();
      final PathSpec path = new PathSpec(context.getSchemaPathSpec());

      // Check for @UrnValidation annotation in primary properties
      final Object urnValidationAnnotationObj =
          context.getEnclosingField().getProperties().get(UrnValidationAnnotation.ANNOTATION_NAME);

      // Check if it's either explicitly annotated with @UrnValidation
      if (urnValidationAnnotationObj != null) {
        addUrnValidationFieldSpec(currentSchema, path, urnValidationAnnotationObj);
      }
    }
  }

  private void addUrnValidationFieldSpec(
      DataSchema currentSchema, PathSpec path, Object annotationObj) {
    UrnValidationAnnotation annotation =
        UrnValidationAnnotation.fromPegasusAnnotationObject(
            annotationObj, FieldSpecUtils.getSchemaFieldName(path), path.toString());

    urnValidationFieldSpecs.add(new UrnValidationFieldSpec(path, annotation, currentSchema));
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
