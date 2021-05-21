package com.linkedin.metadata.models;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.ComplexDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link BrowsePathFieldSpec}s
 * from an aspect schema.
 */
public class BrowsePathFieldSpecExtractor implements SchemaVisitor {

  public static final String ANNOTATION_NAME = "BrowsePath";

  private final List<BrowsePathFieldSpec> _specs = new ArrayList<>();

  public List<BrowsePathFieldSpec> getSpecs() {
    return _specs;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {

    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {

      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();

      // Note: We do NOT support overrides on the @BrowsePath annotation due to the annotation being on
      // a non-leaf field. (Array<String>)
      final Map<String, Object> resolvedProperties = getResolvedProperties(context.getEnclosingField());

      if (currentSchema.getDereferencedDataSchema().isComplex()) {
        final ComplexDataSchema complexSchema = (ComplexDataSchema) currentSchema;
        if (isValidComplexType(complexSchema)) {
          final ArrayDataSchema arraySchema = (ArrayDataSchema) complexSchema;
          final DataSchema itemSchema = arraySchema.getItems().getDereferencedDataSchema();
          if (itemSchema.isPrimitive() && isValidPrimitiveType((PrimitiveDataSchema) itemSchema)) {

            final Object annotationObj = resolvedProperties.get(ANNOTATION_NAME);
            if (annotationObj != null) {
              final PathSpec path = new PathSpec(context.getSchemaPathSpec());
              final BrowsePathFieldSpec fieldSpec = new BrowsePathFieldSpec(path);
              _specs.add(fieldSpec);
            }
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

  private  Map<String, Object> getResolvedProperties(final RecordDataSchema.Field schema) {
    return !schema.getResolvedProperties().isEmpty() ? schema.getResolvedProperties() : schema.getProperties();
  }

  private Boolean isValidComplexType(final ComplexDataSchema schema) {
    return DataSchema.Type.ARRAY.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return DataSchema.Type.STRING.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }
}
