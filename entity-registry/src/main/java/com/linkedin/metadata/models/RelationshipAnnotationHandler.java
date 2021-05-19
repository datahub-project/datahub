package com.linkedin.metadata.models;

import com.linkedin.data.message.Message;
import com.linkedin.data.message.MessageList;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.ComplexDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;


public class RelationshipAnnotationHandler implements SchemaAnnotationHandler  {

  private static final AnnotationValidationResult VALID_RESULT = new AnnotationValidationResult();
  static {
    VALID_RESULT.setValid(true);
  }

  private final List<RelationshipFieldSpec> _specs = new ArrayList<>();
  private final PropertyOverrideComparator _comparator = new PropertyOverrideComparator();


  @Override
  public ResolutionResult resolve(
      final List<Pair<String, Object>> propertiesOverrides,
      final ResolutionMetaData resolutionMetadata) {

    final ResolutionResult result = new ResolutionResult();
    final Map<String, Object> resultMap = new HashMap<>();
    final DataSchema dataSchema = resolutionMetadata.getDataSchemaUnderResolution();

    if (dataSchema != null) {
      resultMap.putAll(dataSchema.getResolvedProperties());
    }

    // If no overrides, take the default.
    if (propertiesOverrides.size() == 0) {
      result.setResolvedResult(resultMap);
      return result;
    }

    // If overrides, choose the most nested override. TODO: Revisit this decision.
    propertiesOverrides.sort(_comparator);

    resultMap.put(getAnnotationNamespace(), propertiesOverrides.get(0).getValue());
    result.setResolvedResult(resultMap);

    return result;
  }


  @Override
  public String getAnnotationNamespace() {
    return RelationshipAnnotation.ANNOTATION_NAME;
  }

  @Override
  public AnnotationValidationResult validate(
      final Map<String, Object> resolvedProperties,
      final ValidationMetaData metaData) {

    if (metaData.getDataSchema() == null) {
      return VALID_RESULT;
    }

    if (resolvedProperties.get(getAnnotationNamespace()) == null) {
      return VALID_RESULT;
    }

    final DataSchema currentSchema = metaData.getDataSchema();

    if (currentSchema.getDereferencedDataSchema().isComplex()) {
      final ComplexDataSchema complexSchema = (ComplexDataSchema) currentSchema;
      if (isValidComplexType(complexSchema)) {
        final ArrayDataSchema arraySchema = (ArrayDataSchema) complexSchema;
        final DataSchema itemSchema = arraySchema.getItems().getDereferencedDataSchema();
        if (itemSchema.isPrimitive() && isValidPrimitiveType((PrimitiveDataSchema) itemSchema)) {

          final Object annotationObj = resolvedProperties.get(getAnnotationNamespace());
          final PathSpec path = new PathSpec(metaData.getPathToSchema());
          final RelationshipAnnotation annotation = RelationshipAnnotation.fromPegasusAnnotationObject(
              annotationObj,
              metaData.getPathToSchema().toString()
          );
          final RelationshipFieldSpec fieldSpec = new RelationshipFieldSpec(path, annotation, currentSchema);
          _specs.add(fieldSpec);

          return VALID_RESULT;
        }
      }

      final AnnotationValidationResult failedResult = new AnnotationValidationResult();
      failedResult.setValid(false);
      final MessageList<Message> errorMessages = new MessageList<>();
      errorMessages.add(new Message(metaData.getPathToSchema().toArray(), String.format("Invalid @%s Annotation: Incorrect field schema type of %s",
          getAnnotationNamespace(),
          currentSchema.getDereferencedType().toString()
      )));
      failedResult.setMessages(errorMessages);
      return failedResult;
    }

    if (isValidPrimitiveType((PrimitiveDataSchema) currentSchema)) {

      final Object annotationObj = resolvedProperties.get(getAnnotationNamespace());
      final PathSpec path = new PathSpec(metaData.getPathToSchema());
      final RelationshipAnnotation annotation = RelationshipAnnotation.fromPegasusAnnotationObject(
          annotationObj,
          metaData.getPathToSchema().toString()
      );
      final RelationshipFieldSpec fieldSpec = new RelationshipFieldSpec(path, annotation, currentSchema);
      _specs.add(fieldSpec);

      return VALID_RESULT;
    }

    final AnnotationValidationResult failedResult = new AnnotationValidationResult();
    failedResult.setValid(false);
    final MessageList<Message> errorMessages = new MessageList<>();
    errorMessages.add(new Message(metaData.getPathToSchema().toArray(), String.format("Invalid @%s Annotation: Incorrect field schema type of %s",
        getAnnotationNamespace(),
        currentSchema.getDereferencedType().toString()
    )));
    failedResult.setMessages(errorMessages);
    return failedResult;
  }

  public List<RelationshipFieldSpec> getSpecs() {
    return _specs;
  }

  private Boolean isValidComplexType(final ComplexDataSchema schema) {
    return DataSchema.Type.ARRAY.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }

  private Boolean isValidPrimitiveType(final PrimitiveDataSchema schema) {
    return DataSchema.Type.STRING.equals(schema.getDereferencedDataSchema().getDereferencedType());
  }
}
