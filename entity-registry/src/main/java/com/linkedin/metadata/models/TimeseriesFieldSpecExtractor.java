package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.TimeseriesFieldAnnotation;
import com.linkedin.metadata.models.annotation.TimeseriesFieldCollectionAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link TimeseriesFieldSpec}
 * and {@link TimeseriesFieldCollectionSpec} from an aspect schema.
 */
@Getter
public class TimeseriesFieldSpecExtractor implements SchemaVisitor {

  private final List<TimeseriesFieldSpec> timeseriesFieldSpecs = new ArrayList<>();
  private final List<TimeseriesFieldCollectionSpec> timeseriesFieldCollectionSpecs =
      new ArrayList<>();
  private final Map<String, String> namesToPath = new HashMap<>();

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order) {

    if (context.getEnclosingField() == null) {
      return;
    }

    if (DataSchemaTraverse.Order.PRE_ORDER.equals(order)) {
      final DataSchema currentSchema = context.getCurrentSchema().getDereferencedDataSchema();
      final PathSpec path = new PathSpec(context.getSchemaPathSpec());

      // First, check for collection in primary properties
      final Map<String, Object> primaryProperties = context.getEnclosingField().getProperties();
      final Object timeseriesFieldAnnotationObj =
          primaryProperties.get(TimeseriesFieldAnnotation.ANNOTATION_NAME);
      final Object timeseriesFieldCollectionAnnotationObj =
          primaryProperties.get(TimeseriesFieldCollectionAnnotation.ANNOTATION_NAME);
      if (currentSchema.getType() == DataSchema.Type.RECORD
          && timeseriesFieldCollectionAnnotationObj != null) {
        validateCollectionAnnotation(
            currentSchema,
            timeseriesFieldCollectionAnnotationObj,
            context.getTraversePath().toString());
        addTimeseriesFieldCollectionSpec(
            currentSchema, path, timeseriesFieldCollectionAnnotationObj);
      } else if (timeseriesFieldAnnotationObj != null
          && !path.getPathComponents()
              .get(path.getPathComponents().size() - 1)
              .equals("*")) { // For arrays make sure to add just the array form
        addTimeseriesFieldSpec(currentSchema, path, timeseriesFieldAnnotationObj);
      } else {
        addTimeseriesFieldCollectionKey(path);
      }
    }
  }

  private void validateCollectionAnnotation(
      DataSchema currentSchema, Object annotationObj, String pathStr) {

    // If primitive, assume the annotation is well formed until resolvedProperties reflects it.
    if (currentSchema.isPrimitive()) {
      return;
    }

    // Required override case. If the annotation keys are not overrides, they are incorrect.
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared inside %s: Invalid value type provided (Expected Map)",
              TimeseriesFieldCollectionAnnotation.ANNOTATION_NAME, pathStr));
    }
  }

  private void addTimeseriesFieldCollectionSpec(
      DataSchema currentSchema, PathSpec path, Object annotationObj) {
    if (currentSchema.getType() == DataSchema.Type.RECORD) {
      TimeseriesFieldCollectionAnnotation annotation =
          TimeseriesFieldCollectionAnnotation.fromPegasusAnnotationObject(
              annotationObj, FieldSpecUtils.getSchemaFieldName(path), path.toString());
      if (namesToPath.containsKey(annotation.getCollectionName())
          && !namesToPath.get(annotation.getCollectionName()).equals(path.toString())) {
        throw new ModelValidationException(
            String.format(
                "There are multiple fields with the same name: %s",
                annotation.getCollectionName()));
      }
      namesToPath.put(annotation.getCollectionName(), path.toString());
      timeseriesFieldCollectionSpecs.add(
          new TimeseriesFieldCollectionSpec(path, annotation, new HashMap<>(), currentSchema));
    }
  }

  private void addTimeseriesFieldSpec(
      DataSchema currentSchema, PathSpec path, Object annotationObj) {
    // First check whether the stat is part of a collection
    String pathStr = path.toString();
    Optional<TimeseriesFieldCollectionSpec> fieldCollectionSpec =
        timeseriesFieldCollectionSpecs.stream()
            .filter(spec -> pathStr.startsWith(spec.getPath().toString()))
            .findFirst();
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObj, FieldSpecUtils.getSchemaFieldName(path), path.toString());
    if (fieldCollectionSpec.isPresent()) {
      fieldCollectionSpec
          .get()
          .getTimeseriesFieldSpecMap()
          .put(
              annotation.getStatName(),
              new TimeseriesFieldSpec(
                  getRelativePath(path, fieldCollectionSpec.get().getPath()),
                  annotation,
                  currentSchema));
    } else {
      if (path.getPathComponents().contains("*")) {
        throw new ModelValidationException(
            String.format(
                "No matching collection found for the given timeseries field %s", pathStr));
      }
      timeseriesFieldSpecs.add(new TimeseriesFieldSpec(path, annotation, currentSchema));
    }
  }

  private void addTimeseriesFieldCollectionKey(PathSpec path) {
    for (TimeseriesFieldCollectionSpec spec : timeseriesFieldCollectionSpecs) {
      if (path.toString().equals(spec.getKeyPathFromAnnotation())) {
        spec.setKeyPath(getRelativePath(path, spec.getPath()));
        return;
      }
    }
  }

  private PathSpec getRelativePath(PathSpec child, PathSpec parent) {
    return new PathSpec(
        child
            .getPathComponents()
            .subList(parent.getPathComponents().size(), child.getPathComponents().size()));
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
