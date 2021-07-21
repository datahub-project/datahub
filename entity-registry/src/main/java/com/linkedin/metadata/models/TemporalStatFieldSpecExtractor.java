package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.SchemaVisitor;
import com.linkedin.data.schema.annotation.SchemaVisitorTraversalResult;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.TemporalStatAnnotation;
import com.linkedin.metadata.models.annotation.TemporalStatCollectionAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;


/**
 * Implementation of {@link SchemaVisitor} responsible for extracting {@link TemporalStatFieldSpec} and
 * {@link TemporalStatCollectionFieldSpec} from an aspect schema.
 */
@Getter
public class TemporalStatFieldSpecExtractor implements SchemaVisitor {

  private final List<TemporalStatFieldSpec> temporalStatFieldSpecs = new ArrayList<>();
  private final List<TemporalStatCollectionFieldSpec> temporalStatCollectionFieldSpecs = new ArrayList<>();
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
      final Object temporalStatAnnotationObj = primaryProperties.get(TemporalStatAnnotation.ANNOTATION_NAME);
      final Object temporalStatCollectionAnnotationObj =
          primaryProperties.get(TemporalStatCollectionAnnotation.ANNOTATION_NAME);
      if (currentSchema.getType() == DataSchema.Type.RECORD && temporalStatCollectionAnnotationObj != null) {
        validateCollectionAnnotation(currentSchema, temporalStatCollectionAnnotationObj,
            context.getTraversePath().toString());
        addTemporalStatCollectionFieldSpec(currentSchema, path, temporalStatCollectionAnnotationObj);
      } else if (temporalStatAnnotationObj != null && !path.getPathComponents()
          .get(path.getPathComponents().size() - 1)
          .equals("*")) { // For arrays make sure to add just the array form
        addTemporalStatFieldSpec(currentSchema, path, temporalStatAnnotationObj);
      } else {
        addTemporalStatCollectionKey(path);
      }
    }
  }

  private void validateCollectionAnnotation(DataSchema currentSchema, Object annotationObj, String pathStr) {

    // If primitive, assume the annotation is well formed until resolvedProperties reflects it.
    if (currentSchema.isPrimitive()) {
      return;
    }

    // Required override case. If the annotation keys are not overrides, they are incorrect.
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared inside %s: Invalid value type provided (Expected Map)",
          TemporalStatCollectionAnnotation.ANNOTATION_NAME, pathStr));
    }
  }

  private void addTemporalStatCollectionFieldSpec(DataSchema currentSchema, PathSpec path, Object annotationObj) {
    if (currentSchema.getType() == DataSchema.Type.RECORD) {
      TemporalStatCollectionAnnotation annotation =
          TemporalStatCollectionAnnotation.fromPegasusAnnotationObject(annotationObj,
              FieldSpecUtils.getSchemaFieldName(path), path.toString());
      if (namesToPath.containsKey(annotation.getCollectionName()) && !namesToPath.get(annotation.getCollectionName())
          .equals(path.toString())) {
        throw new ModelValidationException(
            String.format("There are multiple fields with the same name: %s", annotation.getCollectionName()));
      }
      namesToPath.put(annotation.getCollectionName(), path.toString());
      temporalStatCollectionFieldSpecs.add(
          //TODO: Figure out the correct value of keypath to pass in here
          new TemporalStatCollectionFieldSpec(path, annotation, new ArrayList<>(), currentSchema));
    }
  }

  private void addTemporalStatFieldSpec(DataSchema currentSchema, PathSpec path, Object annotationObj) {
    // First check whether the stat is part of a collection
    String pathStr = path.toString();
    Optional<TemporalStatCollectionFieldSpec> collectionFieldSpec = temporalStatCollectionFieldSpecs.stream()
        .filter(spec -> pathStr.startsWith(spec.getPath().toString()))
        .findFirst();
    TemporalStatAnnotation annotation =
        TemporalStatAnnotation.fromPegasusAnnotationObject(annotationObj, FieldSpecUtils.getSchemaFieldName(path),
            path.toString());
    if (collectionFieldSpec.isPresent()) {
      collectionFieldSpec.get()
          .getTemporalStats()
          .add(new TemporalStatFieldSpec(getRelativePath(path, collectionFieldSpec.get().getPath()), annotation,
              currentSchema));
    } else {
      if (path.getPathComponents().contains("*")) {
        throw new ModelValidationException(
            String.format("No matching collection found for the given temporal stat %s", pathStr));
      }
      temporalStatFieldSpecs.add(new TemporalStatFieldSpec(path, annotation, currentSchema));
    }
  }

  private void addTemporalStatCollectionKey(PathSpec path) {
    for (TemporalStatCollectionFieldSpec spec : temporalStatCollectionFieldSpecs) {
      if (path.toString().equals(spec.getKeyPathFromAnnotation())) {
        spec.setKeyPath(getRelativePath(path, spec.getPath()));
        return;
      }
    }
  }

  private PathSpec getRelativePath(PathSpec child, PathSpec parent) {
    return new PathSpec(
        child.getPathComponents().subList(parent.getPathComponents().size(), child.getPathComponents().size()));
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
