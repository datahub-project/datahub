package com.linkedin.metadata.models;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.TraverserContext;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FieldSpecUtils {

  private FieldSpecUtils() {}

  public static String getSchemaFieldName(PathSpec pathSpec) {
    List<String> components = pathSpec.getPathComponents();
    String lastComponent = components.get(components.size() - 1);
    if (lastComponent.equals("*")) {
      return components.get(components.size() - 2);
    }
    return lastComponent;
  }

  public static Map<String, Object> getResolvedProperties(final DataSchema schema) {
    return !schema.getResolvedProperties().isEmpty()
        ? schema.getResolvedProperties()
        : schema.getProperties();
  }

  public static Optional<PathSpec> getPathSpecWithAspectName(TraverserContext context) {
    Object aspectAnnotationObj =
        context.getTopLevelSchema().getProperties().get(AspectAnnotation.ANNOTATION_NAME);
    if (aspectAnnotationObj == null
        || !Map.class.isAssignableFrom(aspectAnnotationObj.getClass())
        || !((Map) aspectAnnotationObj).containsKey(AspectAnnotation.NAME_FIELD)) {
      return Optional.empty();
    }
    String aspectName = (((Map) aspectAnnotationObj).get(AspectAnnotation.NAME_FIELD)).toString();
    return Optional.of(
        new PathSpec(
            ImmutableList.<String>builder()
                .add(aspectName)
                .addAll(context.getSchemaPathSpec())
                .build()));
  }
}
