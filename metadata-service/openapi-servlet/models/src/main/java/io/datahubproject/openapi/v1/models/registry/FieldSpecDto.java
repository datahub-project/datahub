package io.datahubproject.openapi.v1.models.registry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
public class FieldSpecDto {
  private List<String> pathComponents;
  private String fieldType;

  @JsonSerialize(using = DataSchemaSerializer.class)
  private DataSchema pegasusSchema;

  private Map<String, Object> annotations; // All annotation data
  private Map<String, Object> properties; // Other properties

  public static class DataSchemaSerializer extends JsonSerializer<DataSchema> {
    @Override
    public void serialize(DataSchema value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value != null) {
        gen.writeRawValue(value.toString());
      } else {
        gen.writeNull();
      }
    }
  }

  public static FieldSpecDto fromFieldSpec(FieldSpec spec) {
    if (spec == null) {
      return null;
    }

    FieldSpecDtoBuilder builder =
        FieldSpecDto.builder()
            .pathComponents(spec.getPath().getPathComponents())
            .fieldType(spec.getClass().getSimpleName())
            .pegasusSchema(spec.getPegasusSchema());

    Map<String, Object> annotations = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();

    // Use reflection to find all getter methods
    Method[] methods = spec.getClass().getMethods();
    for (Method method : methods) {
      String methodName = method.getName();

      // Skip common interface methods we've already handled
      if (methodName.equals("getPath")
          || methodName.equals("getPegasusSchema")
          || methodName.equals("getClass")
          || methodName.equals("hashCode")
          || methodName.equals("equals")
          || methodName.equals("toString")) {
        continue;
      }

      // Process getter methods
      if ((methodName.startsWith("get") || methodName.startsWith("is"))
          && method.getParameterCount() == 0) {

        try {
          Object value = method.invoke(spec);
          if (value != null) {
            String propertyName = extractPropertyName(methodName);

            // Sanitize the value to prevent circular references
            value = sanitizeValue(value);

            // If it's an annotation object, serialize it separately
            if (methodName.contains("Annotation")) {
              annotations.put(propertyName, value);
            } else {
              properties.put(propertyName, value);
            }
          }
        } catch (Exception e) {
          // Log error but continue processing
          System.err.println("Error processing method " + methodName + ": " + e.getMessage());
        }
      }
    }

    builder.annotations(annotations);
    builder.properties(properties);

    return builder.build();
  }

  private static String extractPropertyName(String methodName) {
    if (methodName.startsWith("get")) {
      return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
    } else if (methodName.startsWith("is")) {
      return Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
    }
    return methodName;
  }

  private static Map<String, Object> serializeAnnotation(Object annotation) {
    Map<String, Object> result = new HashMap<>();

    // Use reflection to extract all properties from the annotation
    Method[] methods = annotation.getClass().getMethods();
    for (Method method : methods) {
      String methodName = method.getName();

      if ((methodName.startsWith("get") || methodName.startsWith("is"))
          && method.getParameterCount() == 0
          && !methodName.equals("getClass")
          && !methodName.equals("hashCode")
          && !methodName.equals("equals")
          && !methodName.equals("toString")) {

        try {
          Object value = method.invoke(annotation);
          if (value != null) {

            String propertyName = extractPropertyName(methodName);

            // Sanitize the value to prevent circular references
            value = sanitizeValue(value);

            result.put(propertyName, value);
          }
        } catch (Exception e) {
          // Log error but continue
          log.error("Error processing annotation method {}: {}", methodName, e.getMessage());
        }
      }
    }

    return result;
  }

  /**
   * Sanitizes values to prevent circular references and other serialization issues. This method
   * handles special types that can cause problems during JSON serialization.
   */
  private static Object sanitizeValue(Object value) {
    if (value == null) {
      return null;
    }

    // Handle PathSpec to avoid circular references
    if (value instanceof com.linkedin.data.schema.PathSpec) {
      com.linkedin.data.schema.PathSpec pathSpec = (com.linkedin.data.schema.PathSpec) value;
      Map<String, Object> pathSpecMap = new HashMap<>();
      pathSpecMap.put("pathComponents", pathSpec.getPathComponents());
      // Explicitly don't include parent to avoid circular reference
      return pathSpecMap;
    }

    // Handle FieldSpec objects (including TimeseriesFieldSpec, RelationshipFieldSpec, etc.)
    if (value instanceof com.linkedin.metadata.models.FieldSpec) {
      // Convert to FieldSpecDto to avoid circular references
      return FieldSpecDto.fromFieldSpec((com.linkedin.metadata.models.FieldSpec) value);
    }

    // Handle DataMap
    if (value instanceof com.linkedin.data.DataMap) {
      Map<String, Object> dataMapCopy = new HashMap<>();
      ((com.linkedin.data.DataMap) value).forEach(dataMapCopy::put);
      return dataMapCopy;
    }

    // Handle Maps recursively
    if (value instanceof Map) {
      Map<String, Object> sanitizedMap = new HashMap<>();
      ((Map<?, ?>) value)
          .forEach(
              (k, v) -> {
                String key = k != null ? k.toString() : null;
                if (key != null) {
                  sanitizedMap.put(key, sanitizeValue(v));
                }
              });
      return sanitizedMap;
    }

    // Handle Collections recursively
    if (value instanceof List) {
      return ((List<?>) value)
          .stream().map(FieldSpecDto::sanitizeValue).collect(java.util.stream.Collectors.toList());
    }

    // If it's an annotation object (contains "Annotation" in class name), serialize it
    if (value.getClass().getName().contains("Annotation")) {
      return serializeAnnotation(value);
    }

    // Return primitive types and strings as-is
    return value;
  }
}
