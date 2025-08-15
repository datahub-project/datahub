package com.linkedin.metadata.utils;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.BooleanDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.FloatDataSchema;
import com.linkedin.data.schema.IntegerDataSchema;
import com.linkedin.data.schema.Name;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.template.DynamicRecordTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.EnvelopedSystemAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.GenericPayload;
import jakarta.json.JsonPatch;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class GenericRecordUtils {
  public static final String JSON = "application/json";
  public static final String JSON_PATCH = "application/json-patch+json";

  private GenericRecordUtils() {}

  public static <T extends RecordTemplate> T copy(T input, Class<T> clazz) {
    try {
      if (input == null) {
        return null;
      }
      Constructor<T> constructor = clazz.getConstructor(DataMap.class);
      return constructor.newInstance(input.data().copy());
    } catch (CloneNotSupportedException
        | InvocationTargetException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Deserialize the given value into the aspect based on the input aspectSpec */
  @Nonnull
  public static RecordTemplate deserializeAspect(
      @Nonnull ByteString aspectValue,
      @Nonnull String contentType,
      @Nonnull AspectSpec aspectSpec) {
    return deserializeAspect(aspectValue, contentType, aspectSpec.getDataTemplateClass());
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializeAspect(
      @Nonnull ByteString aspectValue, @Nonnull String contentType, @Nonnull Class<T> clazz) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(
          String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(clazz, aspectValue.asString(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializePayload(
      @Nonnull ByteString payloadValue, @Nonnull String contentType, @Nonnull Class<T> clazz) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(
          String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(clazz, payloadValue.asString(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializePayload(
      @Nonnull ByteString payloadValue, @Nonnull Class<T> clazz) {
    return deserializePayload(payloadValue, JSON, clazz);
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull RecordTemplate aspect) {
    return serializeAspect(RecordUtils.toJsonString(aspect));
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull String str) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(str.getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON);
    return genericAspect;
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull JsonNode json) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(json.toString().getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON);
    return genericAspect;
  }

  @Nonnull
  public static GenericAspect serializePatch(@Nonnull JsonPatch jsonPatch) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.unsafeWrap(jsonPatch.toString().getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON_PATCH);
    return genericAspect;
  }

  @Nonnull
  public static GenericAspect serializePatch(@Nonnull JsonNode jsonPatch) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.unsafeWrap(jsonPatch.toString().getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON_PATCH);
    return genericAspect;
  }

  @Nonnull
  public static GenericAspect serializePatch(
      @Nonnull GenericJsonPatch jsonPatch, @Nonnull ObjectMapper objectMapper) {
    try {
      GenericAspect genericAspect = new GenericAspect();
      genericAspect.setValue(
          ByteString.unsafeWrap(
              objectMapper.writeValueAsString(jsonPatch).getBytes(StandardCharsets.UTF_8)));
      genericAspect.setContentType(GenericRecordUtils.JSON_PATCH);
      return genericAspect;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static GenericPayload serializePayload(@Nonnull RecordTemplate payload) {
    GenericPayload genericPayload = new GenericPayload();
    genericPayload.setValue(
        ByteString.unsafeWrap(RecordUtils.toJsonString(payload).getBytes(StandardCharsets.UTF_8)));
    genericPayload.setContentType(GenericRecordUtils.JSON);
    return genericPayload;
  }

  @Nonnull
  public static Map<Urn, Map<String, Aspect>> entityResponseToAspectMap(
      Map<Urn, EntityResponse> inputMap) {
    return inputMap.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().getAspects().entrySet().stream()
                        .map(
                            aspectEntry ->
                                Map.entry(aspectEntry.getKey(), aspectEntry.getValue().getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Nonnull
  public static Map<Urn, Map<String, SystemAspect>> entityResponseToSystemAspectMap(
      Map<Urn, EntityResponse> inputMap, @Nonnull EntityRegistry entityRegistry) {
    return inputMap.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().getAspects().entrySet().stream()
                        .filter(aspectEntry -> aspectEntry.getValue() != null)
                        .map(
                            aspectEntry ->
                                Map.entry(
                                    aspectEntry.getKey(),
                                    EnvelopedSystemAspect.of(
                                        entry.getKey(),
                                        aspectEntry.getValue(),
                                        entityRegistry.getEntitySpec(
                                            entry.getKey().getEntityType()))))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Converts a JsonNode to a DynamicRecordTemplate. Useful for API responses where we're not going
   * to persist the RecordTemplate.
   *
   * @param rootNode json node
   * @param schemaName some internal name for the schema that is inferred
   * @return a RecordTemplate object
   */
  public static DynamicRecordTemplate fromJson(JsonNode rootNode, String schemaName) {
    // Create the schema
    RecordDataSchema schema = inferSchemaFromJsonNode(rootNode, schemaName);

    // Convert JsonNode to DataMap
    DataMap dataMap = toDataMap(rootNode);

    // Create and return the DynamicRecordTemplate
    return new DynamicRecordTemplate(dataMap, schema);
  }

  private static RecordDataSchema inferSchemaFromJsonNode(JsonNode rootNode, String schemaName) {
    // Create name object for the schema
    Name name = new Name(schemaName);

    // Create the record schema
    RecordDataSchema schema = new RecordDataSchema(name, RecordDataSchema.RecordType.RECORD);

    if (rootNode.isObject()) {
      // Create list to hold all fields
      List<RecordDataSchema.Field> fields = new ArrayList<>();

      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = fieldsIterator.next();
        String fieldName = entry.getKey();
        JsonNode fieldValue = entry.getValue();

        // Create field schema based on the value type
        DataSchema fieldSchema = inferFieldSchema(fieldValue, fieldName);

        // Create and configure the field
        RecordDataSchema.Field field = new RecordDataSchema.Field(fieldSchema);

        // Set the field name
        StringBuilder errorBuilder = new StringBuilder();
        boolean nameSetSuccessfully = field.setName(fieldName, errorBuilder);
        if (!nameSetSuccessfully) {
          System.err.println("Warning: Field name issue: " + errorBuilder.toString());
          continue;
        }

        // Set optional if value is null
        if (fieldValue.isNull()) {
          field.setOptional(true);
        }

        // Add field to list
        fields.add(field);
      }

      // Set all fields in the schema with proper error handling
      StringBuilder errorBuilder = new StringBuilder();
      boolean fieldsSetSuccessfully = schema.setFields(fields, errorBuilder);
      if (!fieldsSetSuccessfully) {
        System.err.println("Warning: Schema field issues: " + errorBuilder.toString());
      }
    }

    return schema;
  }

  private static DataSchema inferFieldSchema(JsonNode node, String baseName) {
    if (node.isTextual()) {
      return new StringDataSchema();
    } else if (node.isNumber()) {
      if (node.isInt()) {
        return new IntegerDataSchema();
      } else {
        return new FloatDataSchema();
      }
    } else if (node.isBoolean()) {
      return new BooleanDataSchema();
    } else if (node.isObject()) {
      return inferSchemaFromJsonNode(node, baseName + "Record");
    } else if (node.isArray()) {
      // For simplicity, use first element to determine item type
      if (node.size() > 0) {
        DataSchema itemSchema = inferFieldSchema(node.get(0), baseName + "Item");
        return new ArrayDataSchema(itemSchema);
      } else {
        // Default to string schema for empty arrays
        return new ArrayDataSchema(new StringDataSchema());
      }
    } else if (node.isNull()) {
      // For null values, default to string schema
      return new StringDataSchema();
    }

    // Default to string for unknown types
    return new StringDataSchema();
  }

  private static DataMap toDataMap(JsonNode node) {
    if (node == null || !node.isObject()) {
      return new DataMap();
    }

    DataMap dataMap = new DataMap();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String key = entry.getKey();
      JsonNode value = entry.getValue();

      // Only add non-null values to the DataMap, as DataMap doesn't accept null values
      if (!value.isNull()) {
        Object convertedValue = convertJsonValue(value);
        if (convertedValue != null) {
          dataMap.put(key, convertedValue);
        }
      }
    }

    return dataMap;
  }

  private static Object convertJsonValue(JsonNode node) {
    if (node.isNull()) {
      // Return null, which will be handled by the caller
      return null;
    } else if (node.isTextual()) {
      return node.textValue();
    } else if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isDouble() || node.isFloat()) {
      return node.doubleValue();
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isObject()) {
      return toDataMap(node);
    } else if (node.isArray()) {
      DataList dataList = new DataList();
      for (JsonNode item : node) {
        // Skip null values in arrays
        if (!item.isNull()) {
          Object convertedItem = convertJsonValue(item);
          if (convertedItem != null) {
            dataList.add(convertedItem);
          }
        }
      }
      return dataList;
    }

    // Default - convert to string
    return node.toString();
  }
}
