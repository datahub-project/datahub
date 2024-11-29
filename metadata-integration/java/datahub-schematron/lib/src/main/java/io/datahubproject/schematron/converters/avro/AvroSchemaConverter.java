package io.datahubproject.schematron.converters.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.schema.*;
import io.datahubproject.schematron.converters.SchemaConverter;
import io.datahubproject.schematron.models.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/** Converts Avro schemas to DataHub's schema format following SchemaFieldPath Specification V2. */
@Slf4j
@Builder
public class AvroSchemaConverter implements SchemaConverter<Schema> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Map<String, Supplier<SchemaFieldDataType.Type>> LOGICAL_TYPE_MAPPING;

  static {
    Map<String, Supplier<SchemaFieldDataType.Type>> logicalTypeMap = new HashMap<>();
    logicalTypeMap.put("date", () -> SchemaFieldDataType.Type.create(new DateType()));
    logicalTypeMap.put("time-micros", () -> SchemaFieldDataType.Type.create(new TimeType()));
    logicalTypeMap.put("time-millis", () -> SchemaFieldDataType.Type.create(new TimeType()));
    logicalTypeMap.put("timestamp-micros", () -> SchemaFieldDataType.Type.create(new TimeType()));
    logicalTypeMap.put("timestamp-millis", () -> SchemaFieldDataType.Type.create(new TimeType()));
    logicalTypeMap.put("decimal", () -> SchemaFieldDataType.Type.create(new NumberType()));
    logicalTypeMap.put("uuid", () -> SchemaFieldDataType.Type.create(new StringType()));
    LOGICAL_TYPE_MAPPING = Collections.unmodifiableMap(logicalTypeMap);
  }

  private SchemaFieldDataType.Type getTypeFromLogicalType(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      Supplier<SchemaFieldDataType.Type> typeSupplier =
          LOGICAL_TYPE_MAPPING.get(logicalType.getName());
      if (typeSupplier != null) {
        return typeSupplier.get();
      }
    }
    return getBaseType(schema);
  }

  private SchemaFieldDataType.Type getBaseType(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return SchemaFieldDataType.Type.create(new BooleanType());
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return SchemaFieldDataType.Type.create(new NumberType());
      case STRING:
        return SchemaFieldDataType.Type.create(new StringType());
      case BYTES:
        return SchemaFieldDataType.Type.create(new BytesType());
      case FIXED:
        return SchemaFieldDataType.Type.create(new FixedType());
      case ENUM:
        return SchemaFieldDataType.Type.create(new EnumType());
      case ARRAY:
        return SchemaFieldDataType.Type.create(new ArrayType());
      case MAP:
        return SchemaFieldDataType.Type.create(new MapType());
      case RECORD:
        return SchemaFieldDataType.Type.create(new RecordType());
      case UNION:
        return SchemaFieldDataType.Type.create(new UnionType());
      default:
        return SchemaFieldDataType.Type.create(new NullType());
    }
  }

  private String getFieldType(Schema schema) {
    // For the field path, we just want the base type without the logical type
    return schema.getType().getName().toLowerCase();
  }

  private String getNativeDataType(Schema schema) {
    // For native data type, we can include the logical type information
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      return schema.getType().getName().toLowerCase() + "(" + logicalType.getName() + ")";
    }
    return schema.getType().getName().toLowerCase();
  }

  @Override
  public SchemaMetadata toDataHubSchema(
      Schema schema,
      boolean isKeySchema,
      boolean defaultNullable,
      DataPlatformUrn platformUrn,
      String rawSchemaString) {

    try {
      byte[] fingerprintBytes = null;
      try {
        if (rawSchemaString != null) {
          String canonicalForm = SchemaNormalization.toParsingForm(schema);
          log.debug("Length of canonical form: {}", canonicalForm.length());
          log.debug("Canonical form: {}", canonicalForm);
          fingerprintBytes =
              SchemaNormalization.fingerprint(
                  "MD5", rawSchemaString.getBytes(StandardCharsets.UTF_8));
        } else {
          fingerprintBytes = SchemaNormalization.parsingFingerprint("MD5", schema);
        }
      } catch (Exception e) {
        log.error("Failed to compute schema fingerprint", e);
      }

      String schemaHash = "";
      if (fingerprintBytes != null) {
        // Convert to hex string
        StringBuilder hexString = new StringBuilder();
        for (byte b : fingerprintBytes) {
          hexString.append(String.format("%02x", b));
        }
        schemaHash = hexString.toString();
      }

      List<SchemaField> fields = new ArrayList<>();
      FieldPath basePath = new FieldPath();
      basePath.setKeySchema(isKeySchema);

      // Add the record type to the base path
      if (schema.getType() == Schema.Type.RECORD) {
        basePath = basePath.expandType(schema.getName(), schema.toString());
      }

      processSchema(schema, basePath, defaultNullable, fields);

      return new SchemaMetadata()
          .setSchemaName(schema.getName())
          .setPlatform(platformUrn)
          .setVersion(0)
          .setHash(schemaHash)
          .setPlatformSchema(
              SchemaMetadata.PlatformSchema.create(
                  new OtherSchema().setRawSchema(schema.toString())))
          .setFields(new SchemaFieldArray(fields));

    } catch (Exception e) {
      log.error("Failed to convert Avro schema", e);
      throw new RuntimeException("Failed to convert Avro schema", e);
    }
  }

  private void processSchema(
      Schema schema, FieldPath fieldPath, boolean defaultNullable, List<SchemaField> fields) {
    if (schema.getType() == Schema.Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        processField(field, fieldPath, defaultNullable, fields);
      }
    }
  }

  private void processField(
      Schema.Field field, FieldPath fieldPath, boolean defaultNullable, List<SchemaField> fields) {
    processField(field, fieldPath, defaultNullable, fields, false, null);
  }

  private void processField(
      Schema.Field field,
      FieldPath fieldPath,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean nullableOverride) {
    processField(field, fieldPath, defaultNullable, fields, nullableOverride, null);
  }

  private void processField(
      Schema.Field field,
      FieldPath fieldPath,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean nullableOverride,
      DataHubType typeOverride) {
    log.debug(
        "Processing field: {}, Field path : {}, Field schema: {}",
        field.name(),
        fieldPath.asString(),
        field.schema());
    Schema fieldSchema = field.schema();
    boolean isNullable = isNullable(fieldSchema, defaultNullable);
    if (nullableOverride) {
      // If a nullable override is provided, use the override value
      isNullable = true;
    }
    if (typeOverride != null) {
      // If a type override is provided, use the nullable value from the override
      isNullable = nullableOverride;
    }
    log.debug(
        "DefaultNullability: {}, Determined nullability for field name: {} at path: {} is {}",
        defaultNullable,
        field.name(),
        fieldPath.asString(),
        isNullable);
    String discriminatedType = getDiscriminatedType(fieldSchema);

    FieldElement element =
        new FieldElement(new ArrayList<>(), new ArrayList<>(), field.name(), typeOverride);

    FieldPath newPath = fieldPath.clonePlus(element);

    switch (fieldSchema.getType()) {
      case RECORD:
        processRecordField(
            field, newPath, discriminatedType, defaultNullable, fields, isNullable, typeOverride);
        break;
      case ARRAY:
        processArrayField(field, newPath, discriminatedType, defaultNullable, fields, isNullable);
        break;
      case MAP:
        processMapField(field, newPath, discriminatedType, defaultNullable, fields, isNullable);
        break;
      case UNION:
        processUnionField(
            field, newPath, discriminatedType, defaultNullable, fields, isNullable, typeOverride);
        break;
      case ENUM:
        processEnumField(field, newPath, discriminatedType, defaultNullable, fields, isNullable);
        break;
      default:
        processPrimitiveField(
            field, newPath, discriminatedType, defaultNullable, fields, isNullable);
        break;
    }
  }

  private void processRecordField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable,
      DataHubType typeOverride) {

    log.debug("Record Field Path before expand: {}", fieldPath.asString());
    FieldPath recordPath = fieldPath.expandType(discriminatedType, field.schema().toString());
    log.debug("Record Field Path after expand: {}", recordPath.asString());

    SchemaFieldDataType dataType =
        typeOverride != null
            ? typeOverride.asSchemaFieldType()
            : new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));

    // Add the record field itself
    SchemaField recordField =
        new SchemaField()
            .setFieldPath(recordPath.asString())
            .setType(dataType)
            .setNativeDataType(discriminatedType)
            .setNullable(isNullable || defaultNullable)
            .setIsPartOfKey(fieldPath.isKeySchema());

    populateCommonProperties(field, recordField);

    fields.add(recordField);

    // Process nested fields
    for (Schema.Field nestedField : field.schema().getFields()) {
      processField(nestedField, recordPath, defaultNullable, fields);
    }
  }

  @SneakyThrows
  private static void populateCommonProperties(Schema.Field field, SchemaField datahubField) {
    // Create a new mutable HashMap to store combined properties
    Map<String, Object> combinedProps = new HashMap<>();

    // Add properties from field if any exist
    Map<String, Object> fieldProps = field.getObjectProps();
    if (fieldProps != null) {
      combinedProps.putAll(fieldProps);
    }

    // Add properties from schema if any exist
    Map<String, Object> schemaProps = field.schema().getObjectProps();
    if (schemaProps != null) {
      combinedProps.putAll(schemaProps);
    }

    // Only proceed with serialization if we have properties
    if (!combinedProps.isEmpty()) {
      try {
        String jsonSerializedProps = OBJECT_MAPPER.writeValueAsString(combinedProps);
        datahubField.setJsonProps(jsonSerializedProps);
      } catch (Exception e) {
        log.error(
            "Non-fatal error. Failed to serialize schema properties for field: " + field.name(), e);
      }
    }

    // Set the description if it exists
    if (field.doc() != null && !field.doc().isEmpty()) {
      datahubField.setDescription(field.doc());
      if (field.hasDefaultValue()) {
        Object defaultValue = field.defaultVal();
        // if the default value is the JSON NULL node, then we handle it differently
        if (defaultValue == JsonProperties.NULL_VALUE) {
          datahubField.setDescription(
              datahubField.getDescription() + "\nField default value: null");
        } else {
          datahubField.setDescription(
              datahubField.getDescription()
                  + "\nField default value: "
                  + OBJECT_MAPPER.writeValueAsString(defaultValue));
        }
      }
    }
  }

  private void processArrayField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable) {

    Schema arraySchema = field.schema();
    Schema elementSchema = arraySchema.getElementType();
    String elementType = getDiscriminatedType(elementSchema);

    fieldPath = fieldPath.expandType("array", arraySchema);
    // Set parent type for proper array handling
    DataHubType arrayDataHubType = new DataHubType(ArrayType.class, elementType);

    // Process element type if it's complex
    if (elementSchema.getType() == Schema.Type.RECORD
        || elementSchema.getType() == Schema.Type.ARRAY
        || elementSchema.getType() == Schema.Type.MAP
        || elementSchema.getType() == Schema.Type.UNION) {
      log.debug("Array Field Path before expand: {}", fieldPath.asString());
      fieldPath = fieldPath.popLast();
      fieldPath =
          fieldPath.clonePlus(
              new FieldElement(Collections.singletonList("array"), new ArrayList<>(), null, null));
      Schema.Field elementField =
          new Schema.Field(
              field.name(),
              elementSchema,
              elementSchema.getDoc() != null ? elementSchema.getDoc() : field.doc(),
              null // TODO: What is the default value for an array element?
              );
      processField(elementField, fieldPath, defaultNullable, fields, isNullable, arrayDataHubType);
    } else {

      SchemaField arrayField =
          new SchemaField()
              .setFieldPath(fieldPath.asString())
              .setType(arrayDataHubType.asSchemaFieldType())
              .setNativeDataType("array(" + elementType + ")")
              .setNullable(isNullable || defaultNullable)
              .setIsPartOfKey(fieldPath.isKeySchema());

      populateCommonProperties(field, arrayField);
      log.debug("Array field path: {} with doc: {}", fieldPath.asString(), field.doc());
      fields.add(arrayField);
    }
  }

  private void processMapField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable) {

    Schema mapSchema = field.schema();
    Schema valueSchema = mapSchema.getValueType();
    String valueType = getDiscriminatedType(valueSchema);

    DataHubType mapDataHubType = new DataHubType(MapType.class, valueType);
    fieldPath = fieldPath.expandType("map", mapSchema);

    // Process value type if it's complex
    if (valueSchema.getType() == Schema.Type.RECORD
        || valueSchema.getType() == Schema.Type.ARRAY
        || valueSchema.getType() == Schema.Type.MAP
        || valueSchema.getType() == Schema.Type.UNION) {
      Schema.Field valueField =
          new Schema.Field(
              field.name(),
              valueSchema,
              valueSchema.getDoc() != null ? valueSchema.getDoc() : field.doc(),
              null // TODO: What is the default value for a map value?
              ); // Nullability for map values follows the nullability of the map itself
      FieldPath valueFieldPath =
          fieldPath
              .popLast()
              .clonePlus(
                  new FieldElement(
                      Collections.singletonList("map"), new ArrayList<>(), null, null));
      processField(valueField, valueFieldPath, defaultNullable, fields, isNullable, mapDataHubType);
    } else {
      SchemaField mapField =
          new SchemaField()
              .setFieldPath(fieldPath.asString())
              .setType(mapDataHubType.asSchemaFieldType())
              .setNativeDataType("map<string," + valueType + ">")
              .setNullable(isNullable || defaultNullable)
              .setIsPartOfKey(fieldPath.isKeySchema());

      populateCommonProperties(field, mapField);
      fields.add(mapField);
    }
  }

  private void processUnionField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable,
      DataHubType typeOverride) {

    List<Schema> unionTypes = field.schema().getTypes();

    // If this is just a nullable type (union with null), process the non-null type
    // directly
    if (unionTypes.size() == 2 && isNullable) {
      Schema nonNullSchema =
          unionTypes.stream()
              .filter(s -> s.getType() != Schema.Type.NULL)
              .findFirst()
              .orElseThrow(NoSuchElementException::new);

      processField(
          new Schema.Field(field.name(), nonNullSchema, field.doc()),
          fieldPath.popLast(),
          defaultNullable,
          fields,
          true);
      return;
    }

    log.debug("Union Field Path before expand: {}", fieldPath.asString());

    // Otherwise, process as a true union type
    DataHubType unionDataHubType = new DataHubType(UnionType.class, discriminatedType);
    FieldPath unionFieldPath = fieldPath.expandType("union", field.schema().toString());
    log.debug("Union Field Path after expand: {}", unionFieldPath.asString());

    SchemaField unionField =
        new SchemaField()
            .setFieldPath(unionFieldPath.asString())
            .setType(
                typeOverride == null
                    ? unionDataHubType.asSchemaFieldType()
                    : typeOverride.asSchemaFieldType())
            .setNativeDataType("union")
            .setNullable(isNullable || defaultNullable)
            .setIsPartOfKey(fieldPath.isKeySchema());

    populateCommonProperties(field, unionField);
    fields.add(unionField);

    String unionDescription = field.doc() != null ? field.doc() : field.schema().getDoc();

    // Process each union type
    int typeIndex = 0;
    for (Schema unionSchema : unionTypes) {
      if (unionSchema.getType() != Schema.Type.NULL) {
        log.debug("TypeIndex: {}, Field path : {}", typeIndex, fieldPath.asString());
        FieldPath indexedFieldPath = fieldPath.popLast();
        indexedFieldPath =
            indexedFieldPath.clonePlus(
                new FieldElement(
                    Collections.singletonList("union"), new ArrayList<>(), null, null));
        log.debug("TypeIndex: {}, Indexed Field path : {}", typeIndex, indexedFieldPath.asString());
        // FieldPath unionFieldPath =
        // fieldPath.expandType(getDiscriminatedType(unionSchema),
        // unionSchema.toString());
        log.debug("TypeIndex: {}, Union Field path : {}", typeIndex, unionFieldPath.asString());
        String unionFieldName = field.name();
        Schema.Field unionFieldInner =
            new Schema.Field(
                unionFieldName,
                unionSchema,
                unionSchema.getDoc() != null ? unionSchema.getDoc() : unionDescription,
                null);
        log.debug(
            "TypeIndex: {}, Union Field path : {}, Doc: {}",
            typeIndex,
            unionFieldPath.asString(),
            unionFieldInner.doc());
        processField(unionFieldInner, indexedFieldPath, defaultNullable, fields);
      }
      typeIndex++;
    }
  }

  private void processEnumField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable) {

    fieldPath = fieldPath.expandType("enum", field.schema().toString());

    String enumDescription = field.doc() != null ? field.doc() : "";
    enumDescription +=
        " Allowed symbols are: " + String.join(", ", field.schema().getEnumSymbols());

    SchemaField enumField =
        new SchemaField()
            .setFieldPath(fieldPath.asString())
            .setType(
                new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType())))
            .setNativeDataType("Enum")
            .setNullable(isNullable || defaultNullable)
            .setIsPartOfKey(fieldPath.isKeySchema());

    populateCommonProperties(field, enumField);

    if (field.doc() != null && !field.doc().isEmpty()) {
      enumField.setDescription(enumDescription);
    }

    fields.add(enumField);
  }

  @SneakyThrows
  private void processPrimitiveField(
      Schema.Field field,
      FieldPath fieldPath,
      String discriminatedType,
      boolean defaultNullable,
      List<SchemaField> fields,
      boolean isNullable) {

    fieldPath = fieldPath.expandType(discriminatedType, field.schema().toString());
    SchemaField primitiveField =
        new SchemaField()
            .setFieldPath(fieldPath.asString())
            .setType(new SchemaFieldDataType().setType(getTypeFromLogicalType(field.schema())))
            .setNativeDataType(getNativeDataType(field.schema()))
            .setNullable(isNullable || defaultNullable)
            .setIsPartOfKey(fieldPath.isKeySchema());

    populateCommonProperties(field, primitiveField);

    fields.add(primitiveField);
  }

  private boolean isNullable(Schema schema, boolean defaultNullable) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }
    return defaultNullable;
  }

  /**
   * for record type we want to include the fully qualified name stripped of the namespace
   *
   * @param schema
   * @return
   */
  private String getDiscriminatedType(Schema schema) {

    if (schema.getType() == Schema.Type.RECORD) {
      if (schema.getNamespace() != null) {
        return schema.getFullName().substring(schema.getNamespace().length() + 1);
      } else {
        return schema.getFullName();
      }
    }
    return schema.getType().getName().toLowerCase();
  }

  private SchemaFieldDataType getPrimitiveFieldType(Schema schema) {

    SchemaFieldDataType fieldType = new SchemaFieldDataType();
    switch (schema.getType()) {
      case BOOLEAN:
        fieldType.setType(SchemaFieldDataType.Type.create(new BooleanType()));
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        fieldType.setType(SchemaFieldDataType.Type.create(new NumberType()));
        break;
      case STRING:
        fieldType.setType(SchemaFieldDataType.Type.create(new StringType()));
        break;
      case BYTES:
        fieldType.setType(SchemaFieldDataType.Type.create(new BytesType()));
        break;
      default:
        fieldType.setType(SchemaFieldDataType.Type.create(new NullType()));
    }
    return fieldType;
  }
}
