package com.linkedin.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Env / origin fields were a {@code FabricType} enum in Avro and are now strings ({@code
 * FabricTypeName}). Avro schema resolution can't read an enum as a string, so an event written with
 * an earlier schema would fail to convert. This copies such a record into an equivalent schema
 * where those enums are strings, holding the same symbol, before the schema change.
 */
final class LegacyFabricTypeAvro {

  private static final String FABRIC_TYPE_SUFFIX = ".common.FabricType";

  // Writer schemas are few (one per producer schema version), so this stays small.
  private static final Map<Schema, Schema> WIDENED =
      java.util.Collections.synchronizedMap(new IdentityHashMap<>());

  private LegacyFabricTypeAvro() {}

  /** The record itself when its schema has no FabricType enum, otherwise a widened copy. */
  @Nonnull
  static GenericRecord widen(@Nonnull GenericRecord record) {
    Schema writer = record.getSchema();
    Schema widened = WIDENED.computeIfAbsent(writer, s -> widenSchema(s, new HashMap<>()));
    if (widened == writer) {
      return record;
    }
    return (GenericRecord) copy(record, writer, widened);
  }

  private static boolean isFabricType(Schema schema) {
    return schema.getType() == Schema.Type.ENUM
        && schema.getFullName().endsWith(FABRIC_TYPE_SUFFIX);
  }

  /** Returns {@code schema} itself when nothing beneath it changes. */
  private static Schema widenSchema(Schema schema, Map<String, Schema> done) {
    switch (schema.getType()) {
      case ENUM:
        return isFabricType(schema) ? Schema.create(Schema.Type.STRING) : schema;
      case RECORD:
        {
          if (done.containsKey(schema.getFullName())) {
            return done.get(schema.getFullName());
          }
          // Assume unchanged while visiting, so recursive references terminate.
          done.put(schema.getFullName(), schema);
          List<Schema> fieldSchemas = new ArrayList<>();
          boolean changed = false;
          for (Schema.Field field : schema.getFields()) {
            Schema widened = widenSchema(field.schema(), done);
            changed |= widened != field.schema();
            fieldSchemas.add(widened);
          }
          if (!changed) {
            return schema;
          }
          Schema copy =
              Schema.createRecord(
                  schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
          done.put(schema.getFullName(), copy);
          List<Schema.Field> fields = new ArrayList<>();
          for (int i = 0; i < schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);
            Schema fieldSchema = widenSchema(field.schema(), done);
            fields.add(new Schema.Field(field.name(), fieldSchema, field.doc(), defaultFor(field)));
          }
          copy.setFields(fields);
          return copy;
        }
      case UNION:
        {
          List<Schema> branches = new ArrayList<>();
          boolean changed = false;
          for (Schema branch : schema.getTypes()) {
            Schema widened = widenSchema(branch, done);
            changed |= widened != branch;
            branches.add(widened);
          }
          return changed ? Schema.createUnion(branches) : schema;
        }
      case ARRAY:
        {
          Schema items = widenSchema(schema.getElementType(), done);
          return items == schema.getElementType() ? schema : Schema.createArray(items);
        }
      case MAP:
        {
          Schema values = widenSchema(schema.getValueType(), done);
          return values == schema.getValueType() ? schema : Schema.createMap(values);
        }
      default:
        return schema;
    }
  }

  private static Object defaultFor(Schema.Field field) {
    // An enum default becomes the same symbol as a string default.
    return field.hasDefaultValue() ? field.defaultVal() : null;
  }

  private static Object copy(Object value, Schema from, Schema to) {
    if (value == null || from == to) {
      return value;
    }
    switch (from.getType()) {
      case ENUM:
        return value.toString();
      case RECORD:
        {
          GenericRecord in = (GenericRecord) value;
          GenericData.Record out = new GenericData.Record(to);
          for (Schema.Field field : from.getFields()) {
            out.put(
                field.pos(),
                copy(
                    in.get(field.pos()), field.schema(), to.getFields().get(field.pos()).schema()));
          }
          return out;
        }
      case UNION:
        {
          int branch = GenericData.get().resolveUnion(from, value);
          return copy(value, from.getTypes().get(branch), to.getTypes().get(branch));
        }
      case ARRAY:
        {
          List<Object> out = new ArrayList<>();
          for (Object item : (Iterable<?>) value) {
            out.add(copy(item, from.getElementType(), to.getElementType()));
          }
          return out;
        }
      case MAP:
        {
          Map<Object, Object> out = new LinkedHashMap<>();
          for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            out.put(entry.getKey(), copy(entry.getValue(), from.getValueType(), to.getValueType()));
          }
          return out;
        }
      default:
        return value;
    }
  }
}
