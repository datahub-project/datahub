package com.linkedin.metadata.test.query.schemafield;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.test.query.TestQuery;
import javax.annotation.Nonnull;

public class SchemaFieldUtils {
  public static final String SCHEMA_FIELDS_PROPERTY = "schemaFields";
  public static final String SCHEMA_FIELDS_LENGTH_PROPERTY = "schemaFields.length";

  private static final ObjectMapper mapper = new ObjectMapper();

  public static boolean isSchemaFieldsQuery(@Nonnull final TestQuery query) {
    if (!query.getQueryParts().isEmpty()) {
      return SCHEMA_FIELDS_PROPERTY.equals(query.getQuery())
          || SCHEMA_FIELDS_LENGTH_PROPERTY.equals(query.getQuery());
    }
    return false;
  }

  public static String serializeSchemaField(SchemaField schemaField) {
    try {
      return mapper.writeValueAsString(schemaField);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static SchemaField deserializeSchemaField(String json) {
    try {
      return mapper.readValue(json, SchemaField.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SchemaFieldUtils() {}
}
