package com.linkedin.metadata.test.query.schemafield;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.test.query.TestQuery;
import javax.annotation.Nonnull;

public class TestsSchemaFieldUtils {
  public static final String SCHEMA_FIELDS_PROPERTY = "schemaFields";
  public static final String SCHEMA_FIELDS_LENGTH_PROPERTY = "schemaFields.length";
  private static final String STRUCTURED_PROPERTIES_PART = "structuredProperties";
  private static final String STRUCTURED_PROPERTIES_REGEX = "urn:li:structuredProperty:.+";
  public static final String SHARED_PROPERTIES = "shared";

  private static final ObjectMapper mapper = new ObjectMapper();

  public static boolean isSchemaFieldsQuery(@Nonnull final TestQuery query) {
    if (!query.getQueryParts().isEmpty()) {
      return SCHEMA_FIELDS_PROPERTY.equals(query.getQuery())
          || SCHEMA_FIELDS_LENGTH_PROPERTY.equals(query.getQuery())
          || isStructuredPropertySchemaFieldQuery(query)
          || isSharedStructuredPropertySchemaFieldQuery(query);
    }
    return false;
  }

  public static boolean isStructuredPropertySchemaFieldQuery(TestQuery query) {
    return query.getQueryParts().size() >= 3
        && SCHEMA_FIELDS_PROPERTY.equals(query.getQueryParts().get(0))
        && STRUCTURED_PROPERTIES_PART.equals(query.getQueryParts().get(1))
        && query.getQueryParts().get(2).matches(STRUCTURED_PROPERTIES_REGEX);
  }

  public static boolean isSharedStructuredPropertySchemaFieldQuery(TestQuery query) {
    return query.getQueryParts().size() >= 3
        && SCHEMA_FIELDS_PROPERTY.equals(query.getQueryParts().get(0))
        && STRUCTURED_PROPERTIES_PART.equals(query.getQueryParts().get(1))
        && SHARED_PROPERTIES.equals(query.getQueryParts().get(2));
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

  private TestsSchemaFieldUtils() {}
}
