package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.SchemaFieldKey;
import javax.annotation.Nonnull;

public class SchemaFieldUtils {

  private SchemaFieldUtils() {}

  public static Urn generateSchemaFieldUrn(
      @Nonnull final String resourceUrn, @Nonnull final String fieldPath) {
    // we rely on schemaField fieldPaths to be encoded since we do that on the ingestion side
    final String encodedFieldPath =
        fieldPath.replaceAll("\\(", "%28").replaceAll("\\)", "%29").replaceAll(",", "%2C");
    final SchemaFieldKey key =
        new SchemaFieldKey().setParent(UrnUtils.getUrn(resourceUrn)).setFieldPath(encodedFieldPath);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.SCHEMA_FIELD_ENTITY_NAME);
  }
}
