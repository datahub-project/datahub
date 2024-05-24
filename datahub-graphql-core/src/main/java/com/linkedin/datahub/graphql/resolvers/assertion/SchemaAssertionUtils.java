package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.Schemaless;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import com.linkedin.schema.UnionType;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SchemaAssertionUtils {

  @Nonnull
  public static SchemaMetadata createSchemaMetadata(
      @Nonnull final List<SchemaAssertionFieldInput> fields) {
    final SchemaMetadata result = new SchemaMetadata();
    final List<SchemaField> resultFields =
        fields.stream().map(SchemaAssertionUtils::createSchemaField).collect(Collectors.toList());
    result.setFields(new SchemaFieldArray(resultFields));
    // Unfortunately, these fields are required in the data model we've chosen to reuse. Just fill
    // them in with blanks
    // for now.
    result.setSchemaName("assertion-schema-name");
    result.setVersion(0L);
    try {
      result.setPlatform(
          DataPlatformUrn.createFromUrn(UrnUtils.getUrn("urn:li:dataPlatform:datahub")));
    } catch (Exception e) {
      // ignored. should never happen.
    }
    result.setHash("assertion-schema-hash");
    result.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new Schemaless()));
    return result;
  }

  @Nonnull
  public static SchemaField createSchemaField(@Nonnull final SchemaAssertionFieldInput field) {
    final SchemaField result = new SchemaField();
    result.setFieldPath(field.getPath());
    result.setType(mapSchemaFieldDataType(field.getType()));
    if (field.getNativeType() != null) {
      result.setNativeDataType(field.getNativeType());
    }
    return result;
  }

  private static SchemaFieldDataType mapSchemaFieldDataType(
      @Nonnull final com.linkedin.datahub.graphql.generated.SchemaFieldDataType type) {
    switch (type) {
      case BYTES:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()));
      case FIXED:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new FixedType()));
      case BOOLEAN:
        return new SchemaFieldDataType()
            .setType(SchemaFieldDataType.Type.create(new BooleanType()));
      case STRING:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
      case NUMBER:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
      case DATE:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType()));
      case TIME:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
      case ENUM:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType()));
      case NULL:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType()));
      case ARRAY:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
      case MAP:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType()));
      case STRUCT:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));
      case UNION:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType()));
      default:
        throw new RuntimeException(
            String.format("Unrecognized SchemaFieldDataType provided %s", type.toString()));
    }
  }

  private SchemaAssertionUtils() {}
}
