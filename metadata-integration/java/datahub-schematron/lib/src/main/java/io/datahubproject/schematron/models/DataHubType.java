package io.datahubproject.schematron.models;

import com.linkedin.data.template.StringArray;
import com.linkedin.schema.*;
import lombok.Data;

@Data
public class DataHubType {
  private Class type;
  private String nestedType;

  public DataHubType(Class type, String nestedType) {
    this.type = type;
    this.nestedType = nestedType;
  }

  public SchemaFieldDataType asSchemaFieldType() {
    if (type == UnionType.class) {
      return new SchemaFieldDataType()
          .setType(
              SchemaFieldDataType.Type.create(
                  new UnionType()
                      .setNestedTypes(nestedType != null ? new StringArray(nestedType) : null)));
    } else if (type == ArrayType.class) {
      return new SchemaFieldDataType()
          .setType(
              SchemaFieldDataType.Type.create(
                  new ArrayType()
                      .setNestedType(nestedType != null ? new StringArray(nestedType) : null)));
    } else if (type == MapType.class) {
      return new SchemaFieldDataType()
          .setType(
              SchemaFieldDataType.Type.create(
                  new MapType()
                      .setKeyType("string")
                      .setValueType(nestedType != null ? nestedType : null)));
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }
}
