package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Schema definition */
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema definition")
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaString {

  @JsonProperty("schemaType")
  private String schemaType = null;

  @JsonProperty("schema")
  private String schema = null;

  @JsonProperty("references")
  @Valid
  private List<SchemaReference> references = null;

  @JsonProperty("maxId")
  private Integer maxId = null;

  public SchemaString schemaType(String schemaType) {
    this.schemaType = schemaType;
    return this;
  }

  /**
   * Schema type
   *
   * @return schemaType
   */
  @io.swagger.v3.oas.annotations.media.Schema(example = "AVRO", description = "Schema type")
  public String getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  public SchemaString schema(String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Schema string identified by the ID
   *
   * @return schema
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "{\"schema\": \"{\"type\": \"string\"}\"}",
      description = "Schema string identified by the ID")
  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public SchemaString references(List<SchemaReference> references) {
    this.references = references;
    return this;
  }

  public SchemaString addReferencesItem(SchemaReference referencesItem) {
    if (this.references == null) {
      this.references = new ArrayList<>();
    }
    this.references.add(referencesItem);
    return this;
  }

  /**
   * References to other schemas
   *
   * @return references
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "References to other schemas")
  @Valid
  public List<SchemaReference> getReferences() {
    return references;
  }

  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  public SchemaString maxId(Integer maxId) {
    this.maxId = maxId;
    return this;
  }

  /**
   * Maximum ID
   *
   * @return maxId
   */
  @io.swagger.v3.oas.annotations.media.Schema(example = "1", description = "Maximum ID")
  public Integer getMaxId() {
    return maxId;
  }

  public void setMaxId(Integer maxId) {
    this.maxId = maxId;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaString schemaString = (SchemaString) o;
    return Objects.equals(this.schemaType, schemaString.schemaType)
        && Objects.equals(this.schema, schemaString.schema)
        && Objects.equals(this.references, schemaString.references)
        && Objects.equals(this.maxId, schemaString.maxId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, schema, references, maxId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaString {\n");

    sb.append("    schemaType: ").append(toIndentedString(schemaType)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    references: ").append(toIndentedString(references)).append("\n");
    sb.append("    maxId: ").append(toIndentedString(maxId)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
