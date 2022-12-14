package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import org.springframework.validation.annotation.Validated;


/**
 * Schema
 */
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2022-12-13T18:00:00.821813Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Schema {

  @JsonProperty("subject")
  private String subject = null;

  @JsonProperty("version")
  private Integer version = null;

  @JsonProperty("id")
  private Integer id = null;

  @JsonProperty("schemaType")
  private String schemaType = null;

  @JsonProperty("references")
  @Valid
  private List<SchemaReference> references = null;

  @JsonProperty("schema")
  private String schema = null;

  public Schema subject(String subject) {
    this.subject = subject;
    return this;
  }

  /**
   * Get subject
   * @return subject
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public Schema version(Integer version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   * @return version
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public Schema id(Integer id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Schema schemaType(String schemaType) {
    this.schemaType = schemaType;
    return this;
  }

  /**
   * Get schemaType
   * @return schemaType
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")

  public String getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  public Schema references(List<SchemaReference> references) {
    this.references = references;
    return this;
  }

  public Schema addReferencesItem(SchemaReference referencesItem) {
    if (this.references == null) {
      this.references = new ArrayList<>();
    }
    this.references.add(referencesItem);
    return this;
  }

  /**
   * Get references
   * @return references
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")
  @Valid
  public List<SchemaReference> getReferences() {
    return references;
  }

  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  public Schema schema(String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Get schema
   * @return schema
   **/
  @io.swagger.v3.oas.annotations.media.Schema(description = "")

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema schema = (Schema) o;
    return Objects.equals(this.subject, schema.subject) && Objects.equals(this.version, schema.version)
        && Objects.equals(this.id, schema.id) && Objects.equals(this.schemaType, schema.schemaType) && Objects.equals(
        this.references, schema.references) && Objects.equals(this.schema, schema.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, version, id, schemaType, references, schema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Schema {\n");

    sb.append("    subject: ").append(toIndentedString(subject)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    schemaType: ").append(toIndentedString(schemaType)).append("\n");
    sb.append("    references: ").append(toIndentedString(references)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}