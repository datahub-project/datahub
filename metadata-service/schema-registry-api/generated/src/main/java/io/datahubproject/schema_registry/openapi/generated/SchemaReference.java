package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Schema reference */
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema reference")
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaReference {

  @JsonProperty("name")
  private String name = null;

  @JsonProperty("subject")
  private String subject = null;

  @JsonProperty("version")
  private Integer version = null;

  public SchemaReference name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Reference name
   *
   * @return name
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "io.confluent.kafka.example.User",
      description = "Reference name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SchemaReference subject(String subject) {
    this.subject = subject;
    return this;
  }

  /**
   * Name of the referenced subject
   *
   * @return subject
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "User",
      description = "Name of the referenced subject")
  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public SchemaReference version(Integer version) {
    this.version = version;
    return this;
  }

  /**
   * Version number of the referenced subject
   *
   * @return version
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "1",
      description = "Version number of the referenced subject")
  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaReference schemaReference = (SchemaReference) o;
    return Objects.equals(this.name, schemaReference.name)
        && Objects.equals(this.subject, schemaReference.subject)
        && Objects.equals(this.version, schemaReference.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, subject, version);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaReference {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    subject: ").append(toIndentedString(subject)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
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
