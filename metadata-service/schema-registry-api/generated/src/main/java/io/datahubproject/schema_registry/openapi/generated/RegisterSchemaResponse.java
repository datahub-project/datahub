package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import jakarta.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Schema register response */
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema register response")
@Validated
@jakarta.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RegisterSchemaResponse {

  @JsonProperty("id")
  private Integer id = null;

  public RegisterSchemaResponse id(Integer id) {
    this.id = id;
    return this;
  }

  /**
   * Globally unique identifier of the schema
   *
   * @return id
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "100001",
      description = "Globally unique identifier of the schema")
  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegisterSchemaResponse registerSchemaResponse = (RegisterSchemaResponse) o;
    return Objects.equals(this.id, registerSchemaResponse.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RegisterSchemaResponse {\n");

    sb.append("    id: ").append(toIndentedString(id)).append("\n");
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
