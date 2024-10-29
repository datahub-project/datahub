package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import jakarta.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** SchemaRegistryServerVersion */
@Validated
@jakarta.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaRegistryServerVersion {

  @JsonProperty("version")
  private String version = null;

  @JsonProperty("commitId")
  private String commitId = null;

  public SchemaRegistryServerVersion version(String version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   *
   * @return version
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public SchemaRegistryServerVersion commitId(String commitId) {
    this.commitId = commitId;
    return this;
  }

  /**
   * Get commitId
   *
   * @return commitId
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "")
  public String getCommitId() {
    return commitId;
  }

  public void setCommitId(String commitId) {
    this.commitId = commitId;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaRegistryServerVersion schemaRegistryServerVersion = (SchemaRegistryServerVersion) o;
    return Objects.equals(this.version, schemaRegistryServerVersion.version)
        && Objects.equals(this.commitId, schemaRegistryServerVersion.commitId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, commitId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaRegistryServerVersion {\n");

    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    commitId: ").append(toIndentedString(commitId)).append("\n");
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
