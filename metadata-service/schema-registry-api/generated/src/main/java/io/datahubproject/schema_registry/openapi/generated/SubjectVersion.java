package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Subject version pair */
@io.swagger.v3.oas.annotations.media.Schema(description = "Subject version pair")
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubjectVersion {

  @JsonProperty("subject")
  private String subject = null;

  @JsonProperty("version")
  private Integer version = null;

  public SubjectVersion subject(String subject) {
    this.subject = subject;
    return this;
  }

  /**
   * Name of the subject
   *
   * @return subject
   */
  @io.swagger.v3.oas.annotations.media.Schema(example = "User", description = "Name of the subject")
  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public SubjectVersion version(Integer version) {
    this.version = version;
    return this;
  }

  /**
   * Version number
   *
   * @return version
   */
  @io.swagger.v3.oas.annotations.media.Schema(example = "1", description = "Version number")
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
    SubjectVersion subjectVersion = (SubjectVersion) o;
    return Objects.equals(this.subject, subjectVersion.subject)
        && Objects.equals(this.version, subjectVersion.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, version);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SubjectVersion {\n");

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
