package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import jakarta.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Error message */
@io.swagger.v3.oas.annotations.media.Schema(description = "Error message")
@Validated
@jakarta.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorMessage {

  @JsonProperty("error_code")
  private Integer errorCode = null;

  @JsonProperty("message")
  private String message = null;

  public ErrorMessage errorCode(Integer errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  /**
   * Error code
   *
   * @return errorCode
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "Error code")
  public Integer getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(Integer errorCode) {
    this.errorCode = errorCode;
  }

  public ErrorMessage message(String message) {
    this.message = message;
    return this;
  }

  /**
   * Detailed error message
   *
   * @return message
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "Detailed error message")
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorMessage errorMessage = (ErrorMessage) o;
    return Objects.equals(this.errorCode, errorMessage.errorCode)
        && Objects.equals(this.message, errorMessage.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, message);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ErrorMessage {\n");

    sb.append("    errorCode: ").append(toIndentedString(errorCode)).append("\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
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
