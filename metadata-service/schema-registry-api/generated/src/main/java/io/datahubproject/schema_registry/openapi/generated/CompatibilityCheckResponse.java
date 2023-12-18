package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Compatibility check response */
@io.swagger.v3.oas.annotations.media.Schema(description = "Compatibility check response")
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompatibilityCheckResponse {

  @JsonProperty("is_compatible")
  private Boolean isCompatible = null;

  @JsonProperty("messages")
  @Valid
  private List<String> messages = null;

  public CompatibilityCheckResponse isCompatible(Boolean isCompatible) {
    this.isCompatible = isCompatible;
    return this;
  }

  /**
   * Whether the compared schemas are compatible
   *
   * @return isCompatible
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      description = "Whether the compared schemas are compatible")
  public Boolean isIsCompatible() {
    return isCompatible;
  }

  public void setIsCompatible(Boolean isCompatible) {
    this.isCompatible = isCompatible;
  }

  public CompatibilityCheckResponse messages(List<String> messages) {
    this.messages = messages;
    return this;
  }

  public CompatibilityCheckResponse addMessagesItem(String messagesItem) {
    if (this.messages == null) {
      this.messages = new ArrayList<>();
    }
    this.messages.add(messagesItem);
    return this;
  }

  /**
   * Error messages
   *
   * @return messages
   */
  @io.swagger.v3.oas.annotations.media.Schema(example = "[]", description = "Error messages")
  public List<String> getMessages() {
    return messages;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompatibilityCheckResponse compatibilityCheckResponse = (CompatibilityCheckResponse) o;
    return Objects.equals(this.isCompatible, compatibilityCheckResponse.isCompatible)
        && Objects.equals(this.messages, compatibilityCheckResponse.messages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isCompatible, messages);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CompatibilityCheckResponse {\n");

    sb.append("    isCompatible: ").append(toIndentedString(isCompatible)).append("\n");
    sb.append("    messages: ").append(toIndentedString(messages)).append("\n");
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
