package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;
import jakarta.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Schema Registry operating mode */
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema Registry operating mode")
@Validated
@jakarta.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Mode {

  /** Schema Registry operating mode */
  public enum ModeEnum {
    READWRITE("READWRITE"),

    READONLY("READONLY"),

    READONLY_OVERRIDE("READONLY_OVERRIDE"),

    IMPORT("IMPORT");

    private String value;

    ModeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static ModeEnum fromValue(String text) {
      for (ModeEnum b : ModeEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("mode")
  private ModeEnum mode = null;

  public Mode mode(ModeEnum mode) {
    this.mode = mode;
    return this;
  }

  /**
   * Schema Registry operating mode
   *
   * @return mode
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "READWRITE",
      description = "Schema Registry operating mode")
  public ModeEnum getMode() {
    return mode;
  }

  public void setMode(ModeEnum mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Mode mode = (Mode) o;
    return Objects.equals(this.mode, mode.mode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Mode {\n");

    sb.append("    mode: ").append(toIndentedString(mode)).append("\n");
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
