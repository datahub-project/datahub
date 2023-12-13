package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** Config */
@io.swagger.v3.oas.annotations.media.Schema(description = "Config")
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Config {

  /** Compatibility Level */
  public enum CompatibilityLevelEnum {
    BACKWARD("BACKWARD"),

    BACKWARD_TRANSITIVE("BACKWARD_TRANSITIVE"),

    FORWARD("FORWARD"),

    FORWARD_TRANSITIVE("FORWARD_TRANSITIVE"),

    FULL("FULL"),

    FULL_TRANSITIVE("FULL_TRANSITIVE"),

    NONE("NONE");

    private String value;

    CompatibilityLevelEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static CompatibilityLevelEnum fromValue(String text) {
      for (CompatibilityLevelEnum b : CompatibilityLevelEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("compatibilityLevel")
  private CompatibilityLevelEnum compatibilityLevel = null;

  public Config compatibilityLevel(CompatibilityLevelEnum compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
    return this;
  }

  /**
   * Compatibility Level
   *
   * @return compatibilityLevel
   */
  @io.swagger.v3.oas.annotations.media.Schema(
      example = "FULL_TRANSITIVE",
      description = "Compatibility Level")
  public CompatibilityLevelEnum getCompatibilityLevel() {
    return compatibilityLevel;
  }

  public void setCompatibilityLevel(CompatibilityLevelEnum compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Config config = (Config) o;
    return Objects.equals(this.compatibilityLevel, config.compatibilityLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Config {\n");

    sb.append("    compatibilityLevel: ").append(toIndentedString(compatibilityLevel)).append("\n");
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
