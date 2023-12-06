package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.*;
import org.springframework.validation.annotation.Validated;

/** ServerClusterId */
@Validated
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServerClusterId {

  @JsonProperty("scope")
  @Valid
  private Map<String, Object> scope = null;

  @JsonProperty("id")
  private String id = null;

  public ServerClusterId scope(Map<String, Object> scope) {
    this.scope = scope;
    return this;
  }

  public ServerClusterId putScopeItem(String key, Object scopeItem) {
    if (this.scope == null) {
      this.scope = new HashMap<>();
    }
    this.scope.put(key, scopeItem);
    return this;
  }

  /**
   * Get scope
   *
   * @return scope
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "")
  public Map<String, Object> getScope() {
    return scope;
  }

  public void setScope(Map<String, Object> scope) {
    this.scope = scope;
  }

  public ServerClusterId id(String id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   *
   * @return id
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "")
  public String getId() {
    return id;
  }

  public void setId(String id) {
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
    ServerClusterId serverClusterId = (ServerClusterId) o;
    return Objects.equals(this.scope, serverClusterId.scope)
        && Objects.equals(this.id, serverClusterId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, id);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ServerClusterId {\n");

    sb.append("    scope: ").append(toIndentedString(scope)).append("\n");
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
