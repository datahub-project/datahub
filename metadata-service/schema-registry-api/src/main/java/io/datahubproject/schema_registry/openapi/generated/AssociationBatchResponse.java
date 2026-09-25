package io.datahubproject.schema_registry.openapi.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.validation.annotation.Validated;

/** Batch result wrapper for Stream Governance association lookups. */
@io.swagger.v3.oas.annotations.media.Schema(description = "AssociationBatchResponse")
@Validated
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AssociationBatchResponse {

  @JsonProperty("results")
  private List<Object> results = new ArrayList<>();

  public List<Object> getResults() {
    return results;
  }

  public void setResults(List<Object> results) {
    this.results = results;
  }

  public AssociationBatchResponse results(List<Object> results) {
    this.results = results;
    return this;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationBatchResponse other = (AssociationBatchResponse) o;
    return Objects.equals(this.results, other.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(results);
  }
}
