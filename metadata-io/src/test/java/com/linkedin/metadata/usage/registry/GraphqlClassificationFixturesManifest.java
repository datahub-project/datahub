package com.linkedin.metadata.usage.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class GraphqlClassificationFixturesManifest {
  private List<FixtureCase> cases;

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class FixtureCase {
    private String operationName;
    private String kind;
    private List<String> rootFields;
    private String expected;
  }
}
