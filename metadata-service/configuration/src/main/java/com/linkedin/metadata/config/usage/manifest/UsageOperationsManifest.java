package com.linkedin.metadata.config.usage.manifest;

import java.util.List;
import java.util.Map;
import lombok.Data;

/** YAML shape for {@code usage_operations.yaml}. */
@Data
public class UsageOperationsManifest {

  private Map<String, UsageOperationDefinition> usageOperations;

  @Data
  public static class UsageOperationDefinition {
    private String description;
    private List<String> requestApis;
    private String activityClass;
    private Integer defaultCostUnits;
    private boolean ingestionEndpoint;
    private GraphqlClassification graphql;
  }

  @Data
  public static class GraphqlClassification {
    private List<String> names;
    private List<String> patterns;

    /** Legacy alias merged into {@link #names} by {@code GraphqlClassificationEntries}. */
    private List<String> operationNames;

    /** Legacy alias merged into {@link #names} by {@code GraphqlClassificationEntries}. */
    private List<String> rootFields;
  }
}
