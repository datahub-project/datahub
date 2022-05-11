package com.linkedin.datahub.upgrade.propagate;

import java.util.Map;
import lombok.Value;


public class EntityComparer {

  @Value
  public static class EntityCompareResult {
    double similarityScore;
    Map<String, EntityFetcher.SchemaDetails> commonFieldsWithDetails;
  }
}
