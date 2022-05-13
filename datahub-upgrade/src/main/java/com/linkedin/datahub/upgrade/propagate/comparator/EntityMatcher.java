package com.linkedin.datahub.upgrade.propagate.comparator;

import com.linkedin.datahub.upgrade.propagate.EntityDetails;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;


public interface EntityMatcher {

  @Nullable
  EntityMatchResult match(EntityDetails original, Collection<EntityDetails> others);

  @Value
  @Builder
  class EntityMatchResult {
    EntityDetails matchedEntity;
    double similarityScore;
    Map<String, String> matchingFields;
  }
}
