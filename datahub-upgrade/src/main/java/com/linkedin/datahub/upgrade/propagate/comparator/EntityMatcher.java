package com.linkedin.datahub.upgrade.propagate.comparator;

import com.linkedin.datahub.upgrade.propagate.EntityDetails;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;


public interface EntityMatcher {

  /**
   * Find the entity among others that best match the original entity.
   * If there are no entities that pass the similarity score threshold, do not return result
   *
   * @param original original entity we are trying to match
   * @param others entities that we are looking for matches from
   * @param threshold similarity score threshold
   * @return matched result. Return null if none pass the similarity score threshold
   */
  @Nullable
  EntityMatchResult match(EntityDetails original, Collection<EntityDetails> others, double threshold);

  @Value
  @Builder
  class EntityMatchResult {
    EntityDetails matchedEntity;
    double similarityScore;
    Map<String, String> matchingFields;
  }
}
