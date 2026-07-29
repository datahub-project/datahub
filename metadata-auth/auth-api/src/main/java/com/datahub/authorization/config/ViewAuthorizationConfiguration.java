package com.datahub.authorization.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(chain = true)
public class ViewAuthorizationConfiguration {
  private boolean enabled;
  private ViewAuthorizationRecommendationsConfig recommendations;

  /**
   * Raw operator overlays for entity types that bypass view authorization when enabled. Lean
   * baseline is {@code viewUnrestricted} on the entity registry; overlays live in {@code
   * application.yaml}.
   */
  private ViewUnrestrictedEntityTypes unrestrictedEntityTypes;

  /**
   * Resolved effective set (registry baseline + config overlays, registry-validated). Populated
   * once at OperationContext construction; null means callers treat unrestricted as empty (all
   * types restricted when view auth is enabled).
   */
  @JsonIgnore private Set<String> effectiveUnrestrictedEntityTypes;

  @Builder(toBuilder = true)
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ViewAuthorizationRecommendationsConfig {
    private boolean peerGroupEnabled;
  }
}
