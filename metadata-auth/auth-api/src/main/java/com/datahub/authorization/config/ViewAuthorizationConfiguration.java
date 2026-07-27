package com.datahub.authorization.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class ViewAuthorizationConfiguration {
  private boolean enabled;
  // Behavioral toggle only (batch-prefetch vs lazy ownership resolution); identical entity data and
  // authorization results either way. Excluded from equals/hashCode so it does NOT perturb the
  // OperationContext cache-key fingerprint (OperationContextConfig.getCacheKeyComponent) or
  // invalidate entity-client caches when toggled.
  @EqualsAndHashCode.Exclude private boolean ownershipPrefetchEnabled = false;
  private ViewAuthorizationRecommendationsConfig recommendations;

  @Builder(toBuilder = true)
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ViewAuthorizationRecommendationsConfig {
    private boolean peerGroupEnabled;
  }
}
