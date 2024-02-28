package com.datahub.authorization.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class SearchAuthorizationConfiguration {
  private boolean enabled;
  private SearchAuthorizationRecommendationsConfiguration recommendations;

  @Builder(toBuilder = true)
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class SearchAuthorizationRecommendationsConfiguration {
    private boolean peerGroupEnabled;
  }
}
