package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EbeanConfiguration {
  private String username;
  private String password;
  private String url;
  private String driver;
  private long minConnections;
  private long maxConnections;
  private long maxInactiveTimeSeconds;
  private long maxAgeMinutes;
  private long leakTimeMinutes;
  private long waitTimeoutMillis;
  private boolean autoCreateDdl;
  private boolean postgresUseIamAuth;
  private LockingConfiguration locking;
  private String batchGetMethod;

  public static final EbeanConfiguration testDefault =
      EbeanConfiguration.builder().locking(LockingConfiguration.testDefault).build();

  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  public static class LockingConfiguration {
    private boolean enabled;
    private long durationSeconds;
    private long maximumLocks;

    public static final LockingConfiguration testDefault =
        LockingConfiguration.builder()
            .enabled(true)
            .durationSeconds(60)
            .maximumLocks(10000)
            .build();
  }
}
