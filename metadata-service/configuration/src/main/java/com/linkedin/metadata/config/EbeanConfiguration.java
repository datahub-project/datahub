package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EbeanConfiguration {
  public static final int DEFAULT_QUERY_KEYS_COUNT = 375;

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
  private String batchGetMethod;
  private Integer queryKeysCountForBatch = DEFAULT_QUERY_KEYS_COUNT;

  public static final EbeanConfiguration testDefault = EbeanConfiguration.builder().build();
}
