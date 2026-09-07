package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class MetadataChangeProposalConfig {
  ConsumerBatchConfig consumer;
  ThrottlesConfig throttle;
  MCPValidationConfig validation;
  SideEffectsConfig sideEffects;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ThrottlesConfig {
    Integer updateIntervalMs;
    ComponentsThrottleConfig components;
    ThrottleConfig versioned;
    ThrottleConfig timeseries;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ComponentsThrottleConfig {
    MceConsumerThrottleConfig mceConsumer;
    ApiRequestsThrottleConfig apiRequests;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class MceConsumerThrottleConfig {
    boolean enabled;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ApiRequestsThrottleConfig {
    boolean enabled;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ThrottleConfig {
    boolean enabled;
    Integer threshold;
    Integer maxAttempts;
    Integer initialIntervalMs;
    Integer multiplier;
    Integer maxIntervalMs;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class SideEffectsConfig {
    SchemaFieldSideEffectsConfig schemaField;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class SideEffectConfig {
    boolean enabled;
  }

  /**
   * Schema field MCP side effects: master materialization (key/aliases/status) plus optional
   * domain/ownership mirroring from the parent dataset.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class SchemaFieldSideEffectsConfig {
    boolean enabled;
    SideEffectConfig domain;
    SideEffectConfig ownership;

    public boolean isDomainEnabled() {
      return enabled && domain != null && domain.isEnabled();
    }

    public boolean isOwnershipEnabled() {
      return enabled && ownership != null && ownership.isEnabled();
    }
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class BatchConfig {
    boolean enabled;
    Integer size;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ConsumerBatchConfig {
    BatchConfig batch;
  }
}
