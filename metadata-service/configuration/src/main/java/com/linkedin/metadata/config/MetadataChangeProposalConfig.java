/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
    SideEffectConfig schemaField;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class SideEffectConfig {
    boolean enabled;
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
