package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class SecretServiceConfiguration {
  private boolean v1AlgorithmEnabled;

  /**
   * Controls the human-caller enforcement mode for SecretService.decrypt / encrypt.
   *
   * <ul>
   *   <li>ENFORCE (default) — throw SecurityException when a human browser/mobile agent calls
   *       decrypt.
   *   <li>AUDIT — log a warning but allow the call through; useful for staged rollout.
   *   <li>DISABLED — no enforcement; kill-switch for incident response.
   * </ul>
   */
  @Builder.Default private CallerGuardMode callerGuardMode = CallerGuardMode.ENFORCE;

  public enum CallerGuardMode {
    ENFORCE,
    AUDIT,
    DISABLED
  }
}
