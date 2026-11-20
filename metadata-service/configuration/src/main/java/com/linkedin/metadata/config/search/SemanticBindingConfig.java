package com.linkedin.metadata.config.search;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One named content binding. Each map is aspect name to dotted field paths; an empty path list
 * means the whole aspect.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SemanticBindingConfig {

  /** Fields whose change marks the profile not current. */
  private Map<String, List<String>> triggers = new LinkedHashMap<>();

  /**
   * Fields the embedding reads that never mark the profile not current. Read by embedders through
   * the registry; the server does not watch them.
   */
  private Map<String, List<String>> includes = new LinkedHashMap<>();

  /**
   * Marks the binding as owned by an external provider: nothing dirties it, and it declares no
   * triggers or includes. Required for a binding with no triggers, so a binding written in an older
   * flat shape fails startup instead of silently becoming external.
   */
  private boolean external = false;

  public static SemanticBindingConfig watching(Map<String, List<String>> triggers) {
    return new SemanticBindingConfig(new LinkedHashMap<>(triggers), new LinkedHashMap<>(), false);
  }

  public static SemanticBindingConfig externalBinding() {
    return new SemanticBindingConfig(new LinkedHashMap<>(), new LinkedHashMap<>(), true);
  }
}
