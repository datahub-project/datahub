package com.linkedin.metadata.config.search;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Which aspects and fields make each embedding profile stale, and which further fields it reads. A
 * profile key selects a named binding. Bindings are reused across profile keys.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SemanticProfilesConfiguration {

  /** While true, service-owned profiles can only be written by apply. */
  private boolean generationEnabled = false;

  /** Binding used for a profile key that is not declared under {@code models}. */
  private String defaultBinding = "documentText";

  /** Named bindings. */
  private Map<String, SemanticBindingConfig> bindings = defaultBindings();

  /**
   * Entity type to the one service-generated profile id. Every entry names that same id, a
   * configured service-owned model, or startup fails. Empty leaves generation unconfigured.
   */
  private Map<String, String> active = new LinkedHashMap<>();

  public static Map<String, SemanticBindingConfig> defaultBindings() {
    Map<String, SemanticBindingConfig> bindings = new LinkedHashMap<>();
    bindings.put("documentText", SemanticBindingConfig.watching(defaultDocumentTextTriggers()));
    bindings.put("external", SemanticBindingConfig.externalBinding());
    return bindings;
  }

  public static Map<String, List<String>> defaultDocumentTextTriggers() {
    Map<String, List<String>> documentText = new LinkedHashMap<>();
    documentText.put("documentInfo", List.of("contents.text"));
    documentText.put("semanticText", List.of("text"));
    return documentText;
  }
}
