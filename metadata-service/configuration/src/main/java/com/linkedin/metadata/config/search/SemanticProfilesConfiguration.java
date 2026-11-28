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

  /** Name of the built-in binding on document text, which is always present. */
  public static final String DOCUMENT_TEXT_BINDING = "documentText";

  /** While true, service-owned profiles can only be written by apply. */
  private boolean generationEnabled = false;

  /** Binding used for a profile key that is not declared under {@code models}. */
  private String defaultBinding = DOCUMENT_TEXT_BINDING;

  /** Named bindings. */
  private Map<String, SemanticBindingConfig> bindings = defaultBindings();

  /**
   * Entity type to the one service-generated profile id. Every entry names that same id, a
   * configured service-owned model, or startup fails. Required while {@code generationEnabled} is
   * true, because clients learn which profile to skip from it. May be empty while generation is
   * off.
   */
  private Map<String, String> active = new LinkedHashMap<>();

  public static Map<String, SemanticBindingConfig> defaultBindings() {
    Map<String, SemanticBindingConfig> bindings = new LinkedHashMap<>();
    bindings.put(
        DOCUMENT_TEXT_BINDING, SemanticBindingConfig.watching(defaultDocumentTextTriggers()));
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
