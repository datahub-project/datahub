package com.linkedin.metadata.config.search;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Which aspects and fields make each embedding profile stale. A profile key selects a named
 * binding. Bindings are reused across profile keys.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SemanticProfilesConfiguration {

  /** While true, service-owned profiles can only be written by apply. */
  private boolean generationEnabled = false;

  /** Binding used for a profile key that is not declared under {@code models}. */
  private String defaultBinding = "documentText";

  /**
   * Named bindings. The map value is aspect name to dotted field paths. An empty path list means
   * any change to that aspect. An empty aspect map is the external binding: nothing dirties it.
   */
  private Map<String, Map<String, List<String>>> bindings = defaultBindings();

  public static Map<String, Map<String, List<String>>> defaultBindings() {
    Map<String, Map<String, List<String>>> bindings = new LinkedHashMap<>();
    Map<String, List<String>> documentText = new LinkedHashMap<>();
    documentText.put("documentInfo", List.of("contents.text"));
    documentText.put("semanticText", List.of("text"));
    bindings.put("documentText", documentText);
    bindings.put("external", Map.of());
    return bindings;
  }
}
