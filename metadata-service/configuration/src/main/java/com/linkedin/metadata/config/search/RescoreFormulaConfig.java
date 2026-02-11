package com.linkedin.metadata.config.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

/**
 * Configuration for Java/exp4j based rescoring (Stage 2 reranking).
 *
 * <p>This replaces the Elasticsearch Painless script-based rescoring with a Java implementation
 * using exp4j for formula evaluation, providing full explainability.
 *
 * <p>Example configuration in rescore_config.yaml:
 *
 * <pre>
 * rescore:
 *   enabled: true
 *   windowSize: 100
 *   formula: "pow(norm_bm25, 1.0) * pow(norm_views, 0.8) * pow(hasDesc, 1.3)"
 *   signals:
 *     - name: bm25
 *       normalizedName: norm_bm25
 *       fieldPath: _score
 *       type: SCORE
 *       normalization:
 *         type: SIGMOID
 *         cap: 500.0
 *         outputMin: 1.0
 *         outputMax: 2.0
 *       boost: 1.0
 * </pre>
 */
@Slf4j
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RescoreFormulaConfig {
  /** Enable/disable rescoring globally */
  @Builder.Default private boolean enabled = false;

  /** Path to rescore configuration YAML file */
  @Builder.Default private String file = "rescore_config.yaml";

  /** Number of top results to rescore (default: 100) */
  @Builder.Default private int windowSize = 100;

  /** Maximum number of results to fetch from ES for rescoring (default: 5000) */
  @Builder.Default private int maxRescoreWindow = 5000;

  /** exp4j formula string for computing final score */
  private String formula;

  /** List of signal definitions */
  @Builder.Default private List<SignalConfig> signals = new ArrayList<>();

  /** Whether to include explanation in search results */
  @Builder.Default private boolean includeExplain = true;

  /**
   * Load rescore configuration from file.
   *
   * @param mapper YAML-enabled Jackson mapper
   * @return Loaded rescore configuration
   * @throws IOException if file cannot be read
   */
  public RescoreFormulaConfig resolve(ObjectMapper mapper) throws IOException {
    if (enabled && file != null) {
      log.info("Rescore formula configuration enabled.");
      try (InputStream stream = new ClassPathResource(file).getInputStream()) {
        log.info("Rescore formula configuration found in classpath: {}", file);
        RescoreFormulaYaml yamlConfig = mapper.readValue(stream, RescoreFormulaYaml.class);
        return applyYamlConfig(yamlConfig);
      } catch (FileNotFoundException e) {
        log.info("Rescore formula configuration was NOT found in the classpath.");
        try (InputStream stream = new FileSystemResource(file).getInputStream()) {
          log.info("Rescore formula configuration found in filesystem: {}", file);
          RescoreFormulaYaml yamlConfig = mapper.readValue(stream, RescoreFormulaYaml.class);
          return applyYamlConfig(yamlConfig);
        } catch (Exception e2) {
          log.warn(
              "Rescore enabled, however there was an error loading configuration: " + file, e2);
          return this;
        }
      }
    }
    return this;
  }

  private RescoreFormulaConfig applyYamlConfig(RescoreFormulaYaml yaml) {
    if (yaml != null && yaml.getRescore() != null) {
      RescoreFormulaYaml.RescoreDetails details = yaml.getRescore();
      this.enabled = details.isEnabled();
      this.windowSize = details.getWindowSize();
      this.maxRescoreWindow = details.getMaxRescoreWindow();
      this.formula = details.getFormula();
      this.includeExplain = details.isIncludeExplain();

      // Convert signal configs
      this.signals = new ArrayList<>();
      if (details.getSignals() != null) {
        for (Map<String, Object> signalMap : details.getSignals()) {
          signals.add(SignalConfig.fromMap(signalMap));
        }
      }

      log.info(
          "Rescore formula configuration loaded: enabled={}, windowSize={}, signals={}, formula={}",
          enabled,
          windowSize,
          signals.size(),
          formula != null ? formula.substring(0, Math.min(50, formula.length())) + "..." : null);
    }
    return this;
  }

  /** Signal configuration for YAML parsing */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SignalConfig {
    private String name;
    private String normalizedName;
    private String fieldPath;
    @Builder.Default private String type = "NUMERIC";
    private NormalizationYaml normalization;
    @Builder.Default private double boost = 1.0;

    @SuppressWarnings("unchecked")
    public static SignalConfig fromMap(Map<String, Object> map) {
      SignalConfig config = new SignalConfig();
      config.name = (String) map.get("name");
      config.normalizedName = (String) map.get("normalizedName");
      config.fieldPath = (String) map.get("fieldPath");

      // Validate required fields
      if (config.name == null || config.name.isEmpty()) {
        throw new IllegalArgumentException("Signal 'name' is required");
      }
      if (config.normalizedName == null || config.normalizedName.isEmpty()) {
        throw new IllegalArgumentException(
            "Signal 'normalizedName' is required for: " + config.name);
      }
      if (config.fieldPath == null || config.fieldPath.isEmpty()) {
        throw new IllegalArgumentException("Signal 'fieldPath' is required for: " + config.name);
      }

      config.type = map.getOrDefault("type", "NUMERIC").toString();
      config.boost = map.containsKey("boost") ? ((Number) map.get("boost")).doubleValue() : 1.0;

      if (map.containsKey("normalization")) {
        Map<String, Object> normMap = (Map<String, Object>) map.get("normalization");
        config.normalization = NormalizationYaml.fromMap(normMap);
      }

      return config;
    }
  }

  /** Normalization configuration for YAML parsing */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class NormalizationYaml {
    @Builder.Default private String type = "NONE";

    /**
     * @deprecated Use inputMax instead
     */
    @Builder.Default private double cap = 1000.0;

    /** Minimum input value for sigmoid (values below map to outputMin) */
    @Builder.Default private double inputMin = 0.0;

    /** Maximum input value for sigmoid (values above map to outputMax) */
    @Builder.Default private double inputMax = 1000.0;

    /** Steepness of sigmoid curve (default 6.0 uses 90% of S-curve range) */
    @Builder.Default private double steepness = 6.0;

    @Builder.Default private double scale = 180.0;
    @Builder.Default private double outputMin = 1.0;
    @Builder.Default private double outputMax = 2.0;
    @Builder.Default private double trueValue = 1.0;
    @Builder.Default private double falseValue = 1.0;

    public static NormalizationYaml fromMap(Map<String, Object> map) {
      NormalizationYaml config = new NormalizationYaml();
      config.type = map.getOrDefault("type", "NONE").toString();

      // Handle inputMin/inputMax with backward compatibility for 'cap'
      if (map.containsKey("inputMin"))
        config.inputMin = ((Number) map.get("inputMin")).doubleValue();
      if (map.containsKey("inputMax")) {
        config.inputMax = ((Number) map.get("inputMax")).doubleValue();
      } else if (map.containsKey("cap")) {
        // Backward compatibility: use 'cap' as inputMax if inputMax not specified
        config.inputMax = ((Number) map.get("cap")).doubleValue();
      }
      if (map.containsKey("steepness"))
        config.steepness = ((Number) map.get("steepness")).doubleValue();

      if (map.containsKey("scale")) config.scale = ((Number) map.get("scale")).doubleValue();
      if (map.containsKey("outputMin"))
        config.outputMin = ((Number) map.get("outputMin")).doubleValue();
      if (map.containsKey("outputMax"))
        config.outputMax = ((Number) map.get("outputMax")).doubleValue();
      if (map.containsKey("trueValue"))
        config.trueValue = ((Number) map.get("trueValue")).doubleValue();
      if (map.containsKey("falseValue"))
        config.falseValue = ((Number) map.get("falseValue")).doubleValue();
      return config;
    }
  }

  /** Helper class for parsing rescore_config.yaml structure */
  @Data
  public static class RescoreFormulaYaml {
    private RescoreDetails rescore;

    @Data
    public static class RescoreDetails {
      private boolean enabled = true;
      private int windowSize = 100;
      private int maxRescoreWindow = 5000;
      private String formula;
      private boolean includeExplain = true;
      private List<Map<String, Object>> signals;
    }
  }
}
