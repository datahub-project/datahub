package com.linkedin.metadata.config.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

/**
 * Configuration for Elasticsearch rescore (Stage 2 reranking).
 *
 * <p>Rescore applies a secondary query to the top N results from Stage 1, allowing expensive
 * scoring on a small candidate set.
 *
 * <p>Example configuration in rescore_config.yaml:
 *
 * <pre>
 * rescore:
 *   enabled: true
 *   windowSize: 100
 *   queryWeight: 0.0       # Stage 2 replaces Stage 1
 *   rescoreQueryWeight: 1.0
 *   function_score:
 *     functions: [...]
 *     score_mode: first
 *     boost_mode: replace
 * </pre>
 */
@Slf4j
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RescoreConfiguration {
  /** Enable/disable rescore globally */
  private boolean enabled = false;

  /** Path to rescore configuration YAML file */
  private String file = "rescore_config.yaml";

  /** Number of top results to rescore (default: 100) */
  private int windowSize = 100;

  /** Weight for Stage 1 score (default: 0.0 = Stage 2 replaces Stage 1) */
  private float queryWeight = 0.0f;

  /** Weight for Stage 2 score (default: 1.0 = 100% Stage 2) */
  private float rescoreQueryWeight = 1.0f;

  /**
   * Score combination mode: how to combine Stage 1 and Stage 2 scores. Options: total (add),
   * multiply, avg, max, min Default: total (backwards compatible)
   */
  private String scoreMode = "total";

  /**
   * Function score configuration for Stage 2. Same structure as Stage 1 function scores, typically
   * multiplicative. Loaded from rescore_config.yaml.
   */
  private Map<String, Object> functionScore;

  /** Maximum number of results to fetch from ES for rescoring (default: 500) */
  private int maxRescoreWindow = 500;

  /**
   * Whether to include explanation in search results (default: false, enable only for debugging)
   */
  private boolean includeExplain = false;

  /** exp4j formula string for computing final score (used by RescoreFormulaConfig) */
  private String formula;

  /** List of signal definitions (used by RescoreFormulaConfig) */
  private java.util.List<Map<String, Object>> signals;

  /**
   * Load rescore configuration from file.
   *
   * @param mapper YAML-enabled Jackson mapper
   * @return Loaded rescore configuration
   * @throws IOException if file cannot be read
   */
  public RescoreConfiguration resolve(ObjectMapper mapper) throws IOException {
    if (enabled && file != null) {
      log.info("Rescore configuration enabled.");
      try (InputStream stream = new ClassPathResource(file).getInputStream()) {
        log.info("Rescore configuration found in classpath: {}", file);
        RescoreConfigYaml yamlConfig = mapper.readValue(stream, RescoreConfigYaml.class);
        return applyYamlConfig(yamlConfig);
      } catch (FileNotFoundException e) {
        log.info("Rescore configuration was NOT found in the classpath.");
        try (InputStream stream = new FileSystemResource(file).getInputStream()) {
          log.info("Rescore configuration found in filesystem: {}", file);
          RescoreConfigYaml yamlConfig = mapper.readValue(stream, RescoreConfigYaml.class);
          return applyYamlConfig(yamlConfig);
        } catch (Exception e2) {
          log.warn(
              "Rescore enabled, however there was an error loading configuration: " + file, e2);
          return this; // Return current config if loading fails
        }
      }
    }
    return this;
  }

  private RescoreConfiguration applyYamlConfig(RescoreConfigYaml yaml) {
    if (yaml != null && yaml.getRescore() != null) {
      RescoreConfigYaml.RescoreDetails details = yaml.getRescore();
      this.enabled = details.isEnabled();
      this.windowSize = details.getWindowSize();
      this.queryWeight = details.getQueryWeight();
      this.rescoreQueryWeight = details.getRescoreQueryWeight();
      this.scoreMode = details.getScoreMode();
      this.functionScore = details.getFunctionScore();
      this.maxRescoreWindow = details.getMaxRescoreWindow();
      this.includeExplain = details.isIncludeExplain();
      this.formula = details.getFormula();
      this.signals = details.getSignals();
      log.info(
          "Rescore configuration loaded: enabled={}, windowSize={}, queryWeight={}, rescoreQueryWeight={}, scoreMode={}, maxRescoreWindow={}, includeExplain={}",
          enabled,
          windowSize,
          queryWeight,
          rescoreQueryWeight,
          scoreMode,
          maxRescoreWindow,
          includeExplain);
    }
    return this;
  }

  /** Helper class for parsing rescore_config.yaml structure */
  @Data
  public static class RescoreConfigYaml {
    private RescoreDetails rescore;

    @Data
    public static class RescoreDetails {
      private boolean enabled = true;
      private int windowSize = 100;
      private float queryWeight = 0.0f;
      private float rescoreQueryWeight = 1.0f;
      private String scoreMode = "total";
      private Map<String, Object> functionScore;
      private int maxRescoreWindow = 500;
      private boolean includeExplain = false;
      private String formula;
      private java.util.List<Map<String, Object>> signals;
    }
  }
}
