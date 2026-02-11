package com.linkedin.datahub.graphql.resolvers.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.generated.ConfigPreset;
import com.linkedin.datahub.graphql.generated.NormalizationConfigType;
import com.linkedin.datahub.graphql.generated.RescoreMode;
import com.linkedin.datahub.graphql.generated.SearchConfiguration;
import com.linkedin.datahub.graphql.generated.SignalConfig;
import com.linkedin.datahub.graphql.generated.Stage1Configuration;
import com.linkedin.datahub.graphql.generated.Stage2Configuration;
import com.linkedin.metadata.config.search.RescoreFormulaConfig;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for exposing current search configuration (Stage 1 and Stage 2) to the UI. This allows
 * the search debug page to show server defaults and populate configuration editors.
 */
@Slf4j
public class SearchConfigurationResolver
    implements DataFetcher<CompletableFuture<SearchConfiguration>> {

  private final com.linkedin.metadata.config.search.SearchConfiguration searchConfig;
  private final CustomSearchConfiguration customSearchConfig;
  private final ObjectMapper objectMapper;

  public SearchConfigurationResolver(
      @Nullable com.linkedin.metadata.config.search.SearchConfiguration searchConfig,
      @Nullable CustomSearchConfiguration customSearchConfig) {
    this.searchConfig = searchConfig;
    this.customSearchConfig = customSearchConfig;
    this.objectMapper = ObjectMapperContext.DEFAULT.getObjectMapper();
  }

  @Override
  public CompletableFuture<SearchConfiguration> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          SearchConfiguration result = new SearchConfiguration();
          result.setStage1(buildStage1Config());
          result.setStage2(buildStage2Config());
          return result;
        });
  }

  private Stage1Configuration buildStage1Config() {
    Stage1Configuration stage1 = new Stage1Configuration();

    // Determine source of Stage 1 config
    if (customSearchConfig != null && customSearchConfig.getQueryConfigurations() != null) {
      stage1.setSource("yaml");
      try {
        // Serialize the custom search config to JSON for display
        String configJson = objectMapper.writeValueAsString(customSearchConfig);
        stage1.setFunctionScore(configJson);
      } catch (JsonProcessingException e) {
        log.warn("Failed to serialize custom search config", e);
        stage1.setFunctionScore(null);
      }
    } else {
      stage1.setSource("pdl");
      stage1.setFunctionScore(null);
    }

    // Build presets
    stage1.setPresets(buildStage1Presets());

    return stage1;
  }

  private Stage2Configuration buildStage2Config() {
    Stage2Configuration stage2 = new Stage2Configuration();

    // Check if Java/exp4j rescoring is enabled
    RescoreFormulaConfig rescoreFormula =
        searchConfig != null ? searchConfig.getRescoreFormula() : null;

    if (rescoreFormula != null && rescoreFormula.isEnabled()) {
      stage2.setEnabled(true);
      stage2.setMode(RescoreMode.JAVA_EXP4J);
      stage2.setWindowSize(rescoreFormula.getWindowSize());
      stage2.setFormula(rescoreFormula.getFormula());
      stage2.setSignals(buildSignalConfigs(rescoreFormula));
    } else if (searchConfig != null
        && searchConfig.getRescore() != null
        && searchConfig.getRescore().isEnabled()) {
      // Fall back to ES Painless rescore
      stage2.setEnabled(true);
      stage2.setMode(RescoreMode.ES_PAINLESS);
      stage2.setWindowSize(searchConfig.getRescore().getWindowSize());
      stage2.setFormula(null);
      stage2.setSignals(new ArrayList<>());
    } else {
      stage2.setEnabled(false);
      stage2.setMode(RescoreMode.JAVA_EXP4J);
      stage2.setWindowSize(100);
      stage2.setFormula(null);
      stage2.setSignals(new ArrayList<>());
    }

    // Build presets
    stage2.setPresets(buildStage2Presets());

    return stage2;
  }

  private List<SignalConfig> buildSignalConfigs(RescoreFormulaConfig rescoreFormula) {
    List<SignalConfig> signals = new ArrayList<>();

    if (rescoreFormula.getSignals() == null) {
      return signals;
    }

    for (RescoreFormulaConfig.SignalConfig signal : rescoreFormula.getSignals()) {
      SignalConfig signalConfig = new SignalConfig();
      signalConfig.setName(signal.getName());
      signalConfig.setNormalizedName(signal.getNormalizedName());
      signalConfig.setFieldPath(signal.getFieldPath());
      signalConfig.setType(signal.getType());
      signalConfig.setBoost(signal.getBoost());

      // Build normalization config
      if (signal.getNormalization() != null) {
        NormalizationConfigType normConfig = new NormalizationConfigType();
        RescoreFormulaConfig.NormalizationYaml normYaml = signal.getNormalization();

        normConfig.setType(normYaml.getType());
        normConfig.setInputMin((float) normYaml.getInputMin());
        normConfig.setInputMax((float) normYaml.getInputMax());
        normConfig.setSteepness((float) normYaml.getSteepness());
        normConfig.setScale((float) normYaml.getScale());
        normConfig.setOutputMin((float) normYaml.getOutputMin());
        normConfig.setOutputMax((float) normYaml.getOutputMax());
        normConfig.setTrueValue((float) normYaml.getTrueValue());
        normConfig.setFalseValue((float) normYaml.getFalseValue());

        signalConfig.setNormalization(normConfig);
      }

      signals.add(signalConfig);
    }

    return signals;
  }

  private List<ConfigPreset> buildStage1Presets() {
    List<ConfigPreset> presets = new ArrayList<>();

    presets.add(
        preset(
            "Server Default",
            "Use server configuration (from search_config.yaml or PDL)",
            Map.of()));

    presets.add(
        preset(
            "Quality Signals Only",
            "hasDescription +3, hasOwners +2, deprecated -10",
            Map.of(
                "functions",
                List.of(
                    functionScore("term", "hasDescription", true, 3.0),
                    functionScore("term", "hasOwners", true, 2.0),
                    functionScore("term", "deprecated", true, -10.0)),
                "score_mode",
                "sum",
                "boost_mode",
                "sum")));

    presets.add(
        preset(
            "Quality + Popularity",
            "Quality signals plus viewCount and usageCount with log saturation",
            Map.of(
                "functions",
                List.of(
                    functionScore("term", "hasDescription", true, 3.0),
                    functionScore("term", "hasOwners", true, 2.0),
                    functionScore("term", "deprecated", true, -10.0),
                    scriptScore("viewCount", "Math.log10(doc['viewCount'].value + 1)"),
                    scriptScore("usageCount", "Math.log10(doc['usageCount'].value + 1)")),
                "score_mode",
                "sum",
                "boost_mode",
                "sum")));

    presets.add(
        preset(
            "Pure BM25",
            "No function scores, pure BM25 text relevance",
            Map.of("functions", List.of(), "score_mode", "sum", "boost_mode", "replace")));

    return presets;
  }

  /**
   * Build Stage 2 (rescore) presets for the debug UI. These are sample configurations for
   * experimentation — production config comes from rescore_config.yaml.
   */
  private List<ConfigPreset> buildStage2Presets() {
    List<ConfigPreset> presets = new ArrayList<>();

    presets.add(
        preset("Server Default", "Use server configuration (from rescore_config.yaml)", Map.of()));
    presets.add(preset("Disabled", "Disable Stage 2 rescoring entirely", Map.of("enabled", false)));

    presets.add(
        preset(
            "BM25 + Quality",
            "Emphasize BM25 and quality signals (description, owners)",
            Map.of(
                "formula",
                "pow(norm_bm25, 1.0) * pow(hasDesc, 1.5) * pow(hasOwners, 1.3)",
                "signals",
                List.of(
                    signal("bm25", "norm_bm25", "_score", "SCORE", 1.0, sigmoidNorm(500, 1.0, 2.0)),
                    signal(
                        "hasDescription",
                        "hasDesc",
                        "hasDescription",
                        "BOOLEAN",
                        1.5,
                        booleanNorm(1.5, 1.0)),
                    signal(
                        "hasOwners",
                        "hasOwners",
                        "hasOwners",
                        "BOOLEAN",
                        1.3,
                        booleanNorm(1.3, 1.0))))));

    presets.add(
        preset(
            "Full Quality + Usage",
            "All quality signals plus usage metrics",
            Map.of(
                "formula",
                "pow(norm_bm25, 1.0) * pow(norm_views, 0.8) * pow(hasDesc, 1.3) * pow(hasOwners, 1.2) * pow(notDeprecated, 1.0)",
                "signals",
                List.of(
                    signal("bm25", "norm_bm25", "_score", "SCORE", 1.0, sigmoidNorm(500, 1.0, 2.0)),
                    signal(
                        "viewCount",
                        "norm_views",
                        "viewCount",
                        "NUMERIC",
                        0.8,
                        sigmoidNorm(1000, 1.0, 2.0)),
                    signal(
                        "hasDescription",
                        "hasDesc",
                        "hasDescription",
                        "BOOLEAN",
                        1.3,
                        booleanNorm(1.3, 1.0)),
                    signal(
                        "hasOwners",
                        "hasOwners",
                        "hasOwners",
                        "BOOLEAN",
                        1.2,
                        booleanNorm(1.2, 1.0)),
                    signal(
                        "deprecated",
                        "notDeprecated",
                        "deprecated",
                        "BOOLEAN",
                        1.0,
                        booleanNorm(0.7, 1.0))))));

    return presets;
  }

  // --- Preset builder helpers ---

  private ConfigPreset preset(String name, String description, Map<String, Object> config) {
    ConfigPreset p = new ConfigPreset();
    p.setName(name);
    p.setDescription(description);
    try {
      p.setConfig(objectMapper.writeValueAsString(config));
    } catch (JsonProcessingException e) {
      log.warn("Failed to serialize preset config: {}", name, e);
      p.setConfig("{}");
    }
    return p;
  }

  private static Map<String, Object> functionScore(
      String filterType, String field, Object value, double weight) {
    Map<String, Object> fn = new LinkedHashMap<>();
    fn.put("filter", Map.of(filterType, Map.of(field, value)));
    fn.put("weight", weight);
    return fn;
  }

  private static Map<String, Object> scriptScore(String field, String script) {
    Map<String, Object> fn = new LinkedHashMap<>();
    fn.put("filter", Map.of("exists", Map.of("field", field)));
    fn.put("script_score", Map.of("script", script));
    return fn;
  }

  private static Map<String, Object> signal(
      String name,
      String normalizedName,
      String fieldPath,
      String type,
      double boost,
      Map<String, Object> normalization) {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("name", name);
    s.put("normalizedName", normalizedName);
    s.put("fieldPath", fieldPath);
    s.put("type", type);
    s.put("boost", boost);
    s.put("normalization", normalization);
    return s;
  }

  private static Map<String, Object> sigmoidNorm(
      double inputMax, double outputMin, double outputMax) {
    Map<String, Object> n = new LinkedHashMap<>();
    n.put("type", "SIGMOID");
    n.put("inputMax", inputMax);
    n.put("outputMin", outputMin);
    n.put("outputMax", outputMax);
    return n;
  }

  private static Map<String, Object> booleanNorm(double trueValue, double falseValue) {
    Map<String, Object> n = new LinkedHashMap<>();
    n.put("type", "BOOLEAN");
    n.put("trueValue", trueValue);
    n.put("falseValue", falseValue);
    return n;
  }
}
