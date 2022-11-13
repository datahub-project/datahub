package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Builder for generating settings for elasticsearch indices
 */
public class SettingsBuilder {
  public static final String KEYWORD_LOWERCASE_ANALYZER = "custom_keyword";
  public static final String TEXT_ANALYZER = "word_delimited";
  public static final String TEXT_SEARCH_ANALYZER = "query_word_delimited";
  private final Map<String, Object> settings;

  public SettingsBuilder(String mainTokenizer) {
    try {
      settings = buildSettings(mainTokenizer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, Object> getSettings() {
    return settings;
  }

  private static Map<String, Object> buildSettings(String mainTokenizer) throws IOException {
    ImmutableMap.Builder<String, Object> settings = ImmutableMap.builder();
    settings.put("max_ngram_diff", 17);
    settings.put("analysis", ImmutableMap.<String, Object>builder()
            .put("filter", buildFilters())
            .put("tokenizer", buildTokenizers())
            .put("normalizer", buildNormalizers())
            .put("analyzer", buildAnalyzers(mainTokenizer))
            .build());
    return settings.build();
  }

  private static Map<String, Object> buildFilters() throws IOException {
    PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();

    ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();

    // Filter to split string into words
    filters.put("custom_delimiter", ImmutableMap.<String, Object>builder()
            .put("type", "word_delimiter")
            .put("split_on_numerics", false)
            .put("preserve_original", true)
            .put("type_table", ImmutableList.of(
                    ": => SUBWORD_DELIM"
            ))
            .build());

    filters.put("custom_delimiter_graph", ImmutableMap.<String, Object>builder()
            .put("type", "word_delimiter_graph")
            .put("split_on_numerics", false)
            .put("preserve_original", true)
            .put("type_table", ImmutableList.of(
                    ": => SUBWORD_DELIM"
            ))
            .build());

    filters.put("urn_stop", ImmutableMap.<String, Object>builder()
            .put("type", "stop")
            .put("ignore_case", "true")
            .put("stopwords", ImmutableList.of("urn", "li"))
            .build());

    filters.put("trim_colon", ImmutableMap.<String, Object>builder()
            .put("type", "pattern_replace")
            .put("all", "false")
            .put("pattern", ":$")
            .put("replacement", "")
            .build());

    filters.put("min_length_2", ImmutableMap.<String, Object>builder()
            .put("type", "length")
            .put("min", "2")
            .build());

    Resource stemOverride = resourceResolver.getResource("classpath:elasticsearch/stem_override.txt");
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stemOverride.getInputStream()))) {
      filters.put("stem_override", ImmutableMap.<String, Object>builder()
              .put("type", "stemmer_override")
              .put("rules", reader.lines()
                      .map(String::trim)
                      .map(String::toLowerCase)
                      .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                      .collect(Collectors.toList()))
              .build());
    }

    filters.put("alpha_only", ImmutableMap.<String, Object>builder()
            .put("type", "pattern_capture")
            .put("patterns", ImmutableList.of(
                    "([a-z0-9]{2,})"
            ))
            .build());

    filters.put("shingle_2_3", ImmutableMap.<String, Object>builder()
            .put("type", "shingle")
            .put("min_shingle_size", "2")
            .put("max_shingle_size", "3")
            .build());

    filters.put("multifilter", ImmutableMap.<String, Object>builder()
            .put("type", "multiplexer")
            .put("filters", ImmutableList.of(
                    "custom_delimiter_graph,trim_colon,urn_stop,flatten_graph",
                    "default_syn_graph,flatten_graph",
                    "alpha_only,default_syn_graph,flatten_graph"
            ))
            .build());

    filters.put("multifilter_graph", ImmutableMap.<String, Object>builder()
            .put("type", "multiplexer")
            .put("filters", ImmutableList.of(
                    "custom_delimiter_graph,trim_colon,urn_stop",
                    "default_syn_graph",
                    "alpha_only,default_syn_graph"
            ))
            .build());

    Resource[] synonyms = resourceResolver.getResources("classpath:elasticsearch/synonyms/*.txt");
    for (Resource syn: synonyms) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(syn.getInputStream()))) {
        filters.put(String.format("%s_syn_graph", FilenameUtils.getBaseName(syn.getFilename())), ImmutableMap.<String, Object>builder()
                .put("type", "synonym_graph")
                .put("lenient", "true")
                .put("synonyms", reader.lines()
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .collect(Collectors.toList()))
                .build());
      }
    }

    return filters.build();
  }

  private static Map<String, Object> buildTokenizers() {
    ImmutableMap.Builder<String, Object> tokenizers = ImmutableMap.builder();
    // Tokenize by slashes
    tokenizers.put("slash_tokenizer",
        ImmutableMap.<String, Object>builder()
                .put("type", "pattern")
                .put("pattern", "[/]")
                .build());

    // Tokenize by whitespace and most special chars
    tokenizers.put("main_tokenizer",
            ImmutableMap.<String, Object>builder()
                    .put("type", "pattern")
                    .put("pattern", "[\\s(),./]")
                    .build());

    return tokenizers.build();
  }

  // Normalizers return a single token for a given string. Suitable for keywords
  private static Map<String, Object> buildNormalizers() {
    ImmutableMap.Builder<String, Object> normalizers = ImmutableMap.builder();
    // Analyzer for partial matching (i.e. autocomplete) - Prefix matching of each token
    normalizers.put("keyword_normalizer",
        ImmutableMap.<String, Object>builder().put("filter", ImmutableList.of("lowercase", "asciifolding")).build());

    return normalizers.build();
  }

  // Analyzers turn fields into multiple tokens
  private static Map<String, Object> buildAnalyzers(String mainTokenizer) {
    ImmutableMap.Builder<String, Object> analyzers = ImmutableMap.builder();
    // For special analysis, the substitution can be read from the configuration (chinese tokenizer: ik_smart / smartCN)
    // Analyzer for partial matching (i.e. autocomplete) - Prefix matching of each token
    analyzers.put("partial", ImmutableMap.<String, Object>builder()
            .put("tokenizer", StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : "main_tokenizer")
            .put("filter", ImmutableList.of(
                    "asciifolding",
                    "lowercase",
                    "custom_delimiter",
                    "trim_colon",
                    "urn_stop")
            ).build());

    // Analyzer for text tokenized into words (split by spaces, periods, and slashes)
    analyzers.put(TEXT_ANALYZER, ImmutableMap.<String, Object>builder()
            .put("tokenizer", StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : "main_tokenizer")
            .put("filter", ImmutableList.of(
                    "asciifolding",
                    "lowercase",
                    "multifilter",
                    "stop",
                    "unique",
                    "stem_override",
                    "snowball",
                    "min_length_2")
            ).build());

    analyzers.put(TEXT_SEARCH_ANALYZER, ImmutableMap.<String, Object>builder()
            .put("tokenizer", StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : "keyword")
            .put("filter", ImmutableList.of(
                    "asciifolding",
                    "lowercase",
                    "multifilter_graph",
                    "stop",
                    "unique",
                    "stem_override",
                    "snowball",
                    "min_length_2"
                    )
            ).build());

    // Analyzer for splitting by slashes (used to get depth of browsePath)
    analyzers.put("slash_pattern", ImmutableMap.<String, Object>builder()
            .put("tokenizer", "slash_tokenizer")
            .put("filter", ImmutableList.of("lowercase"))
            .build());

    // Analyzer for matching browse path
    analyzers.put("browse_path_hierarchy", ImmutableMap.<String, Object>builder()
            .put("tokenizer", "path_hierarchy")
            .build());

    // Analyzer for case-insensitive exact matching - Only used when building queries
    analyzers.put(KEYWORD_LOWERCASE_ANALYZER, ImmutableMap.<String, Object>builder()
            .put("tokenizer", "keyword")
            .put("filter", ImmutableList.of("trim", "lowercase", "asciifolding"))
            .build());

    // Analyzer for getting urn components
    analyzers.put("urn_component", ImmutableMap.<String, Object>builder()
            .put("tokenizer", "main_tokenizer")
            .put("filter", ImmutableList.of(
                    "asciifolding",
                    "lowercase",
                    "custom_delimiter",
                    "trim_colon",
                    "urn_stop",
                    "stop",
                    "stem_override",
                    "snowball",
                    "min_length_2"))
            .build());

    // Analyzer for partial matching urn components
    analyzers.put("partial_urn_component", ImmutableMap.<String, Object>builder()
            .put("tokenizer", "main_tokenizer")
            .put("filter", ImmutableList.of(
                    "asciifolding",
                    "lowercase",
                    "custom_delimiter",
                    "trim_colon",
                    "urn_stop"))
            .build());

    return analyzers.build();
  }
}
