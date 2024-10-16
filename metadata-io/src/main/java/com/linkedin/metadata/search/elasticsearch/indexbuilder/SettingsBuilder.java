package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/** Builder for generating settings for elasticsearch indices */
public class SettingsBuilder {

  // ElasticSearch Property Map Keys
  public static final String ALL = "all";
  public static final String ANALYSIS = "analysis";
  public static final String ANALYZER = "analyzer";
  public static final String FIELDDATA = "fielddata";
  public static final String FIELDS = "fields";
  public static final String FILTER = "filter";
  public static final String FILTERS = "filters";
  public static final String IGNORE_CASE = "ignore_case";
  public static final String KEYWORD = "keyword";
  public static final String LENIENT = "lenient";
  public static final String MAX_NGRAM_DIFF = "max_ngram_diff";
  public static final String MAX_SHINGLE_SIZE = "max_shingle_size";

  public static final String DOC_VALUES = "doc_values";
  public static final String NGRAM = "ngram";
  public static final String NORMALIZER = "normalizer";
  public static final String PATTERN = "pattern";
  public static final String PATTERNS = "patterns";
  public static final String REPLACEMENT = "replacement";
  public static final String PRESERVE_ORIGINAL = "preserve_original";
  public static final String SEARCH_ANALYZER = "search_analyzer";
  public static final String SEARCH_QUOTE_ANALYZER = "search_quote_analyzer";
  public static final String CUSTOM_QUOTE_ANALYZER = "quote_analyzer";
  public static final String SPLIT_ON_NUMERICS = "split_on_numerics";
  public static final String SPLIT_ON_CASE_CHANGE = "split_on_case_change";
  public static final String STOPWORDS = "stopwords";
  public static final String SYNONYMS = "synonyms";
  public static final String TOKENIZER = "tokenizer";
  public static final String TYPE = "type";
  public static final String TYPE_TABLE = "type_table";
  public static final String DELIMITER = "delimiter";
  public static final String UNIT_SEPARATOR_DELIMITER = "␟";

  // Analyzers
  public static final String BROWSE_PATH_HIERARCHY_ANALYZER = "browse_path_hierarchy";
  public static final String BROWSE_PATH_V2_HIERARCHY_ANALYZER = "browse_path_v2_hierarchy";
  public static final String KEYWORD_LOWERCASE_ANALYZER = "custom_keyword";
  public static final String PARTIAL_ANALYZER = "partial";
  public static final String SLASH_PATTERN_ANALYZER = "slash_pattern";
  public static final String UNIT_SEPARATOR_PATTERN_ANALYZER = "unit_separator_pattern";
  public static final String TEXT_ANALYZER = "word_delimited";
  public static final String TEXT_SEARCH_ANALYZER = "query_word_delimited";
  public static final String KEYWORD_ANALYZER = "keyword";
  public static final String URN_ANALYZER = "urn_component";
  public static final String URN_SEARCH_ANALYZER = "query_urn_component";
  public static final String WORD_GRAM_2_ANALYZER = "word_gram_2";
  public static final String WORD_GRAM_3_ANALYZER = "word_gram_3";
  public static final String WORD_GRAM_4_ANALYZER = "word_gram_4";

  // Filters
  public static final String ALPHANUM_SPACE_ONLY = "alpha_num_space";
  public static final String REMOVE_QUOTES = "remove_quotes";
  public static final String ASCII_FOLDING = "asciifolding";
  public static final String AUTOCOMPLETE_CUSTOM_DELIMITER = "autocomplete_custom_delimiter";
  public static final String STICKY_DELIMITER_GRAPH = "sticky_delimiter_graph";
  public static final String DEFAULT_SYN_GRAPH = "default_syn_graph";
  public static final String FLATTEN_GRAPH = "flatten_graph";
  public static final String LOWERCASE = "lowercase";
  public static final String MIN_LENGTH = "min_length";
  public static final String MULTIFILTER = "multifilter";
  public static final String MULTIFILTER_GRAPH = "multifilter_graph";
  public static final String PARTIAL_URN_COMPONENT = "partial_urn_component";
  public static final String SHINGLE = "shingle";
  public static final String WORD_GRAM_2_FILTER = "word_gram_2_filter";
  public static final String WORD_GRAM_3_FILTER = "word_gram_3_filter";
  public static final String WORD_GRAM_4_FILTER = "word_gram_4_filter";
  public static final String SNOWBALL = "snowball";
  public static final String STEM_OVERRIDE = "stem_override";
  public static final String STOP = "stop";
  public static final String UNIQUE = "unique";
  public static final String DATAHUB_STOP_WORDS = "datahub_stop_words";
  public static final String WORD_DELIMITER = "word_delimiter";
  public static final String WORD_DELIMITER_GRAPH = "word_delimiter_graph";

  public static final String TRIM = "trim";

  // MultiFilters
  public static final String MULTIFILTER_GRAPH_1 =
      String.join(",", LOWERCASE, STICKY_DELIMITER_GRAPH);
  public static final String MULTIFILTER_GRAPH_2 =
      String.join(",", LOWERCASE, ALPHANUM_SPACE_ONLY, DEFAULT_SYN_GRAPH);

  public static final String MULTIFILTER_1 = String.join(",", MULTIFILTER_GRAPH_1, FLATTEN_GRAPH);
  public static final String MULTIFILTER_2 = String.join(",", MULTIFILTER_GRAPH_2, FLATTEN_GRAPH);

  // Normalizers
  public static final String KEYWORD_NORMALIZER = "keyword_normalizer";

  // Tokenizers
  public static final String KEYWORD_TOKENIZER = "keyword";
  public static final String MAIN_TOKENIZER = "main_tokenizer";
  public static final String PATH_HIERARCHY_TOKENIZER = "path_hierarchy";
  public static final String SLASH_TOKENIZER = "slash_tokenizer";
  public static final String UNIT_SEPARATOR_PATH_TOKENIZER = "unit_separator_path_tokenizer";
  public static final String UNIT_SEPARATOR_TOKENIZER = "unit_separator_tokenizer";
  public static final String WORD_GRAM_TOKENIZER = "word_gram_tokenizer";
  // Do not remove the space, needed for multi-term synonyms
  public static final List<String> ALPHANUM_SPACE_PATTERNS =
      ImmutableList.of("([a-z0-9 _-]{2,})", "([a-z0-9 ]{2,})", "\\\"([^\\\"]*)\\\"");

  public static final List<String> DATAHUB_STOP_WORDS_LIST = ImmutableList.of("urn", "li");

  public static final List<String> WORD_DELIMITER_TYPE_TABLE =
      ImmutableList.of(": => SUBWORD_DELIM", "_ => ALPHANUM", "- => ALPHA");
  public static final List<String> INDEX_TOKEN_FILTERS =
      ImmutableList.of(
          ASCII_FOLDING,
          MULTIFILTER,
          TRIM,
          LOWERCASE,
          DATAHUB_STOP_WORDS,
          STOP,
          STEM_OVERRIDE,
          SNOWBALL,
          REMOVE_QUOTES,
          UNIQUE,
          MIN_LENGTH);

  public static final List<String> SEARCH_TOKEN_FILTERS =
      ImmutableList.of(
          ASCII_FOLDING,
          MULTIFILTER_GRAPH,
          TRIM,
          LOWERCASE,
          DATAHUB_STOP_WORDS,
          STOP,
          STEM_OVERRIDE,
          SNOWBALL,
          REMOVE_QUOTES,
          UNIQUE,
          MIN_LENGTH);

  public static final List<String> QUOTED_TOKEN_FILTERS =
      ImmutableList.of(
          ASCII_FOLDING, LOWERCASE, REMOVE_QUOTES, DATAHUB_STOP_WORDS, STOP, MIN_LENGTH);

  public static final List<String> PARTIAL_AUTOCOMPLETE_TOKEN_FILTERS =
      ImmutableList.of(ASCII_FOLDING, AUTOCOMPLETE_CUSTOM_DELIMITER, LOWERCASE);

  public static final List<String> WORD_GRAM_TOKEN_FILTERS =
      ImmutableList.of(ASCII_FOLDING, LOWERCASE, TRIM, REMOVE_QUOTES);

  public final Map<String, Object> settings;

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
    settings.put(MAX_NGRAM_DIFF, 17);
    settings.put(
        ANALYSIS,
        ImmutableMap.<String, Object>builder()
            .put(FILTER, buildFilters())
            .put(TOKENIZER, buildTokenizers())
            .put(NORMALIZER, buildNormalizers())
            .put(ANALYZER, buildAnalyzers(mainTokenizer))
            .build());
    return settings.build();
  }

  private static Map<String, Object> buildFilters() throws IOException {
    PathMatchingResourcePatternResolver resourceResolver =
        new PathMatchingResourcePatternResolver();

    ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();

    // Filter to split string into words
    filters.put(
        AUTOCOMPLETE_CUSTOM_DELIMITER,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, WORD_DELIMITER)
            .put(SPLIT_ON_NUMERICS, false)
            .put(SPLIT_ON_CASE_CHANGE, false)
            .put(PRESERVE_ORIGINAL, true)
            .put(TYPE_TABLE, WORD_DELIMITER_TYPE_TABLE)
            .build());

    filters.put(
        STICKY_DELIMITER_GRAPH,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, WORD_DELIMITER_GRAPH)
            .put(SPLIT_ON_NUMERICS, false)
            .put(SPLIT_ON_CASE_CHANGE, false)
            .put(PRESERVE_ORIGINAL, true)
            .put("generate_number_parts", false)
            .put(TYPE_TABLE, WORD_DELIMITER_TYPE_TABLE)
            .build());

    filters.put(
        DATAHUB_STOP_WORDS,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, STOP)
            .put(IGNORE_CASE, "true")
            .put(STOPWORDS, DATAHUB_STOP_WORDS_LIST)
            .build());

    filters.put(
        MIN_LENGTH,
        ImmutableMap.<String, Object>builder().put(TYPE, "length").put("min", "3").build());

    Resource stemOverride =
        resourceResolver.getResource("classpath:elasticsearch/stem_override.txt");
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stemOverride.getInputStream()))) {
      filters.put(
          STEM_OVERRIDE,
          ImmutableMap.<String, Object>builder()
              .put(TYPE, "stemmer_override")
              .put(
                  "rules",
                  reader
                      .lines()
                      .map(String::trim)
                      .map(String::toLowerCase)
                      .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                      .collect(Collectors.toList()))
              .build());
    }

    filters.put(
        ALPHANUM_SPACE_ONLY,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "pattern_capture")
            .put(PATTERNS, ALPHANUM_SPACE_PATTERNS)
            .build());

    filters.put(
        REMOVE_QUOTES,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "pattern_replace")
            .put(PATTERN, "['\"]")
            .put(REPLACEMENT, "")
            .build());

    // Index Time
    filters.put(
        MULTIFILTER,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "multiplexer")
            .put(FILTERS, ImmutableList.of(MULTIFILTER_1, MULTIFILTER_2))
            .build());

    // Search Time
    filters.put(
        MULTIFILTER_GRAPH,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "multiplexer")
            .put(FILTERS, ImmutableList.of(MULTIFILTER_GRAPH_1, MULTIFILTER_GRAPH_2))
            .build());

    Resource[] synonyms = resourceResolver.getResources("classpath:elasticsearch/synonyms/*.txt");
    for (Resource syn : synonyms) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(syn.getInputStream()))) {
        filters.put(
            String.format("%s_syn_graph", FilenameUtils.getBaseName(syn.getFilename())),
            ImmutableMap.<String, Object>builder()
                .put(TYPE, "synonym_graph")
                .put(LENIENT, "false")
                .put(
                    SYNONYMS,
                    reader
                        .lines()
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .collect(Collectors.toList()))
                .build());
      }

      for (Map.Entry<String, Integer> entry :
          Map.of(WORD_GRAM_2_FILTER, 2, WORD_GRAM_3_FILTER, 3, WORD_GRAM_4_FILTER, 4).entrySet()) {
        String filterName = entry.getKey();
        Integer gramSize = entry.getValue();
        filters.put(
            filterName,
            ImmutableMap.<String, Object>builder()
                .put(TYPE, SHINGLE)
                .put("min_shingle_size", gramSize)
                .put("max_shingle_size", gramSize)
                .put("output_unigrams", false)
                .build());
      }
    }

    return filters.build();
  }

  private static Map<String, Object> buildTokenizers() {
    ImmutableMap.Builder<String, Object> tokenizers = ImmutableMap.builder();
    // Tokenize by slashes
    tokenizers.put(
        SLASH_TOKENIZER,
        ImmutableMap.<String, Object>builder().put(TYPE, PATTERN).put(PATTERN, "[/]").build());

    tokenizers.put(
        UNIT_SEPARATOR_TOKENIZER,
        ImmutableMap.<String, Object>builder().put(TYPE, PATTERN).put(PATTERN, "[␟]").build());

    tokenizers.put(
        UNIT_SEPARATOR_PATH_TOKENIZER,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, PATH_HIERARCHY_TOKENIZER)
            .put(DELIMITER, "␟")
            .build());

    // Tokenize by most special chars
    // Do NOT tokenize by whitespace to keep multi-word synonyms in the same token
    // The split by whitespace is done later in the token filters phase
    tokenizers.put(
        MAIN_TOKENIZER,
        ImmutableMap.<String, Object>builder().put(TYPE, PATTERN).put(PATTERN, "[(),./:]").build());

    // Tokenize by whitespace and most special chars for wordgrams
    // only split on - when not preceded by a whitespace to preserve exclusion functionality
    // i.e. "logging-events-bkcp" and "logging-events -bckp" should be handled differently
    tokenizers.put(
        WORD_GRAM_TOKENIZER,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, PATTERN)
            .put(PATTERN, "[(),./:\\s_]|(?<=\\S)(-)")
            .build());

    return tokenizers.build();
  }

  // Normalizers return a single token for a given string. Suitable for keywords
  private static Map<String, Object> buildNormalizers() {
    ImmutableMap.Builder<String, Object> normalizers = ImmutableMap.builder();
    // Analyzer for partial matching (i.e. autocomplete) - Prefix matching of each token
    normalizers.put(
        KEYWORD_NORMALIZER,
        ImmutableMap.<String, Object>builder()
            .put(FILTER, ImmutableList.of(LOWERCASE, ASCII_FOLDING))
            .build());

    return normalizers.build();
  }

  // Analyzers turn fields into multiple tokens
  private static Map<String, Object> buildAnalyzers(String mainTokenizer) {
    ImmutableMap.Builder<String, Object> analyzers = ImmutableMap.builder();

    // Analyzer for splitting by slashes (used to get depth of browsePath)
    analyzers.put(
        SLASH_PATTERN_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, SLASH_TOKENIZER)
            .put(FILTER, ImmutableList.of(LOWERCASE))
            .build());

    // Analyzer for splitting by unit-separator (used to get depth of browsePathV2)
    analyzers.put(
        UNIT_SEPARATOR_PATTERN_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, UNIT_SEPARATOR_TOKENIZER)
            .put(FILTER, ImmutableList.of(LOWERCASE))
            .build());

    // Analyzer for matching browse path
    analyzers.put(
        BROWSE_PATH_HIERARCHY_ANALYZER,
        ImmutableMap.<String, Object>builder().put(TOKENIZER, PATH_HIERARCHY_TOKENIZER).build());

    // Analyzer for matching browse path v2
    analyzers.put(
        BROWSE_PATH_V2_HIERARCHY_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, UNIT_SEPARATOR_PATH_TOKENIZER)
            .build());

    // Analyzer for case-insensitive exact matching - Only used when building queries
    analyzers.put(
        KEYWORD_LOWERCASE_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, KEYWORD_TOKENIZER)
            .put(FILTER, ImmutableList.of("trim", LOWERCASE, ASCII_FOLDING, SNOWBALL))
            .build());

    // Analyzer for quotes words
    analyzers.put(
        CUSTOM_QUOTE_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, KEYWORD_TOKENIZER)
            .put(FILTER, QUOTED_TOKEN_FILTERS)
            .build());

    // Analyzer for text tokenized into words (split by spaces, periods, and slashes)
    analyzers.put(
        TEXT_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : MAIN_TOKENIZER)
            .put(FILTER, INDEX_TOKEN_FILTERS)
            .build());

    analyzers.put(
        TEXT_SEARCH_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : MAIN_TOKENIZER)
            .put(FILTER, SEARCH_TOKEN_FILTERS)
            .build());

    // Analyzer for getting urn components
    analyzers.put(
        URN_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, MAIN_TOKENIZER)
            .put(FILTER, INDEX_TOKEN_FILTERS)
            .build());

    analyzers.put(
        URN_SEARCH_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, MAIN_TOKENIZER)
            .put(FILTER, SEARCH_TOKEN_FILTERS)
            .build());

    // Support word grams
    for (Map.Entry<String, String> entry :
        Map.of(
                WORD_GRAM_2_ANALYZER, WORD_GRAM_2_FILTER,
                WORD_GRAM_3_ANALYZER, WORD_GRAM_3_FILTER,
                WORD_GRAM_4_ANALYZER, WORD_GRAM_4_FILTER)
            .entrySet()) {
      String analyzerName = entry.getKey();
      String filterName = entry.getValue();
      analyzers.put(
          analyzerName,
          ImmutableMap.<String, Object>builder()
              .put(TOKENIZER, WORD_GRAM_TOKENIZER)
              .put(
                  FILTER,
                  ImmutableList.<Object>builder()
                      .addAll(WORD_GRAM_TOKEN_FILTERS)
                      .add(filterName)
                      .build())
              .build());
    }

    // For special analysis, the substitution can be read from the configuration (chinese tokenizer:
    // ik_smart / smartCN)
    // Analyzer for partial matching (i.e. autocomplete) - Prefix matching of each token
    analyzers.put(
        PARTIAL_ANALYZER,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, StringUtils.isNotBlank(mainTokenizer) ? mainTokenizer : MAIN_TOKENIZER)
            .put(FILTER, PARTIAL_AUTOCOMPLETE_TOKEN_FILTERS)
            .build());

    // Analyzer for partial matching urn components
    analyzers.put(
        PARTIAL_URN_COMPONENT,
        ImmutableMap.<String, Object>builder()
            .put(TOKENIZER, MAIN_TOKENIZER)
            .put(FILTER, PARTIAL_AUTOCOMPLETE_TOKEN_FILTERS)
            .build());

    return analyzers.build();
  }
}
