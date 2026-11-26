package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/** Builder for generating settings for elasticsearch indices */
@Slf4j
public class V2LegacySettingsBuilder implements SettingsBuilder {

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
  // Rich text analyzers for document bodies. The standard tokenizer splits on whitespace and caps
  // tokens at 255 chars, so no term can hit Lucene's 32,766-byte limit (word_delimited can't: its
  // tokenizer never splits whitespace). A word_delimiter_graph then splits identifiers (snake_case,
  // dotted, camelCase, letter/digit) keeping the whole token, plus snowball stemming. Defined only
  // on indices with a RICH_TEXT field (see shouldDefineRichTextAnalyzers), so only those reindex.
  // The query builder never names these analyzers on a cross-index clause (it relies on the field's
  // mapping search_analyzer), so indices without the field never need them defined.
  public static final String RICH_TEXT_ANALYZER = "rich_text_delimited";
  public static final String RICH_TEXT_SEARCH_ANALYZER = "query_rich_text_delimited";
  public static final String RICH_TEXT_DELIMITER = "rich_text_delimiter";
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
  public static final String STANDARD_TOKENIZER = "standard";
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

  public static final List<String> WORD_GRAM_TOKEN_FILTERS_V2_5 =
      ImmutableList.of(ASCII_FOLDING, LOWERCASE, TRIM, REMOVE_QUOTES, STEM_OVERRIDE, SNOWBALL);

  private static final String PRIMARY_SEARCH_GROUP = "primary";

  final String mainTokenizer;
  @Nonnull private final IndexConvention indexConvention;
  private final boolean enableSearchV2_5;
  @Nullable private final EntityRegistry entityRegistry;

  public V2LegacySettingsBuilder(
      @Nonnull IndexConfiguration indexConfiguration,
      @Nonnull IndexConvention indexConvention,
      boolean enableSearchV2_5) {
    this(indexConfiguration, indexConvention, enableSearchV2_5, null);
  }

  public V2LegacySettingsBuilder(
      @Nonnull IndexConfiguration indexConfiguration,
      @Nonnull IndexConvention indexConvention,
      boolean enableSearchV2_5,
      @Nullable EntityRegistry entityRegistry) {
    this.mainTokenizer = indexConfiguration.getMainTokenizer();
    this.indexConvention = indexConvention;
    this.enableSearchV2_5 = enableSearchV2_5;
    this.entityRegistry = entityRegistry;
  }

  @Override
  public Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName) {
    try {
      // For v2, only apply settings to indices that match the v2 entity naming pattern. Uses the
      // prefix-INDEPENDENT type check: the index name is already fully resolved (it may carry a
      // per-operation prefix), and this bootstrap path only needs its type — a prefix-scoped check
      // with EMPTY would reject an index whose prefix came from an in-flight operation.
      if (!indexConvention.isV2EntityIndexType(indexName)) {
        // Return empty settings if this is not a v2 entity index
        return ImmutableMap.of();
      }

      boolean useV2_5 = enableSearchV2_5;
      if (useV2_5 && entityRegistry != null) {
        useV2_5 = isPrimarySearchGroup(indexName);
      }

      return buildSettings(indexConfiguration, useV2_5, shouldDefineRichTextAnalyzers(indexName));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Whether to define the rich_text analyzers on this index. Only a RICH_TEXT field's mapping
   * references them (as its index/search analyzer), and only the {@code document} entity has such a
   * field, so only that index needs the definitions — and only that index reindexes. The query
   * builder never names these analyzers on a cross-index clause (it relies on each field's mapping
   * search_analyzer), so indices without the field don't need them defined. We still default to
   * defining them when we can't check the entity (registry unavailable, e.g. some test fixtures),
   * since the mapping would otherwise reference an undefined analyzer and index creation fails.
   */
  private boolean shouldDefineRichTextAnalyzers(@Nonnull String indexName) {
    if (entityRegistry == null) {
      return true;
    }
    // Index settings/mappings are built at index-creation time — no request context;
    // resolves the deploy-wide prefix.
    Optional<String> entityName =
        indexConvention.getEntityName(OperationFingerprint.EMPTY, indexName);
    if (entityName.isEmpty()) {
      return true;
    }
    try {
      EntitySpec spec = entityRegistry.getEntitySpec(entityName.get());
      return spec == null
          || spec.getSearchableFieldSpecs().stream()
              .anyMatch(
                  fieldSpec ->
                      fieldSpec.getSearchableAnnotation().getFieldType()
                          == SearchableAnnotation.FieldType.RICH_TEXT);
    } catch (IllegalArgumentException e) {
      return true;
    }
  }

  /**
   * Checks whether the entity for the given index belongs to the "primary" search group. Only
   * primary entities are searched by users through the search bar and benefit from V2.5 analyzer
   * upgrades (stemmer overrides, synonyms). Non-primary entities (query, timeseries, schemaField)
   * skip V2.5 to avoid unnecessary reindexing.
   */
  private boolean isPrimarySearchGroup(@Nonnull String indexName) {
    // Index settings/mappings are built at index-creation time — no request context;
    // resolves the deploy-wide prefix.
    Optional<String> entityName =
        indexConvention.getEntityName(OperationFingerprint.EMPTY, indexName);
    if (entityName.isPresent()) {
      try {
        EntitySpec spec = entityRegistry.getEntitySpec(entityName.get());
        if (spec != null) {
          return PRIMARY_SEARCH_GROUP.equals(spec.getSearchGroup());
        }
      } catch (IllegalArgumentException e) {
        log.warn("No EntitySpec found for entity '{}', defaulting to primary", entityName.get());
      }
    }
    // Default to primary if we can't determine the entity
    return true;
  }

  private Map<String, Object> buildSettings(
      @Nonnull IndexConfiguration indexConfiguration, boolean useV2_5, boolean hasRichText)
      throws IOException {
    ImmutableMap.Builder<String, Object> settings = ImmutableMap.builder();
    settings.put(MAX_NGRAM_DIFF, 17);
    settings.put(
        ANALYSIS,
        ImmutableMap.<String, Object>builder()
            .put(FILTER, buildFilters(indexConfiguration, useV2_5, hasRichText))
            .put(TOKENIZER, buildTokenizers())
            .put(NORMALIZER, buildNormalizers())
            .put(ANALYZER, buildAnalyzers(mainTokenizer, useV2_5, hasRichText))
            .build());
    return settings.build();
  }

  /** The analysis section alone, for V3 indices that reuse the V2 analyzers. */
  @SuppressWarnings("unchecked")
  public Map<String, Object> buildAnalysisSettings(
      @Nonnull IndexConfiguration indexConfiguration, boolean useV2_5) throws IOException {
    // hasRichText=true: the v3 consolidated index can hold a RICH_TEXT field (e.g. document), and
    // its query-time analyzer resolves to rich_text_delimited, so the definitions must be present.
    return buildSettings(indexConfiguration, useV2_5, true).entrySet().stream()
        .filter(entry -> ANALYSIS.equals(entry.getKey()))
        .findFirst()
        .map(entry -> (Map<String, Object>) entry.getValue())
        .orElseGet(Map::of);
  }

  private Map<String, Object> buildFilters(
      @Nonnull IndexConfiguration indexConfiguration, boolean useV2_5, boolean hasRichText)
      throws IOException {
    Map<String, Object> base =
        useV2_5 ? buildFiltersV2_5(indexConfiguration) : buildFiltersV2(indexConfiguration);
    if (!hasRichText) {
      return base;
    }
    // word_delimiter for the rich_text analyzers. preserve_original keeps the whole token;
    // split_on_numerics matches the query builder's splitAlphanumericTokens, so "arityus2017" is
    // found by "arityus", "2017", or the whole token.
    return ImmutableMap.<String, Object>builder()
        .putAll(base)
        .put(
            RICH_TEXT_DELIMITER,
            ImmutableMap.<String, Object>builder()
                .put(TYPE, WORD_DELIMITER_GRAPH)
                .put(SPLIT_ON_NUMERICS, true)
                .put(SPLIT_ON_CASE_CHANGE, true)
                .put(PRESERVE_ORIGINAL, true)
                .build())
        .build();
  }

  /** V2 filters: base stem overrides and base synonyms with strict parsing. */
  private Map<String, Object> buildFiltersV2(@Nonnull IndexConfiguration indexConfiguration)
      throws IOException {
    PathMatchingResourcePatternResolver resourceResolver =
        new PathMatchingResourcePatternResolver();
    ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();

    addCommonFilters(filters, indexConfiguration);

    Resource stemOverride =
        resourceResolver.getResource("classpath:elasticsearch/stem_override.txt");
    filters.put(STEM_OVERRIDE, buildStemOverrideFilter(stemOverride));

    addCommonPostStemFilters(filters);

    List<String> synonymRules =
        readSynonymFile(
            resourceResolver.getResource("classpath:elasticsearch/synonyms/default.txt"));
    filters.put(
        DEFAULT_SYN_GRAPH,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "synonym_graph")
            .put(LENIENT, "false")
            .put(SYNONYMS, synonymRules)
            .build());

    addWordGramFilters(filters);

    return filters.build();
  }

  /**
   * V2.5 filters: extended stem overrides (with British spelling normalization) and expanded
   * synonyms with lenient parsing for multi-word and hyphenated terms.
   */
  private Map<String, Object> buildFiltersV2_5(@Nonnull IndexConfiguration indexConfiguration)
      throws IOException {
    PathMatchingResourcePatternResolver resourceResolver =
        new PathMatchingResourcePatternResolver();
    ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();

    addCommonFilters(filters, indexConfiguration);

    Resource stemOverride =
        resourceResolver.getResource("classpath:elasticsearch/stem_override_v2_5.txt");
    filters.put(STEM_OVERRIDE, buildStemOverrideFilter(stemOverride));

    addCommonPostStemFilters(filters);

    List<String> synonymRules =
        readSynonymFile(
            resourceResolver.getResource("classpath:elasticsearch/synonyms/default_v2_5.txt"));
    filters.put(
        DEFAULT_SYN_GRAPH,
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "synonym_graph")
            .put(LENIENT, "true")
            .put(SYNONYMS, synonymRules)
            .build());

    addWordGramFilters(filters);

    return filters.build();
  }

  /** Filters shared by V2 and V2.5: word delimiters, stop words, min length. */
  private static void addCommonFilters(
      ImmutableMap.Builder<String, Object> filters,
      @Nonnull IndexConfiguration indexConfiguration) {
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
        ImmutableMap.<String, Object>builder()
            .put(TYPE, "length")
            .put("min", indexConfiguration.getMinSearchFilterLength())
            .build());
  }

  /**
   * Filters shared by V2 and V2.5 that come after stem overrides: pattern capture, quotes,
   * multiplexers.
   */
  private static void addCommonPostStemFilters(ImmutableMap.Builder<String, Object> filters) {
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
  }

  private static Map<String, Object> buildStemOverrideFilter(@Nonnull Resource stemOverride)
      throws IOException {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stemOverride.getInputStream()))) {
      return ImmutableMap.<String, Object>builder()
          .put(TYPE, "stemmer_override")
          .put(
              "rules",
              reader
                  .lines()
                  .map(String::trim)
                  .map(String::toLowerCase)
                  .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                  .collect(Collectors.toList()))
          .build();
    }
  }

  private static List<String> readSynonymFile(@Nonnull Resource synonymFile) throws IOException {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(synonymFile.getInputStream()))) {
      return reader
          .lines()
          .map(String::trim)
          .map(String::toLowerCase)
          .filter(line -> !line.isEmpty() && !line.startsWith("#"))
          .collect(Collectors.toList());
    }
  }

  private static void addWordGramFilters(ImmutableMap.Builder<String, Object> filters) {
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
  private static Map<String, Object> buildAnalyzers(
      String mainTokenizer, boolean useV2_5, boolean hasRichText) {
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
                      .addAll(useV2_5 ? WORD_GRAM_TOKEN_FILTERS_V2_5 : WORD_GRAM_TOKEN_FILTERS)
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

    if (hasRichText) {
      // Index: standard tokenizer -> split identifiers -> flatten graph -> lowercase -> stem.
      // The delimiter runs before lowercase so it can still see camelCase boundaries.
      analyzers.put(
          RICH_TEXT_ANALYZER,
          ImmutableMap.<String, Object>builder()
              .put(TOKENIZER, STANDARD_TOKENIZER)
              .put(
                  FILTER,
                  ImmutableList.of(
                      RICH_TEXT_DELIMITER, FLATTEN_GRAPH, LOWERCASE, ASCII_FOLDING, SNOWBALL))
              .build());
      // Search: same chain minus flatten_graph (the query parser consumes the token graph
      // directly).
      analyzers.put(
          RICH_TEXT_SEARCH_ANALYZER,
          ImmutableMap.<String, Object>builder()
              .put(TOKENIZER, STANDARD_TOKENIZER)
              .put(
                  FILTER, ImmutableList.of(RICH_TEXT_DELIMITER, LOWERCASE, ASCII_FOLDING, SNOWBALL))
              .build());
    }

    return analyzers.build();
  }
}
