package com.linkedin.metadata.search.elasticsearch.client.shim;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es7CompatibilitySearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearch2SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.ShimConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.opensearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.ParsedGlobal;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.ParsedAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.opensearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.ParsedMissing;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.ParsedNested;
import org.opensearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.opensearch.search.aggregations.bucket.range.ParsedDateRange;
import org.opensearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.opensearch.search.aggregations.bucket.range.ParsedRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongRareTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongRareTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedUnsignedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.StringRareTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.opensearch.search.aggregations.matrix.stats.ParsedMatrixStats;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedAvg;
import org.opensearch.search.aggregations.metrics.ParsedCardinality;
import org.opensearch.search.aggregations.metrics.ParsedExtendedStats;
import org.opensearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.opensearch.search.aggregations.metrics.ParsedStats;
import org.opensearch.search.aggregations.metrics.ParsedSum;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;
import org.opensearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.ParsedDerivative;
import org.opensearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.opensearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.opensearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.opensearch.search.suggest.phrase.PhraseSuggestion;
import org.opensearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestion;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;

/**
 * Factory for creating appropriate SearchClientShim implementations based on the target search
 * engine type and version.
 */
@Slf4j
public class SearchClientShimUtil {

  private static final Map<String, ContextParser<Object, ? extends Aggregation>>
      AGGREGATION_TYPE_MAP = new HashMap<>();
  private static final Map<String, ContextParser<Object, ? extends Suggest.Suggestion<?>>>
      SUGGESTION_TYPE_MAP = new HashMap<>();
  public static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    // We have to set up this mapping because of SearchResponse mapping from XContent using the
    // Aggregations fromXContent
    // in innerFromXContent, unfortunately fromXContent is not an inherited method so there's a lot
    // of repetition
    // ============ BUCKET AGGREGATIONS ============

    // Simple
    AGGREGATION_TYPE_MAP.put(
        InternalSimpleValue.NAME,
        (parser, context) -> ParsedSimpleValue.fromXContent(parser, (String) context));

    // Terms Aggregations
    AGGREGATION_TYPE_MAP.put(
        StringTerms.NAME,
        (parser, context) -> ParsedStringTerms.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        LongTerms.NAME,
        (parser, context) -> ParsedLongTerms.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        DoubleTerms.NAME,
        (parser, context) -> ParsedDoubleTerms.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        UnsignedLongTerms.NAME,
        (parser, context) -> ParsedUnsignedLongTerms.fromXContent(parser, (String) context));

    // Rare Terms
    AGGREGATION_TYPE_MAP.put(
        StringRareTerms.NAME,
        (parser, context) -> ParsedStringRareTerms.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        LongRareTerms.NAME,
        (parser, context) -> ParsedLongRareTerms.fromXContent(parser, (String) context));

    // Multi Terms
    AGGREGATION_TYPE_MAP.put(
        MultiTermsAggregationBuilder.NAME,
        (parser, context) -> ParsedMultiTerms.fromXContent(parser, (String) context));

    // Significant Terms
    AGGREGATION_TYPE_MAP.put(
        SignificantStringTerms.NAME,
        (parser, context) -> ParsedSignificantStringTerms.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        SignificantLongTerms.NAME,
        (parser, context) -> ParsedSignificantLongTerms.fromXContent(parser, (String) context));

    // Histogram Aggregations
    AGGREGATION_TYPE_MAP.put(
        HistogramAggregationBuilder.NAME,
        (parser, context) -> ParsedHistogram.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        DateHistogramAggregationBuilder.NAME,
        (parser, context) -> ParsedDateHistogram.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        AutoDateHistogramAggregationBuilder.NAME,
        (parser, context) -> ParsedAutoDateHistogram.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        VariableWidthHistogramAggregationBuilder.NAME,
        (parser, context) -> ParsedVariableWidthHistogram.fromXContent(parser, (String) context));

    // Range Aggregations
    AGGREGATION_TYPE_MAP.put(
        RangeAggregationBuilder.NAME,
        (parser, context) -> ParsedRange.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        DateRangeAggregationBuilder.NAME,
        (parser, context) -> ParsedDateRange.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        GeoDistanceAggregationBuilder.NAME,
        (parser, context) -> ParsedGeoDistance.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        IpRangeAggregationBuilder.NAME,
        (parser, context) -> ParsedBinaryRange.fromXContent(parser, (String) context));

    // Filter Aggregations
    AGGREGATION_TYPE_MAP.put(
        FilterAggregationBuilder.NAME,
        (parser, context) -> ParsedFilter.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        FiltersAggregationBuilder.NAME,
        (parser, context) -> ParsedFilters.fromXContent(parser, (String) context));

    // Nested Aggregations
    AGGREGATION_TYPE_MAP.put(
        NestedAggregationBuilder.NAME,
        (parser, context) -> ParsedNested.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        ReverseNestedAggregationBuilder.NAME,
        (parser, context) -> ParsedReverseNested.fromXContent(parser, (String) context));

    // Other Bucket Aggregations
    AGGREGATION_TYPE_MAP.put(
        GlobalAggregationBuilder.NAME,
        (parser, context) -> ParsedGlobal.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        MissingAggregationBuilder.NAME,
        (parser, context) -> ParsedMissing.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        InternalSampler.PARSER_NAME,
        (parser, context) -> ParsedSampler.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        CompositeAggregationBuilder.NAME,
        (parser, context) -> ParsedComposite.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        AdjacencyMatrixAggregationBuilder.NAME,
        (parser, context) -> ParsedAdjacencyMatrix.fromXContent(parser, (String) context));

    // ============ METRICS AGGREGATIONS ============

    // Basic Metrics
    AGGREGATION_TYPE_MAP.put(
        AvgAggregationBuilder.NAME,
        (parser, context) -> ParsedAvg.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        SumAggregationBuilder.NAME,
        (parser, context) -> ParsedSum.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        MinAggregationBuilder.NAME,
        (parser, context) -> ParsedMin.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        MaxAggregationBuilder.NAME,
        (parser, context) -> ParsedMax.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        ValueCountAggregationBuilder.NAME,
        (parser, context) -> ParsedValueCount.fromXContent(parser, (String) context));

    // Statistical Metrics
    AGGREGATION_TYPE_MAP.put(
        StatsAggregationBuilder.NAME,
        (parser, context) -> ParsedStats.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        ExtendedStatsAggregationBuilder.NAME,
        (parser, context) -> ParsedExtendedStats.fromXContent(parser, (String) context));

    // Percentile Metrics
    AGGREGATION_TYPE_MAP.put(
        InternalTDigestPercentiles.NAME,
        (parser, context) -> ParsedTDigestPercentiles.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        InternalTDigestPercentileRanks.NAME,
        (parser, context) -> ParsedTDigestPercentileRanks.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        InternalHDRPercentiles.NAME,
        (parser, context) -> ParsedHDRPercentiles.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        InternalHDRPercentileRanks.NAME,
        (parser, context) -> ParsedHDRPercentileRanks.fromXContent(parser, (String) context));

    // Cardinality
    AGGREGATION_TYPE_MAP.put(
        CardinalityAggregationBuilder.NAME,
        (parser, context) -> ParsedCardinality.fromXContent(parser, (String) context));

    // Geo Metrics
    AGGREGATION_TYPE_MAP.put(
        GeoCentroidAggregationBuilder.NAME,
        (parser, context) -> ParsedGeoCentroid.fromXContent(parser, (String) context));

    // Other Metrics
    AGGREGATION_TYPE_MAP.put(
        WeightedAvgAggregationBuilder.NAME,
        (parser, context) -> ParsedWeightedAvg.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        TopHitsAggregationBuilder.NAME,
        (parser, context) -> ParsedTopHits.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        ScriptedMetricAggregationBuilder.NAME,
        (parser, context) -> ParsedScriptedMetric.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        MedianAbsoluteDeviationAggregationBuilder.NAME,
        (parser, context) -> ParsedMedianAbsoluteDeviation.fromXContent(parser, (String) context));

    // ============ PIPELINE AGGREGATIONS ============

    AGGREGATION_TYPE_MAP.put(
        DerivativePipelineAggregationBuilder.NAME,
        (parser, context) -> ParsedDerivative.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        PercentilesBucketPipelineAggregationBuilder.NAME,
        (parser, context) -> ParsedPercentilesBucket.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        StatsBucketPipelineAggregationBuilder.NAME,
        (parser, context) -> ParsedStatsBucket.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        ExtendedStatsBucketPipelineAggregationBuilder.NAME,
        (parser, context) -> ParsedExtendedStatsBucket.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        MinBucketPipelineAggregationBuilder.NAME,
        (parser, context) -> ParsedBucketMetricValue.fromXContent(parser, (String) context));
    AGGREGATION_TYPE_MAP.put(
        InternalBucketMetricValue.NAME,
        (parser, context) -> ParsedBucketMetricValue.fromXContent(parser, (String) context));

    // ============ MATRIX AGGREGATIONS ============

    AGGREGATION_TYPE_MAP.put(
        MatrixStatsAggregationBuilder.NAME,
        (parser, context) -> ParsedMatrixStats.fromXContent(parser, (String) context));

    // ============ SUGGESTIONS ============
    // Required for ES8 shim to parse search responses containing suggestions

    SUGGESTION_TYPE_MAP.put(
        TermSuggestionBuilder.SUGGESTION_NAME,
        (parser, context) -> TermSuggestion.fromXContent(parser, (String) context));
    SUGGESTION_TYPE_MAP.put(
        PhraseSuggestionBuilder.SUGGESTION_NAME,
        (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context));
    SUGGESTION_TYPE_MAP.put(
        CompletionSuggestionBuilder.SUGGESTION_NAME,
        (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context));

    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
    List<NamedXContentRegistry.Entry> aggregationEntries =
        AGGREGATION_TYPE_MAP.entrySet().stream()
            .map(
                entry -> {
                  try {
                    return new NamedXContentRegistry.Entry(
                        Aggregation.class, new ParseField(entry.getKey()), entry.getValue());
                  } catch (Exception e) {
                    log.warn("Unable to get PARSER from class: {}", entry.getValue());
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    namedXContents.addAll(aggregationEntries);

    List<NamedXContentRegistry.Entry> suggestionEntries =
        SUGGESTION_TYPE_MAP.entrySet().stream()
            .map(
                entry -> {
                  try {
                    return new NamedXContentRegistry.Entry(
                        Suggest.Suggestion.class, new ParseField(entry.getKey()), entry.getValue());
                  } catch (Exception e) {
                    log.warn("Unable to get PARSER for suggestion type: {}", entry.getKey());
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    namedXContents.addAll(suggestionEntries);

    X_CONTENT_REGISTRY = new NamedXContentRegistry(namedXContents);
  }

  /**
   * Create a SearchClientShim instance based on the provided configuration. This method will
   * automatically select the appropriate client implementation based on the target search engine
   * type.
   *
   * @param config Configuration specifying the target search engine and connection parameters
   * @return A SearchClientShim implementation suitable for the specified engine type
   * @throws IllegalArgumentException if the engine type is not supported
   * @throws IOException if there's an error creating the client connection
   */
  @Nonnull
  public static SearchClientShim<?> createShim(
      @Nonnull ShimConfiguration config, ObjectMapper objectMapper) throws IOException {
    SearchEngineType engineType = config.getEngineType();

    log.info("Creating SearchClientShim for engine type: {} ", engineType);

    switch (engineType) {
      case ELASTICSEARCH_7:
        return new Es7CompatibilitySearchClientShim(config);

      case ELASTICSEARCH_8:
      case ELASTICSEARCH_9:
        return new Es8SearchClientShim(config, objectMapper);

      case OPENSEARCH_2:
        return new OpenSearch2SearchClientShim(config);

      default:
        throw new IllegalArgumentException("Unsupported search engine type: " + engineType);
    }
  }

  /**
   * Auto-detect the search engine type by connecting to the cluster and examining the version. This
   * is useful when you want to automatically adapt to the target environment.
   *
   * @param config Base configuration with connection parameters (engine type will be overridden)
   * @return A SearchClientShim implementation suitable for the detected engine type
   * @throws IOException if there's an error detecting the engine type or creating the client
   */
  @Nonnull
  public static SearchClientShim<?> createShimWithAutoDetection(
      @Nonnull ShimConfiguration config, ObjectMapper objectMapper) throws IOException {
    SearchEngineType detectedType = detectEngineType(config, objectMapper);

    log.info("Auto-detected search engine type: {}", detectedType);

    // Create a new config with the detected engine type
    ShimConfiguration detectedConfig =
        new ShimConfigurationBuilder(config).withEngineType(detectedType).build();

    return createShim(detectedConfig, objectMapper);
  }

  /**
   * Detect the search engine type by attempting to connect and examine the cluster information.
   * This method tries different client types in order of preference until one succeeds.
   *
   * @param config Base configuration with connection parameters
   * @return The detected SearchEngineType
   * @throws IOException if unable to detect the engine type
   */
  @Nonnull
  private static SearchEngineType detectEngineType(
      @Nonnull ShimConfiguration config, ObjectMapper objectMapper) throws IOException {
    List<String> failures = new ArrayList<>();
    String endpoint = config.getHost() + ":" + config.getPort();

    // Try OpenSearch 2.x first (most commonly deployed currently)
    try {
      ShimConfiguration testConfig =
          new ShimConfigurationBuilder(config)
              .withEngineType(SearchEngineType.OPENSEARCH_2)
              .build();

      try (SearchClientShim<?> testShim = new OpenSearch2SearchClientShim(testConfig)) {
        String version = testShim.getEngineVersion();

        if (version != null && version.startsWith("2.")) {
          return SearchEngineType.OPENSEARCH_2;
        }
        failures.add("OpenSearch: connected but version='" + version + "' (expected 2.x)");
      }
    } catch (Exception e) {
      String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      failures.add("OpenSearch: " + msg);
      log.debug("OpenSearch detection failed: {}", msg);
    }

    // Try Elasticsearch 7.x with high-level client
    try {
      ShimConfiguration testConfig =
          new ShimConfigurationBuilder(config)
              .withEngineType(SearchEngineType.ELASTICSEARCH_7)
              .build();

      try (SearchClientShim<?> testShim = new Es7CompatibilitySearchClientShim(testConfig)) {
        String version = testShim.getEngineVersion();

        if (version != null && version.startsWith("7.")) {
          return SearchEngineType.ELASTICSEARCH_7;
        }
        failures.add("ES7: connected but version='" + version + "' (expected 7.x)");
      }
    } catch (Exception e) {
      String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      failures.add("ES7: " + msg);
      log.debug("Elasticsearch 7.x detection failed: {}", msg);
    }

    // Try Elasticsearch 8.x/9.x with new Java client
    try {
      ShimConfiguration testConfig =
          new ShimConfigurationBuilder(config)
              .withEngineType(SearchEngineType.ELASTICSEARCH_8)
              .build();

      try (SearchClientShim<?> testShim = new Es8SearchClientShim(testConfig, objectMapper)) {
        String version = testShim.getEngineVersion();

        if (version != null && version.startsWith("8.")) {
          return SearchEngineType.ELASTICSEARCH_8;
        } else if (version != null && version.startsWith("9.")) {
          return SearchEngineType.ELASTICSEARCH_9;
        }
        failures.add(
            "ES8: connected but version='"
                + version
                + "' (expected 8.x/9.x). Misconfiguration? ES8 client may be talking to an ES7 cluster.");
      }
    } catch (Exception e) {
      String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      failures.add("ES8: " + msg);
      log.debug("Elasticsearch 8.x/9.x detection failed: {}", msg);
    }

    String detail = String.join("; ", failures);
    log.warn(
        "Search engine type detection failed for {} (no matching probe). Details: {}",
        endpoint,
        detail);
    throw new IOException(
        "Unable to detect search engine type for "
            + endpoint
            + ". Ensure the cluster is accessible and running a supported version. Details: "
            + detail);
  }

  /** Builder class for creating ShimConfiguration instances */
  public static class ShimConfigurationBuilder {
    private SearchEngineType engineType;
    private String host;
    private Integer port;
    private String username;
    private String password;
    private boolean useSSL = false;
    private String pathPrefix;
    private boolean useAwsIamAuth = false;
    private String region;
    private Integer threadCount = 1;
    private Integer connectionRequestTimeout = 5000;
    private SSLContext sSLContext;

    public ShimConfigurationBuilder() {}

    public ShimConfigurationBuilder(@Nonnull ShimConfiguration existing) {
      this.engineType = existing.getEngineType();
      this.host = existing.getHost();
      this.port = existing.getPort();
      this.username = existing.getUsername();
      this.password = existing.getPassword();
      this.useSSL = existing.isUseSSL();
      this.pathPrefix = existing.getPathPrefix();
      this.useAwsIamAuth = existing.isUseAwsIamAuth();
      this.region = existing.getRegion();
      this.threadCount = existing.getThreadCount();
      this.connectionRequestTimeout = existing.getConnectionRequestTimeout();
      this.sSLContext = existing.getSSLContext();
    }

    public ShimConfigurationBuilder withEngineType(SearchEngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public ShimConfigurationBuilder withHost(String host) {
      this.host = host;
      return this;
    }

    public ShimConfigurationBuilder withPort(Integer port) {
      this.port = port;
      return this;
    }

    public ShimConfigurationBuilder withCredentials(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }

    public ShimConfigurationBuilder withSSL(boolean useSSL) {
      this.useSSL = useSSL;
      return this;
    }

    public ShimConfigurationBuilder withPathPrefix(String pathPrefix) {
      this.pathPrefix = pathPrefix;
      return this;
    }

    public ShimConfigurationBuilder withAwsIamAuth(boolean useAwsIamAuth, String region) {
      this.useAwsIamAuth = useAwsIamAuth;
      this.region = region;
      return this;
    }

    public ShimConfigurationBuilder withThreadCount(Integer threadCount) {
      this.threadCount = threadCount;
      return this;
    }

    public ShimConfigurationBuilder withConnectionRequestTimeout(Integer connectionRequestTimeout) {
      this.connectionRequestTimeout = connectionRequestTimeout;
      return this;
    }

    public ShimConfigurationBuilder withSSLContext(SSLContext sSLContext) {
      this.sSLContext = sSLContext;
      return this;
    }

    public ShimConfiguration build() {
      return new ShimConfigurationImpl(
          engineType,
          host,
          port,
          username,
          password,
          useSSL,
          pathPrefix,
          useAwsIamAuth,
          region,
          threadCount,
          connectionRequestTimeout,
          sSLContext);
    }
  }

  /** Internal implementation of ShimConfiguration */
  @Data
  private static class ShimConfigurationImpl implements ShimConfiguration {
    private final SearchEngineType engineType;
    private final String host;
    private final Integer port;
    private final String username;
    private final String password;
    private final boolean useSSL;
    private final String pathPrefix;
    private final boolean useAwsIamAuth;
    private final String region;
    private final Integer threadCount;
    private final Integer connectionRequestTimeout;
    private final SSLContext sSLContext;

    public ShimConfigurationImpl(
        SearchEngineType engineType,
        String host,
        Integer port,
        String username,
        String password,
        boolean useSSL,
        String pathPrefix,
        boolean useAwsIamAuth,
        String region,
        Integer threadCount,
        Integer connectionRequestTimeout,
        SSLContext sslContext) {
      this.engineType = engineType;
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.useSSL = useSSL;
      this.pathPrefix = pathPrefix;
      this.useAwsIamAuth = useAwsIamAuth;
      this.region = region;
      this.threadCount = threadCount;
      this.connectionRequestTimeout = connectionRequestTimeout;
      this.sSLContext = sslContext;
    }
  }
}
