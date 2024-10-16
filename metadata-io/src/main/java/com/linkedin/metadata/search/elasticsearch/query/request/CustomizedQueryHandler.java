package com.linkedin.metadata.search.elasticsearch.query.request;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.custom.AutocompleteConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchModule;

@Slf4j
@Builder(builderMethodName = "hiddenBuilder")
@Getter
public class CustomizedQueryHandler {
  private static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  private CustomSearchConfiguration customSearchConfiguration;

  @Builder.Default
  private List<Map.Entry<Pattern, QueryConfiguration>> queryConfigurations = List.of();

  @Builder.Default
  private List<Map.Entry<Pattern, AutocompleteConfiguration>> autocompleteConfigurations =
      List.of();

  public Optional<QueryConfiguration> lookupQueryConfig(String query) {
    return queryConfigurations.stream()
        .filter(e -> e.getKey().matcher(query).matches())
        .map(Map.Entry::getValue)
        .findFirst();
  }

  public Optional<AutocompleteConfiguration> lookupAutocompleteConfig(String query) {
    return autocompleteConfigurations.stream()
        .filter(e -> e.getKey().matcher(query).matches())
        .map(Map.Entry::getValue)
        .findFirst();
  }

  public static String unquote(String query) {
    return query.replaceAll("[\"']", "");
  }

  public static boolean isQuoted(String query) {
    return Stream.of("\"", "'").anyMatch(query::contains);
  }

  public static Optional<BoolQueryBuilder> boolQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      QueryConfiguration customQueryConfiguration,
      String query) {
    if (customQueryConfiguration.getBoolQuery() != null) {
      log.debug(
          "Using custom query configuration queryRegex: {}",
          customQueryConfiguration.getQueryRegex());
    }
    return Optional.ofNullable(customQueryConfiguration.getBoolQuery())
        .map(bq -> toBoolQueryBuilder(objectMapper, query, bq));
  }

  public static Optional<BoolQueryBuilder> boolQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      AutocompleteConfiguration customAutocompleteConfiguration,
      String query) {
    if (customAutocompleteConfiguration.getBoolQuery() != null) {
      log.debug(
          "Using custom query autocomplete queryRegex: {}",
          customAutocompleteConfiguration.getQueryRegex());
    }
    return Optional.ofNullable(customAutocompleteConfiguration.getBoolQuery())
        .map(bq -> toBoolQueryBuilder(objectMapper, query, bq));
  }

  private static BoolQueryBuilder toBoolQueryBuilder(
      @Nonnull ObjectMapper objectMapper, String query, BoolQueryConfiguration boolQuery) {
    try {
      String jsonFragment =
          objectMapper
              .writeValueAsString(boolQuery)
              .replace("\"{{query_string}}\"", objectMapper.writeValueAsString(query))
              .replace(
                  "\"{{unquoted_query_string}}\"", objectMapper.writeValueAsString(unquote(query)));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return BoolQueryBuilder.fromXContent(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FunctionScoreQueryBuilder functionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull QueryConfiguration customQueryConfiguration,
      QueryBuilder queryBuilder,
      String query) {
    return toFunctionScoreQueryBuilder(
        objectMapper, queryBuilder, customQueryConfiguration.getFunctionScore(), query);
  }

  public static Optional<FunctionScoreQueryBuilder> functionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull AutocompleteConfiguration customAutocompleteConfiguration,
      QueryBuilder queryBuilder,
      @Nullable QueryConfiguration customQueryConfiguration,
      String query) {

    Optional<FunctionScoreQueryBuilder> result = Optional.empty();

    if ((customAutocompleteConfiguration.getFunctionScore() == null
            || customAutocompleteConfiguration.getFunctionScore().isEmpty())
        && customAutocompleteConfiguration.isInheritFunctionScore()
        && customQueryConfiguration != null) {
      log.debug(
          "Inheriting query configuration for autocomplete function scoring: "
              + customQueryConfiguration);
      // inherit if not overridden
      result =
          Optional.of(
              toFunctionScoreQueryBuilder(
                  objectMapper, queryBuilder, customQueryConfiguration.getFunctionScore(), query));
    } else if (customAutocompleteConfiguration.getFunctionScore() != null
        && !customAutocompleteConfiguration.getFunctionScore().isEmpty()) {
      log.debug("Applying custom autocomplete function scores.");
      result =
          Optional.of(
              toFunctionScoreQueryBuilder(
                  objectMapper,
                  queryBuilder,
                  customAutocompleteConfiguration.getFunctionScore(),
                  query));
    }

    return result;
  }

  private static FunctionScoreQueryBuilder toFunctionScoreQueryBuilder(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull QueryBuilder queryBuilder,
      @Nonnull Map<String, Object> params,
      String query) {
    try {
      HashMap<String, Object> body = new HashMap<>(params);
      if (!body.isEmpty()) {
        log.debug("Using custom scoring functions: {}", body);
      }

      body.put("query", objectMapper.readValue(queryBuilder.toString(), Map.class));

      String jsonFragment =
          objectMapper
              .writeValueAsString(Map.of("function_score", body))
              .replace("\"{{query_string}}\"", objectMapper.writeValueAsString(query))
              .replace(
                  "\"{{unquoted_query_string}}\"", objectMapper.writeValueAsString(unquote(query)));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return (FunctionScoreQueryBuilder) FunctionScoreQueryBuilder.parseInnerQueryBuilder(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static CustomizedQueryHandlerBuilder builder(
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    CustomizedQueryHandlerBuilder builder =
        hiddenBuilder().customSearchConfiguration(customSearchConfiguration);

    if (customSearchConfiguration != null) {
      builder.queryConfigurations(
          customSearchConfiguration.getQueryConfigurations().stream()
              .map(cfg -> Map.entry(Pattern.compile(cfg.getQueryRegex()), cfg))
              .collect(Collectors.toList()));
      builder.autocompleteConfigurations(
          customSearchConfiguration.getAutocompleteConfigurations().stream()
              .map(cfg -> Map.entry(Pattern.compile(cfg.getQueryRegex()), cfg))
              .collect(Collectors.toList()));
    }

    return builder;
  }
}
