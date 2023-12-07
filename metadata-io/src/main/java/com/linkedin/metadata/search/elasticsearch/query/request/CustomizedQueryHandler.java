package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(builderMethodName = "hiddenBuilder")
@Getter
public class CustomizedQueryHandler {
  private CustomSearchConfiguration customSearchConfiguration;

  @Builder.Default
  private List<Map.Entry<Pattern, QueryConfiguration>> queryConfigurations = List.of();

  public Optional<QueryConfiguration> lookupQueryConfig(String query) {
    return queryConfigurations.stream()
        .filter(e -> e.getKey().matcher(query).matches())
        .map(Map.Entry::getValue)
        .findFirst();
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
    }
    return builder;
  }
}
