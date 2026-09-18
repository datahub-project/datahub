package com.linkedin.gms.factory.search.canonicalization;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Optional;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Supplies the query time canonicalizer, which is attached to the system {@link
 * io.datahubproject.metadata.context.OperationContext} and inherited by every derived session
 * context.
 *
 * <p>The bean is always defined. When the feature is off or misconfigured this resolves to an exact
 * pass-through, so no consumer needs null handling and a bad config cannot prevent startup. That
 * pass-through still reports {@code canonicalization.skipped} when metrics are available, so an
 * operator can tell "never runs" apart from "runs and does not help".
 */
@Configuration
public class QueryTimeCanonicalizerFactory {

  @Bean
  public QueryTimeCanonicalizer queryTimeCanonicalizer(
      final ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Nullable final MetricUtils metricUtils) {
    return QueryTimeCanonicalizer.fromConfig(
        Optional.ofNullable(configurationProvider.getElasticSearch())
            .map(ElasticSearchConfiguration::getSearch)
            .map(SearchConfiguration::getCanonicalization)
            .orElse(null),
        metricUtils);
  }
}
