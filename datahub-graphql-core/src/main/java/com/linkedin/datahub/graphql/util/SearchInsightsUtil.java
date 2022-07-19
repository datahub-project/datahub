package com.linkedin.datahub.graphql.util;

import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.generated.SearchInsight;
// import com.linkedin.metadata.search.features.Features;
import java.util.ArrayList;
import java.util.List;
// import java.util.Optional;
import javax.annotation.Nonnull;

public class SearchInsightsUtil {

  @Nonnull
  public static List<SearchInsight> getInsightsFromFeatures(@Nonnull final DoubleMap features) {
    // Currently no features are extracted.
    final List<SearchInsight> insights = new ArrayList<>();
    // TODO: Consider which search insights would be best suited for this.
    // final Optional<SearchInsight> queryCountInsight = extractQueryCountInsight(features);
    // queryCountInsight.ifPresent(insights::add);
    return insights;
  }

  /*@Nonnull
  private static Optional<SearchInsight> extractQueryCountInsight(final DoubleMap features) {
    if (features.containsKey(Features.Name.QUERY_COUNT.toString())) {
      // Search result has query count.
      Double queryCount = features.get(Features.Name.QUERY_COUNT.toString());
      if (queryCount > 0) {
        // Show the query count insight.
        String queryCountStr = queryCount > 1000 ? "1000+" : String.valueOf((int) queryCount.doubleValue());
        return Optional.of(new SearchInsight(String.format("Queried %s times in the past month", queryCountStr), "\uD83D\uDE80"));
      }
    }
    return Optional.empty();
  }*/

  private SearchInsightsUtil() { }
}
