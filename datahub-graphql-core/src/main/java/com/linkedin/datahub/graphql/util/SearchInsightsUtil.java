package com.linkedin.datahub.graphql.util;

import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.generated.SearchInsight;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class SearchInsightsUtil {

  public static List<SearchInsight> getInsightsFromFeatures(@Nullable final DoubleMap features) {
    // Currently no features are extracted.
    if (features == null) {
      return Collections.emptyList();
    }

    return Collections.emptyList();
  }

  private SearchInsightsUtil() {}
}
