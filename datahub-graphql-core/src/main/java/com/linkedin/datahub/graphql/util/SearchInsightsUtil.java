package com.linkedin.datahub.graphql.util;

import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.generated.SearchInsight;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SearchInsightsUtil {

  public static List<SearchInsight> getInsightsFromFeatures(@Nonnull final DoubleMap features) {
    // Currently no features are extracted.
    return Collections.emptyList();
  }
}
