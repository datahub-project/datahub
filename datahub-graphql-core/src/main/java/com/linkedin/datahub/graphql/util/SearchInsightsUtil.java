/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
