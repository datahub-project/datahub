/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

public interface TimelineService {

  List<ChangeTransaction> getTimeline(
      @Nonnull final Urn urn,
      @Nonnull Set<ChangeCategory> elements,
      long startMillis,
      long endMillis,
      String startVersionStamp,
      String endVersionStamp,
      boolean rawDiffRequested)
      throws JsonProcessingException;
}
