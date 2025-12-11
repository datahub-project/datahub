/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class UpstreamLineagesMapper {

  public static final UpstreamLineagesMapper INSTANCE = new UpstreamLineagesMapper();

  public static List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> map(
      @Nonnull final com.linkedin.dataset.UpstreamLineage upstreamLineage) {
    return INSTANCE.apply(upstreamLineage);
  }

  public List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> apply(
      @Nonnull final com.linkedin.dataset.UpstreamLineage upstreamLineage) {
    if (!upstreamLineage.hasFineGrainedLineages()
        || upstreamLineage.getFineGrainedLineages() == null) {
      return new ArrayList<>();
    }

    return FineGrainedLineagesMapper.map(upstreamLineage.getFineGrainedLineages());
  }
}
