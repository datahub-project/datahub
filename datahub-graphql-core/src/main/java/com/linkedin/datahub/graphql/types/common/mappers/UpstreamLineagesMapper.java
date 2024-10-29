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
