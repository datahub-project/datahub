package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.MLModelLineageInfo;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps the lineage fields shared by MLModelProperties and MLModelGroupProperties. Jobs are listed
 * from both the deprecated URN arrays (written by ingestion) and the edge arrays (written from the
 * UI).
 */
public class MLModelLineageInfoMapper {

  private MLModelLineageInfoMapper() {}

  @SuppressWarnings("deprecation")
  public static MLModelLineageInfo map(@Nonnull final DataMap propertiesData) {
    final com.linkedin.ml.metadata.MLModelLineageInfo lineageInfo =
        new com.linkedin.ml.metadata.MLModelLineageInfo(propertiesData);
    final MLModelLineageInfo result = new MLModelLineageInfo();
    if (lineageInfo.hasTrainingJobs() || lineageInfo.hasTrainingJobEdges()) {
      result.setTrainingJobs(
          jobUrns(lineageInfo.getTrainingJobs(), lineageInfo.getTrainingJobEdges()));
    }
    if (lineageInfo.hasDownstreamJobs() || lineageInfo.hasDownstreamJobEdges()) {
      result.setDownstreamJobs(
          jobUrns(lineageInfo.getDownstreamJobs(), lineageInfo.getDownstreamJobEdges()));
    }
    return result;
  }

  private static List<String> jobUrns(@Nullable UrnArray jobs, @Nullable EdgeArray jobEdges) {
    return Stream.concat(
            jobs == null ? Stream.empty() : jobs.stream(),
            jobEdges == null ? Stream.empty() : jobEdges.stream().map(Edge::getDestinationUrn))
        .map(Urn::toString)
        .distinct()
        .collect(Collectors.toList());
  }
}
