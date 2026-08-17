package io.datahubproject.openapi.openlineage.mapping;

import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RunEventMapper {

  public RunEventMapper() {}

  public Stream<MetadataChangeProposal> map(
      OpenLineage.RunEvent runEvent, RunEventMapper.MappingConfig mappingConfig) {
    try {
      return OpenLineageToDataHub.convertRunEventToJob(runEvent, mappingConfig.getDatahubConfig())
          .toMcps(mappingConfig.datahubConfig)
          .stream();
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Stream<MetadataChangeProposal> map(
      OpenLineage.JobEvent jobEvent, RunEventMapper.MappingConfig mappingConfig) {
    try {
      return OpenLineageToDataHub.convertJobEventToJob(jobEvent, mappingConfig.getDatahubConfig())
          .toMcps(mappingConfig.datahubConfig)
          .stream();
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Stream<MetadataChangeProposal> map(
      OpenLineage.DatasetEvent datasetEvent, RunEventMapper.MappingConfig mappingConfig) {
    try {
      return OpenLineageToDataHub.convertDatasetEventToMcps(
          datasetEvent, mappingConfig.getDatahubConfig())
          .stream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Builder
  @Getter
  public static class MappingConfig {
    DatahubOpenlineageConfig datahubConfig;
  }
}
