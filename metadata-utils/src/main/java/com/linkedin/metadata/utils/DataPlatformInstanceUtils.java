package com.linkedin.metadata.utils;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DataProcessKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.MLFeatureTableKey;
import com.linkedin.metadata.key.MLModelDeploymentKey;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.metadata.key.MLModelKey;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataPlatformInstanceUtils {
  private DataPlatformInstanceUtils() {}

  private static DataPlatformUrn getPlatformUrn(String name) {
    return new DataPlatformUrn(name.toLowerCase());
  }

  private static Urn getDefaultDataPlatform(String entityType, RecordTemplate keyAspect)
      throws URISyntaxException {
    switch (entityType) {
      case "dataset":
        return ((DatasetKey) keyAspect).getPlatform();
      case "chart":
        return getPlatformUrn(((ChartKey) keyAspect).getDashboardTool());
      case "dashboard":
        return getPlatformUrn(((DashboardKey) keyAspect).getDashboardTool());
      case "dataFlow":
        return getPlatformUrn(((DataFlowKey) keyAspect).getOrchestrator());
      case "dataJob":
        return getPlatformUrn(
            DataFlowUrn.createFromUrn(((DataJobKey) keyAspect).getFlow()).getOrchestratorEntity());
      case "dataProcess":
        return getPlatformUrn(((DataProcessKey) keyAspect).getOrchestrator());
      case "mlModel":
        return ((MLModelKey) keyAspect).getPlatform();
      case "mlFeatureTable":
        return ((MLFeatureTableKey) keyAspect).getPlatform();
      case "mlModelDeployment":
        return ((MLModelDeploymentKey) keyAspect).getPlatform();
      case "mlModelGroup":
        return ((MLModelGroupKey) keyAspect).getPlatform();
      default:
        log.debug(
            String.format(
                "Failed to generate default platform for unknown entity type %s", entityType));
        return null;
    }
  }

  @Nonnull
  public static DataPlatformUrn getDataPlatform(Urn urn) {
    try {
      switch (urn.getEntityType()) {
        case "dataset":
          return DatasetUrn.createFromUrn(urn).getPlatformEntity();
        case "chart":
          return getPlatformUrn(ChartUrn.createFromUrn(urn).getDashboardToolEntity());
        case "dashboard":
          return getPlatformUrn(DashboardUrn.createFromUrn(urn).getDashboardToolEntity());
        case "dataFlow":
          return getPlatformUrn(DataFlowUrn.createFromUrn(urn).getOrchestratorEntity());
        case "dataJob":
          return getPlatformUrn(
              DataFlowUrn.createFromUrn(DataJobUrn.createFromUrn(urn).getFlowEntity())
                  .getOrchestratorEntity());
        case "dataProcess":
          return getPlatformUrn(DataProcessUrn.createFromUrn(urn).getOrchestratorEntity());
        case "mlModel":
          return MLModelUrn.createFromUrn(urn).getPlatformEntity();
        case "mlFeatureTable":
          return DataPlatformUrn.createFromUrn(
              ((MLFeatureTableKey)
                      EntityKeyUtils.convertUrnToEntityKeyInternal(
                          urn, MLFeatureTableKey.dataSchema()))
                  .getPlatform());
        case "mlModelDeployment":
          return DataPlatformUrn.createFromUrn(
              ((MLModelDeploymentKey)
                      EntityKeyUtils.convertUrnToEntityKeyInternal(
                          urn, MLModelDeploymentKey.dataSchema()))
                  .getPlatform());
        case "mlModelGroup":
          return DataPlatformUrn.createFromUrn(
              ((MLModelGroupKey)
                      EntityKeyUtils.convertUrnToEntityKeyInternal(
                          urn, MLModelGroupKey.dataSchema()))
                  .getPlatform());
        default:
          log.error(
              String.format(
                  "Failed to generate default platform for unknown entity type %s",
                  urn.getEntityType()));
          throw new IllegalArgumentException(
              String.format("Unable to convert urn to platform: %s", urn));
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Unable to convert urn to platform: %s", urn), e);
    }
  }

  public static Optional<DataPlatformInstance> buildDataPlatformInstance(
      String entityType, RecordTemplate keyAspect) {
    try {
      return Optional.ofNullable(getDefaultDataPlatform(entityType, keyAspect))
          .map(platform -> new DataPlatformInstance().setPlatform(platform));
    } catch (URISyntaxException e) {
      log.error(
          "Failed to generate data platform instance for entity {}, keyAspect {}",
          entityType,
          keyAspect);
      return Optional.empty();
    }
  }
}
