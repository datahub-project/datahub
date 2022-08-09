package com.linkedin.metadata.search.utils;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.key.NotebookKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BrowsePathUtils {

  private static final Character DEFAULT_BROWSE_PATH_DELIMITER = '.';

  private BrowsePathUtils() {
    //not called
  }

  public static BrowsePaths buildBrowsePath(Urn urn, EntityRegistry registry, EntityService service) throws URISyntaxException {
    String defaultBrowsePath = getDefaultBrowsePath(urn, registry);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  public static String getDefaultBrowsePath(Urn urn, EntityRegistry entityRegistry, EntityService service) throws URISyntaxException {

    String dataPlatformDelimiter = getDataPlatformDelimiter(urn, service);

    switch (urn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        DatasetKey dsKey = (DatasetKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataPlatformKey dpKey = (DataPlatformKey) EntityKeyUtils.convertUrnToEntityKey(
            dsKey.getPlatform(),
            getKeySchema(dsKey.getPlatform().getEntityType(),
                entityRegistry));
        return ("/" + dsKey.getOrigin() + "/" + dpKey.getPlatformName() + "/"
            + dsKey.getName()).replace('.', '/').toLowerCase();
      case Constants.CHART_ENTITY_NAME:
        ChartKey chartKey = (ChartKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + chartKey.getDashboardTool() + "/"  + chartKey.getChartId()).toLowerCase();
      case Constants.DASHBOARD_ENTITY_NAME:
        DashboardKey dashboardKey = (DashboardKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dashboardKey.getDashboardTool() + "/"  + dashboardKey.getDashboardId()).toLowerCase();
      case Constants.DATA_FLOW_ENTITY_NAME:
        DataFlowKey dataFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dataFlowKey.getOrchestrator() + "/" + dataFlowKey.getCluster() + "/" + dataFlowKey.getFlowId())
            .toLowerCase();
      case Constants.DATA_JOB_ENTITY_NAME:
        DataJobKey dataJobKey = (DataJobKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataFlowKey parentFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(dataJobKey.getFlow(),
            getKeySchema(dataJobKey.getFlow().getEntityType(), entityRegistry));
        return ("/" + parentFlowKey.getOrchestrator() + "/" + parentFlowKey.getFlowId() + "/"
            + dataJobKey.getJobId()).toLowerCase();
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        // TODO: Is this the best way to represent glossary term key?
        GlossaryTermKey glossaryTermKey = (GlossaryTermKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return "/" + glossaryTermKey.getName().replace('.', '/').toLowerCase();
      default:
        return "";
    }
  }

  /**
   * Returns a delimiter on which the name of an asset may be split.
   */
  private String getDataPlatformDelimiter(Urn urn, EntityRegistry entityRegistry, EntityService service) {
    // Attempt to construct the appropriate Data Platform URN
    Urn dataPlatformUrn = buildDataPlatformUrn(urn, entityRegistry);
    if (dataPlatformUrn != null) {
      // Attempt to resolve the delimiter from Data Platform Info
      DataPlatformInfo dataPlatformInfo = getDataPlatformInfo(dataPlatformUrn, service);
      if (dataPlatformInfo != null) {
        return dataPlatformInfo.getDatasetNameDelimiter();
      }
    }
    // Else, fallback to a default delimiter if one cannot be resolved.
    return DEFAULT_BROWSE_PATH_DELIMITER.toString();
  }

  @Nullable
  private Urn buildDataPlatformUrn(Urn urn, EntityRegistry entityRegistry) {
    switch (urn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        DatasetKey dsKey = (DatasetKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return dsKey.getPlatform();
      case Constants.CHART_ENTITY_NAME:
        ChartKey chartKey = (ChartKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return UrnUtils.getUrn(String.format("urn:li:%s:%s", Constants.DATA_PLATFORM_ENTITY_NAME, chartKey.getDashboardTool()));
      case Constants.DASHBOARD_ENTITY_NAME:
        DashboardKey dashboardKey = (DashboardKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return UrnUtils.getUrn(String.format("urn:li:%s:%s", Constants.DATA_PLATFORM_ENTITY_NAME, dashboardKey.getDashboardTool()));
      case Constants.DATA_FLOW_ENTITY_NAME:
        DataFlowKey dataFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return UrnUtils.getUrn(String.format("urn:li:%s:%s", Constants.DATA_PLATFORM_ENTITY_NAME, dataFlowKey.getOrchestrator()));
      case Constants.DATA_JOB_ENTITY_NAME:
        DataJobKey dataJobKey = (DataJobKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataFlowKey parentFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(dataJobKey.getFlow(),
            getKeySchema(dataJobKey.getFlow().getEntityType(), entityRegistry));
        return UrnUtils.getUrn(String.format("urn:li:%s:%s", Constants.DATA_PLATFORM_ENTITY_NAME, parentFlowKey.getOrchestrator()));
      case Constants.NOTEBOOK_ENTITY_NAME:
        NotebookKey notebookKey = (NotebookKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return UrnUtils.getUrn(String.format("urn:li:%s:%s", Constants.DATA_PLATFORM_ENTITY_NAME, notebookKey.getNotebookTool()));
      default:
        // Could not resolve a data platform
        return null;
    }
  }

  @Nullable
  private DataPlatformInfo getDataPlatformInfo(Urn urn, EntityService entityService) {
    try {
      final EntityResponse entityResponse = entityService.getEntityV2(
          Constants.DATA_PLATFORM_ENTITY_NAME,
          urn,
          ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)
      );
      if (entityResponse.hasAspects() && entityResponse.getAspects().containsKey(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)) {
        return new DataPlatformInfo(entityResponse.getAspects().get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME).getValue().data());
      }
    } catch (Exception e) {
      log.warn(String.format("Failed to find Data Platform Info for urn %s", urn));
    }
    return null;
  }

  protected static RecordDataSchema getKeySchema(
      final String entityName,
      final EntityRegistry registry) {
    final EntitySpec spec = registry.getEntitySpec(entityName);
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getPegasusSchema();
  }
}
