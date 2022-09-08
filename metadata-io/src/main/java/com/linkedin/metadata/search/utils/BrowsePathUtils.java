package com.linkedin.metadata.search.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.Constants;
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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BrowsePathUtils {

  public static String getDefaultBrowsePath(
      @Nonnull Urn urn,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Character dataPlatformDelimiter) throws URISyntaxException {

    switch (urn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        DatasetKey dsKey = (DatasetKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataPlatformKey dpKey = (DataPlatformKey) EntityKeyUtils.convertUrnToEntityKey(
            dsKey.getPlatform(),
            getKeySchema(dsKey.getPlatform().getEntityType(),
                entityRegistry));
        String datasetNamePath = getDatasetPath(dsKey.getName(), dataPlatformDelimiter);
        return ("/" + dsKey.getOrigin() + "/" + dpKey.getPlatformName() + datasetNamePath).toLowerCase();
      case Constants.CHART_ENTITY_NAME:
        ChartKey chartKey = (ChartKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + chartKey.getDashboardTool());
      case Constants.DASHBOARD_ENTITY_NAME: // TODO -> Improve the quality of our browse path here.
        DashboardKey dashboardKey = (DashboardKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dashboardKey.getDashboardTool()).toLowerCase();
      case Constants.DATA_FLOW_ENTITY_NAME: // TODO -> Improve the quality of our browse path here.
        DataFlowKey dataFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dataFlowKey.getOrchestrator() + "/" + dataFlowKey.getCluster())
            .toLowerCase();
      case Constants.DATA_JOB_ENTITY_NAME: // TODO -> Improve the quality of our browse path here.
        DataJobKey dataJobKey = (DataJobKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataFlowKey parentFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(dataJobKey.getFlow(),
            getKeySchema(dataJobKey.getFlow().getEntityType(), entityRegistry));
        return ("/" + parentFlowKey.getOrchestrator() + "/" + parentFlowKey.getCluster()).toLowerCase();
      default:
        return "";
    }
  }

  @Nullable
  public static Urn buildDataPlatformUrn(Urn urn, EntityRegistry entityRegistry) {
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

  public static String getLegacyDefaultBrowsePath(Urn urn, EntityRegistry entityRegistry) throws URISyntaxException {
    switch (urn.getEntityType()) {
      case "dataset":
        DatasetKey dsKey = (DatasetKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataPlatformKey dpKey = (DataPlatformKey) EntityKeyUtils.convertUrnToEntityKey(
            dsKey.getPlatform(),
            getKeySchema(dsKey.getPlatform().getEntityType(),
                entityRegistry));
        return ("/" + dsKey.getOrigin() + "/" + dpKey.getPlatformName() + "/"
            + dsKey.getName()).replace('.', '/').toLowerCase();
      case "chart":
        ChartKey chartKey = (ChartKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + chartKey.getDashboardTool() + "/"  + chartKey.getChartId()).toLowerCase();
      case "dashboard":
        DashboardKey dashboardKey = (DashboardKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dashboardKey.getDashboardTool() + "/"  + dashboardKey.getDashboardId()).toLowerCase();
      case "dataFlow":
        DataFlowKey dataFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return ("/" + dataFlowKey.getOrchestrator() + "/" + dataFlowKey.getCluster() + "/" + dataFlowKey.getFlowId())
            .toLowerCase();
      case "dataJob":
        DataJobKey dataJobKey = (DataJobKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        DataFlowKey parentFlowKey = (DataFlowKey) EntityKeyUtils.convertUrnToEntityKey(dataJobKey.getFlow(),
            getKeySchema(dataJobKey.getFlow().getEntityType(), entityRegistry));
        return ("/" + parentFlowKey.getOrchestrator() + "/" + parentFlowKey.getFlowId() + "/"
            + dataJobKey.getJobId()).toLowerCase();
      case "glossaryTerm":
        // TODO: Is this the best way to represent glossary term key?
        GlossaryTermKey glossaryTermKey = (GlossaryTermKey) EntityKeyUtils.convertUrnToEntityKey(urn, getKeySchema(urn.getEntityType(), entityRegistry));
        return "/" + glossaryTermKey.getName().replace('.', '/').toLowerCase();
      default:
        return "";
    }
  }

  /**
   * Attempts to convert a dataset name into a proper browse path by splitting it using the Data Platform delimiter.
   * If there are not > 1 name parts, then an empty string will be returned.
   */
  private static String getDatasetPath(@Nonnull final String datasetName, @Nonnull final Character delimiter) {
    if (datasetName.contains(delimiter.toString())) {
      final List<String> datasetNamePathParts = Arrays.asList(datasetName.split(Pattern.quote(delimiter.toString())));
      System.out.println(datasetNamePathParts);
      // Omit the name from the path.
      final String datasetPath = String.join("/", datasetNamePathParts.subList(0, datasetNamePathParts.size() - 1));
      return datasetPath.startsWith("/") ? datasetPath : String.format("/%s", datasetPath);
    }
    return "";
  }

  protected static RecordDataSchema getKeySchema(
      final String entityName,
      final EntityRegistry registry) {
    final EntitySpec spec = registry.getEntitySpec(entityName);
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getPegasusSchema();
  }

  private BrowsePathUtils() { }
}
