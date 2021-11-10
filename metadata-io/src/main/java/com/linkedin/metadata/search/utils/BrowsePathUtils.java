package com.linkedin.metadata.search.utils;

import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BrowsePathUtils {
  private BrowsePathUtils() {
    //not called
  }

  public static BrowsePaths buildBrowsePath(Urn urn, EntityRegistry registry) throws URISyntaxException {
    String defaultBrowsePath = getDefaultBrowsePath(urn, registry);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  public static String getDefaultBrowsePath(Urn urn, EntityRegistry entityRegistry) throws URISyntaxException {
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

  protected static RecordDataSchema getKeySchema(
      final String entityName,
      final EntityRegistry registry) {
    final EntitySpec spec = registry.getEntitySpec(entityName);
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getPegasusSchema();
  }
}
