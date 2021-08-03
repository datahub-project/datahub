package com.linkedin.metadata.search.utils;

import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.aspect.ChartAspectArray;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.aspect.DashboardAspectArray;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.aspect.DataFlowAspectArray;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.aspect.DataJobAspectArray;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.aspect.GlossaryTermAspect;
import com.linkedin.metadata.aspect.GlossaryTermAspectArray;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import javax.annotation.Nullable;
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
        return ("/" + dsKey.getOrigin() + "/" + dsKey.getPlatform() + "/"
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
      default:
        return "";
    }
  }

  public static void addBrowsePathIfNotExists(Snapshot snapshot, @Nullable Entity browsePathEntity, EntityRegistry registry)
      throws URISyntaxException {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshot);
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final BrowsePaths defaultBrowsePaths = buildBrowsePath(urn, registry);

    if (urn.getEntityType().equals("dataset")) {
      final DatasetAspectArray aspects = snapshot.getDatasetSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final DatasetAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getDatasetSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(DatasetAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType().equals("glossaryTerm")) {
      final GlossaryTermAspectArray aspects = snapshot.getGlossaryTermSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final GlossaryTermAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getGlossaryTermSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(glossaryTermAspect -> glossaryTermAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(glossaryTermAspect -> glossaryTermAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(GlossaryTermAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType().equals("chart")) {
      final ChartAspectArray aspects = snapshot.getChartSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final ChartAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getChartSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(ChartAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType().equals("dashboard")) {
      final DashboardAspectArray aspects = snapshot.getDashboardSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final DashboardAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getDashboardSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(DashboardAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType().equals("dataJob")) {
      final DataJobAspectArray aspects = snapshot.getDataJobSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final DataJobAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getDataJobSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(DataJobAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType().equals("dataFlow")) {
      final DataFlowAspectArray aspects = snapshot.getDataFlowSnapshot().getAspects();
      boolean hasBrowse = false;
      if (browsePathEntity != null) {
        final DataFlowAspectArray aspectsWithExistingBrowse = browsePathEntity.getValue().getDataFlowSnapshot().getAspects();
        hasBrowse = aspects.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent()
            || aspectsWithExistingBrowse.stream()
            .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      }
      if (!hasBrowse) {
        aspects.add(DataFlowAspect.create(defaultBrowsePaths));
      }
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
