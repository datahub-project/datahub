package com.linkedin.metadata.search.utils;

import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
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
import com.linkedin.metadata.builders.search.ChartIndexBuilder;
import com.linkedin.metadata.builders.search.DashboardIndexBuilder;
import com.linkedin.metadata.builders.search.DataFlowIndexBuilder;
import com.linkedin.metadata.builders.search.DataJobIndexBuilder;
import com.linkedin.metadata.builders.search.DatasetIndexBuilder;
import com.linkedin.metadata.builders.search.GlossaryTermInfoIndexBuilder;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BrowsePathUtils {
  private BrowsePathUtils() {
    //not called
  }

  public static BrowsePaths buildBrowsePath(Urn urn) throws URISyntaxException {
    String defaultBrowsePath = getDefaultBrowsePath(urn);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  public static String getDefaultBrowsePath(Urn urn) throws URISyntaxException {
    switch (urn.getEntityType()) {
      case "dataset":
        return DatasetIndexBuilder.buildBrowsePath(DatasetUrn.createFromUrn(urn));
      case "chart":
        return ChartIndexBuilder.buildBrowsePath(ChartUrn.createFromUrn(urn));
      case "dashboard":
        return DashboardIndexBuilder.buildBrowsePath(DashboardUrn.createFromUrn(urn));
      case "dataFlow":
        return DataFlowIndexBuilder.buildBrowsePath(DataFlowUrn.createFromUrn(urn));
      case "dataJob":
        return DataJobIndexBuilder.buildBrowsePath(DataJobUrn.createFromUrn(urn));
      case "glossaryTerm":
        return GlossaryTermInfoIndexBuilder.buildBrowsePath(GlossaryTermUrn.createFromUrn(urn));
      default:
        log.debug(String.format("Failed to generate default browse path for unknown entity type %s", urn.getEntityType()));
        return "";
    }
  }

  public static void addBrowsePathIfNotExists(Snapshot snapshot, @Nullable Entity browsePathEntity)
      throws URISyntaxException {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshot);
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final BrowsePaths defaultBrowsePaths = buildBrowsePath(urn);

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
}
