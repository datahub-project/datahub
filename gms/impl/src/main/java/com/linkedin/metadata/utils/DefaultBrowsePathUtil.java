package com.linkedin.metadata.utils;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataFlow;
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
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.search.indexbuilder.BrowsePathUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;


public class DefaultBrowsePathUtil {
  private DefaultBrowsePathUtil() { }

  public static void addBrowsePathIfNotExists(Snapshot snapshot) throws URISyntaxException {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshot);
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final BrowsePaths defaultBrowsePaths = BrowsePathUtils.buildBrowsePath(urn);

    if (urn.getEntityType() == "dataset") {
      final DatasetAspectArray aspects = snapshot.getDatasetSnapshot().getAspects();
      boolean hasBrowse = aspects.stream()
          .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      if (!hasBrowse) {
        aspects.add(DatasetAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType() == "chart") {
      final ChartAspectArray aspects = snapshot.getChartSnapshot().getAspects();
      boolean hasBrowse = aspects.stream()
          .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      if (!hasBrowse) {
        aspects.add(ChartAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType() == "dashboard") {
      final DashboardAspectArray aspects = snapshot.getDashboardSnapshot().getAspects();
      boolean hasBrowse = aspects.stream()
          .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      if (!hasBrowse) {
        aspects.add(DashboardAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType() == "dataJob") {
      final DataJobAspectArray aspects = snapshot.getDataJobSnapshot().getAspects();
      boolean hasBrowse = aspects.stream()
          .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      if (!hasBrowse) {
        aspects.add(DataJobAspect.create(defaultBrowsePaths));
      }
    }
    if (urn.getEntityType() == "dataFlow") {
      final DataFlowAspectArray aspects = snapshot.getDataFlowSnapshot().getAspects();
      boolean hasBrowse = aspects.stream()
          .filter(datasetAspect -> datasetAspect.isBrowsePaths()).findFirst().isPresent();
      if (!hasBrowse) {
        aspects.add(DataFlowAspect.create(defaultBrowsePaths));
      }
    }
  }
}
