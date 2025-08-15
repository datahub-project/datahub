package com.linkedin.metadata.aspect.patch.template.dashboard;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.ChartUrnArray;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DashboardInfoTemplate implements ArrayMergingTemplate<DashboardInfo> {

  private static final String CHART_EDGES_FIELD_NAME = "chartEdges";
  private static final String DATASET_EDGES_FIELD_NAME = "datasetEdges";
  private static final String DATASETS_FIELD_NAME = "datasets";
  private static final String CHARTS_FIELD_NAME = "charts";
  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";
  private static final String DASHBOARDS_FIELD_NAME = "dashboards";

  @Override
  public DashboardInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DashboardInfo) {
      return (DashboardInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataJobInputOutput");
  }

  @Override
  public Class<DashboardInfo> getTemplateType() {
    return DashboardInfo.class;
  }

  @Nonnull
  @Override
  public DashboardInfo getDefault() {
    DashboardInfo dashboardInfo = new DashboardInfo();
    dashboardInfo.setTitle("");
    dashboardInfo.setDescription("");
    ChangeAuditStamps changeAuditStamps = new ChangeAuditStamps();
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    changeAuditStamps.setCreated(auditStamp).setLastModified(auditStamp);
    dashboardInfo.setLastModified(changeAuditStamps);
    dashboardInfo.setChartEdges(new EdgeArray());
    dashboardInfo.setDatasetEdges(new EdgeArray());

    // Deprecated fields
    dashboardInfo.setDatasets(new UrnArray());
    dashboardInfo.setCharts(new ChartUrnArray());

    return dashboardInfo;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            CHART_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(
            transformedNode,
            DASHBOARDS_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode =
        arrayFieldToMap(transformedNode, DATASETS_FIELD_NAME, Collections.emptyList());

    transformedNode = arrayFieldToMap(transformedNode, CHARTS_FIELD_NAME, Collections.emptyList());

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            DATASET_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            CHART_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            DASHBOARDS_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode = transformedMapToArray(rebasedNode, DATASETS_FIELD_NAME, Collections.emptyList());
    rebasedNode = transformedMapToArray(rebasedNode, CHARTS_FIELD_NAME, Collections.emptyList());

    return rebasedNode;
  }
}
