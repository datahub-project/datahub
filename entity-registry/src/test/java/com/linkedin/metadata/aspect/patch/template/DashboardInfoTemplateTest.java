package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.aspect.patch.template.dashboard.DashboardInfoTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatchBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardInfoTemplateTest {

  @Test
  public void testDashboardInfoTemplate() throws Exception {
    DashboardInfoTemplate dashboardInfoTemplate = new DashboardInfoTemplate();
    DashboardInfo dashboardInfo = dashboardInfoTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
    jsonPatchBuilder.add(
        "/datasetEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        Json.createObjectBuilder()
            .add(
                "destinationUrn",
                Json.createValue(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"))
            .build());

    DashboardInfo result =
        dashboardInfoTemplate.applyPatch(dashboardInfo, jsonPatchBuilder.build());

    Assert.assertEquals(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        result.getDatasetEdges().get(0).getDestinationUrn());
  }

  @Test
  public void testDashboardInfoTemplateDashboardsField() throws Exception {
    DashboardInfoTemplate dashboardInfoTemplate = new DashboardInfoTemplate();
    DashboardInfo dashboardInfo = dashboardInfoTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
    jsonPatchBuilder.add(
        "/dashboards/urn:li:dashboard:(urn:li:dataPlatform:tableau,SampleDashboard,PROD)",
        Json.createObjectBuilder()
            .add(
                "destinationUrn",
                Json.createValue(
                    "urn:li:dashboard:(urn:li:dataPlatform:tableau,SampleDashboard,PROD)"))
            .build());

    DashboardInfo result =
        dashboardInfoTemplate.applyPatch(dashboardInfo, jsonPatchBuilder.build());

    Assert.assertEquals(
        UrnUtils.getUrn("urn:li:dashboard:(urn:li:dataPlatform:tableau,SampleDashboard,PROD)"),
        result.getDashboards().get(0).getDestinationUrn());
  }
}
