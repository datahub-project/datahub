/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.chart.ChartInfoTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ChartInfoTemplateTest {

  @Test
  public void testChartInfoTemplate() throws Exception {
    ChartInfoTemplate chartInfoTemplate = new ChartInfoTemplate();
    ChartInfo dashboardInfo = chartInfoTemplate.getDefault();
    JsonPatchBuilder patchOperations = Json.createPatchBuilder();

    JsonObjectBuilder edgeNode = Json.createObjectBuilder();
    edgeNode.add(
        "destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");

    patchOperations.add(
        "/inputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        edgeNode.build());
    ChartInfo result = chartInfoTemplate.applyPatch(dashboardInfo, patchOperations.build());

    Assert.assertEquals(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        result.getInputEdges().get(0).getDestinationUrn());
  }
}
