package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.dataset.FineGrainedLineage;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * OpenLineage states column transformations per (output field, input field) pair. A column that
 * only appeared in a GROUP BY must not inherit the DIRECT:IDENTITY a sibling column earned, because
 * in the UI that reads as "this output value came from that column" -- which the event does not
 * say.
 */
public class ColumnLineageTransformationTest {

  /**
   * One output column fed by two inputs with different roles: {@code direct_col} produces the
   * value, {@code group_col} only groups.
   */
  private static final String EVENT =
      "{"
          + "\"eventType\":\"COMPLETE\",\"eventTime\":\"2024-01-01T10:00:00.000Z\","
          + "\"run\":{\"runId\":\"d46e465b-d358-4d32-83d4-df660ff614dd\"},"
          + "\"job\":{\"namespace\":\"ns\",\"name\":\"job\"},"
          + "\"inputs\":[{\"namespace\":\"postgres://h:5432\",\"name\":\"db.sch.src\"}],"
          + "\"outputs\":[{\"namespace\":\"postgres://h:5432\",\"name\":\"db.sch.dst\",\"facets\":{"
          + "  \"columnLineage\":{\"fields\":{\"out_col\":{\"inputFields\":["
          + "    {\"namespace\":\"postgres://h:5432\",\"name\":\"db.sch.src\",\"field\":\"direct_col\","
          + "     \"transformations\":[{\"type\":\"DIRECT\",\"subtype\":\"IDENTITY\"}]},"
          + "    {\"namespace\":\"postgres://h:5432\",\"name\":\"db.sch.src\",\"field\":\"group_col\","
          + "     \"transformations\":[{\"type\":\"INDIRECT\",\"subtype\":\"GROUP_BY\"}]}"
          + "  ]}}}"
          + "}}],"
          + "\"producer\":\"https://github.com/apache/airflow/tree/1.0.0\"}";

  private static Map<String, String> transformationByUpstreamColumn(boolean includeIndirect)
      throws Exception {
    OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(EVENT);
    List<DatahubDataset> outputs =
        OpenLineageToDataHub.convertRunEventToJob(event, config(includeIndirect))
            .getOutSet()
            .stream()
            .collect(Collectors.toList());
    assertEquals(outputs.size(), 1);
    List<FineGrainedLineage> fgls = outputs.get(0).getLineage().getFineGrainedLineages();
    return fgls.stream()
        .collect(
            Collectors.toMap(
                f -> {
                  String urn = f.getUpstreams().get(0).toString();
                  return urn.substring(urn.lastIndexOf(',') + 1, urn.length() - 1);
                },
                FineGrainedLineage::getTransformOperation));
  }

  @Test
  public void eachUpstreamColumnKeepsItsOwnTransformation() throws Exception {
    Map<String, String> byColumn = transformationByUpstreamColumn(true);

    assertEquals(byColumn.size(), 2, "one entry per (output, input) pair: " + byColumn);
    assertEquals(byColumn.get("direct_col"), "DIRECT:IDENTITY");
    assertEquals(
        byColumn.get("group_col"),
        "INDIRECT:GROUP_BY",
        "a GROUP BY column must not inherit the sibling's DIRECT:IDENTITY");
  }

  /**
   * With indirect columns excluded the group column has no entry of its own, so the only place its
   * role can still be recorded is on the pairs that survived. Losing it would hide that the query
   * grouped at all.
   */
  @Test
  public void excludedIndirectRolesAreStillRecordedOnTheSurvivingPairs() throws Exception {
    Map<String, String> byColumn = transformationByUpstreamColumn(false);

    assertEquals(byColumn.size(), 1, "the indirect-only column should be dropped: " + byColumn);
    assertTrue(
        byColumn.get("direct_col").contains("DIRECT:IDENTITY"),
        "the surviving pair keeps its own role: " + byColumn);
    assertTrue(
        byColumn.get("direct_col").contains("INDIRECT:GROUP_BY"),
        "and still records that a GROUP BY happened: " + byColumn);
  }

  private static DatahubOpenlineageConfig config(boolean includeIndirect) {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .orchestrator("airflow")
        .materializeDataset(true)
        .captureColumnLevelLineage(true)
        .includeIndirectColumnLineage(includeIndirect)
        .build();
  }
}
