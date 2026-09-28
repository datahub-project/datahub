package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.FineGrainedLineage;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Regression test against OpenLineage events captured from a Microsoft Fabric notebook on Fabric
 * Runtime 2.0 (Spark 4.1, the runtime's bundled openlineage-spark_2.13 1.40.1, which Fabric pins to
 * the file transport).
 *
 * <p>The notebook reads {@code bronze_lh} and writes {@code silver_lh} (both schemas enabled): a
 * {@code MERGE INTO} of {@code customers}, a {@code CREATE OR REPLACE TABLE ... AS SELECT} of
 * {@code customer_totals}, a DataFrame join + {@code saveAsTable} of {@code region_totals} and an
 * {@code INSERT OVERWRITE} of {@code customer_totals}. Unlike Runtime 1.3 (1.26), every write
 * carries a column-lineage facet and the MERGE event has table-level inputs. Workspace, item,
 * session and storage identifiers were replaced consistently with fake values; the event shapes are
 * unchanged.
 */
public class FabricRuntime2EventsTest {

  private static final String FIXTURE = "/ol_events/fabric_runtime2_notebook_events.jsonl";

  private static final String WS = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
  private static final String BRONZE = "11112222-3333-4444-5555-666677778888";
  private static final String SILVER = "aaaabbbb-cccc-dddd-eeee-ffff00001111";
  private static final String NOTEBOOK = "33334444-5555-6666-7777-888899990000";
  private static final String SESSION_FLOW =
      "nb_bronze_to_silver_00000000_1111_4222_8333_555555555555";

  private static final String BRONZE_CUSTOMERS = oneLake(BRONZE, "customers");
  private static final String BRONZE_ORDERS = oneLake(BRONZE, "orders");
  private static final String SILVER_CUSTOMERS = oneLake(SILVER, "customers");
  private static final String CUSTOMER_TOTALS = oneLake(SILVER, "customer_totals");
  private static final String REGION_TOTALS = oneLake(SILVER, "region_totals");

  private List<OpenLineage.RunEvent> events;

  private static String oneLake(String item, String table) {
    return "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
        + WS
        + "."
        + item
        + ".dbo."
        + table
        + ",PROD)";
  }

  private static String field(String datasetUrn, String field) {
    return "urn:li:schemaField:(" + datasetUrn + "," + field + ")";
  }

  private static DatahubOpenlineageConfig config(boolean notebookFlowNames) {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .captureColumnLevelLineage(true)
        .fabricOneLakeEnabled(true)
        .fabricNotebookFlowNames(notebookFlowNames)
        .build();
  }

  @BeforeClass
  public void loadEvents() throws Exception {
    events = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Objects.requireNonNull(getClass().getResourceAsStream(FIXTURE)),
                StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isBlank()) {
          events.add(OpenLineageClientUtils.runEventFromJson(line));
        }
      }
    }
    assertEquals(events.size(), 9);
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  /** Downstream field -> upstream fields of the event's only output. */
  private static Map<String, Set<String>> columnLineage(DatahubJob job) {
    DatahubDataset output = job.getOutSet().iterator().next();
    Map<String, Set<String>> fgl = new TreeMap<>();
    for (FineGrainedLineage entry :
        Objects.requireNonNull(output.getLineage().getFineGrainedLineages())) {
      assertEquals(Objects.requireNonNull(entry.getDownstreams()).size(), 1);
      fgl.put(
          entry.getDownstreams().get(0).toString(),
          Objects.requireNonNull(entry.getUpstreams()).stream()
              .map(Urn::toString)
              .collect(Collectors.toSet()));
    }
    return fgl;
  }

  @Test
  public void testEveryWriteStatementCarriesColumnLineage() throws Exception {
    DatahubOpenlineageConfig config = config(false);

    // MERGE INTO (the ON key lists the target column as its own upstream, as the producer does)
    Map<String, Set<String>> merge =
        columnLineage(OpenLineageToDataHub.convertRunEventToJob(events.get(0), config));
    Map<String, Set<String>> expectedMerge = new TreeMap<>();
    expectedMerge.put(
        field(SILVER_CUSTOMERS, "CustomerID"),
        set(field(BRONZE_CUSTOMERS, "CustomerID"), field(SILVER_CUSTOMERS, "CustomerID")));
    for (String column : List.of("CustomerName", "Region", "SignupDate")) {
      expectedMerge.put(field(SILVER_CUSTOMERS, column), set(field(BRONZE_CUSTOMERS, column)));
    }
    assertEquals(merge, expectedMerge);

    // CREATE OR REPLACE TABLE ... AS SELECT and INSERT OVERWRITE (overwrite_by_expression_exec_v1)
    Map<String, Set<String>> totals = new TreeMap<>();
    totals.put(field(CUSTOMER_TOTALS, "customer_id"), set(field(BRONZE_ORDERS, "CustomerID")));
    totals.put(field(CUSTOMER_TOTALS, "total_amount"), set(field(BRONZE_ORDERS, "order_amount")));
    for (int i : new int[] {2, 7}) {
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(i), config);
      assertTrue(
          events.get(i).getJob().getName().contains(".overwrite_by_expression_exec_v1."), "" + i);
      assertEquals(columnLineage(job), totals, "event " + i);
    }

    // DataFrame join + groupBy + saveAsTable
    Map<String, Set<String>> region = new TreeMap<>();
    region.put(field(REGION_TOTALS, "region"), set(field(BRONZE_CUSTOMERS, "Region")));
    region.put(field(REGION_TOTALS, "region_amount"), set(field(BRONZE_ORDERS, "order_amount")));
    region.put(field(REGION_TOTALS, "customer_count"), set(field(BRONZE_ORDERS, "CustomerID")));
    assertEquals(
        columnLineage(OpenLineageToDataHub.convertRunEventToJob(events.get(5), config)), region);

    // The atomic_replace_table_as_select events repeat the table-level lineage only.
    for (int i : new int[] {3, 6}) {
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(i), config);
      assertTrue(events.get(i).getJob().getName().contains(".atomic_replace_table_as_select."));
      assertNull(job.getOutSet().iterator().next().getLineage(), "event " + i);
    }
  }

  @Test
  public void testMergeHasTableLevelInputs() throws Exception {
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(0), config(false));
    assertEquals(
        job.getInSet().stream().map(d -> d.getUrn().toString()).collect(Collectors.toSet()),
        set(BRONZE_CUSTOMERS, SILVER_CUSTOMERS));
    assertEquals(job.getOutSet().iterator().next().getUrn().toString(), SILVER_CUSTOMERS);
  }

  @Test
  public void testNotebookFlowNames() throws Exception {
    String flow = "urn:li:dataFlow:(spark," + NOTEBOOK + ",default)";
    for (OpenLineage.RunEvent event : events) {
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config(true));
      assertEquals(job.getFlowUrn().toString(), flow);
      assertEquals(job.getDataFlowInfo().getName(), "nb_bronze_to_silver");
      String suffix = event.getJob().getName().substring(SESSION_FLOW.length() + 1);
      assertEquals(job.getJobUrn().toString(), "urn:li:dataJob:(" + flow + "," + suffix + ")");
    }

    // Off by default: one DataFlow per Spark session, job names keep the session prefix.
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(0), config(false));
    assertEquals(
        job.getFlowUrn().toString(), "urn:li:dataFlow:(spark," + SESSION_FLOW + ",default)");
    assertEquals(
        job.getJobUrn().toString(),
        "urn:li:dataJob:("
            + job.getFlowUrn()
            + ","
            + SESSION_FLOW
            + ".execute_merge_into_command.customers)");
  }

  @Test
  public void testConfiguredPipelineNameWinsOverNotebookName() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .fabricOneLakeEnabled(true)
            .fabricNotebookFlowNames(true)
            .pipelineName("nightly_silver")
            .build();
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(0), config);
    assertEquals(job.getFlowUrn().toString(), "urn:li:dataFlow:(spark,nightly_silver,default)");
    assertEquals(job.getDataFlowInfo().getName(), "nightly_silver");
  }
}
