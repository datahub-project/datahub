package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.mxe.MetadataChangeProposal;
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
import java.util.Collections;
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
 * Regression test against OpenLineage events captured from a Microsoft Fabric notebook (Fabric
 * Runtime 1.3, Spark 3.5, the runtime's bundled openlineage-spark_2.12 1.26.0, file transport).
 *
 * <p>The notebook reads {@code bronze_lh} (schemas disabled: {@code Tables/<table>}) and writes
 * {@code silver_lh} (schemas enabled: {@code Tables/dbo/<table>}): a {@code saveAsTable} overwrite
 * of {@code customers}, a {@code MERGE INTO} of {@code customers}, and a {@code CREATE OR REPLACE
 * TABLE ... AS SELECT} of {@code customer_totals}. Workspace, item and session identifiers and the
 * workspace name were replaced consistently with fake values; the event shapes are unchanged.
 *
 * <p>Events are converted one by one with the GMS OpenLineage endpoint's defaults (as {@code
 * RunEventMapper} does), and the dataset URNs must equal what the fabric-onelake source ingests for
 * these tables.
 */
public class FabricOneLakeRuntimeEventsTest {

  private static final String FIXTURE = "/ol_events/fabric_runtime_notebook_events.jsonl";

  private static final String WS = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
  private static final String BRONZE = "11112222-3333-4444-5555-666677778888";
  private static final String SILVER = "aaaabbbb-cccc-dddd-eeee-ffff00001111";
  private static final String NOTEBOOK = "22223333-4444-5555-6666-777788889999";
  private static final String FLOW_NAME =
      "nb_bronze_to_silver_00000000_1111_4222_8333_444444444444";

  // What the fabric-onelake source ingests for these tables (convert_urns_to_lowercase=false).
  private static final String BRONZE_CUSTOMERS = oneLake(BRONZE, "customers");
  private static final String BRONZE_ORDERS = oneLake(BRONZE, "orders");
  private static final String SILVER_CUSTOMERS = oneLake(SILVER, "customers");
  private static final String SILVER_CUSTOMER_TOTALS = oneLake(SILVER, "customer_totals");

  // Previous behaviour (mapping disabled): the ABFS location on the abs platform.
  private static final String ABS_BRONZE_CUSTOMERS = abs(BRONZE + "/Tables/customers");
  private static final String ABS_BRONZE_ORDERS = abs(BRONZE + "/Tables/orders");
  private static final String ABS_SILVER_CUSTOMERS = abs(SILVER + "/Tables/dbo/customers");
  private static final String ABS_SILVER_CUSTOMER_TOTALS =
      abs(SILVER + "/Tables/dbo/customer_totals");

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

  private static String abs(String path) {
    return "urn:li:dataset:(urn:li:dataPlatform:abs,"
        + WS
        + "@onelake.dfs.fabric.microsoft.com/"
        + path
        + ",PROD)";
  }

  private static String field(String datasetUrn, String field) {
    return "urn:li:schemaField:(" + datasetUrn + "," + field + ")";
  }

  /** The GMS OpenLineage endpoint's defaults (DatahubOpenlineageProperties). */
  private static DatahubOpenlineageConfig gmsConfig(boolean fabricOneLakeEnabled) {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .captureColumnLevelLineage(true)
        .usePatch(false)
        .fabricOneLakeEnabled(fabricOneLakeEnabled)
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
    assertEquals(events.size(), 12);
  }

  private static Set<String> urns(Set<DatahubDataset> datasets) {
    return datasets.stream().map(d -> d.getUrn().toString()).collect(Collectors.toSet());
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  /** Suffix of the job name after the flow name, e.g. {@code execute_merge_into_command...}. */
  private static String jobSuffix(OpenLineage.RunEvent event) {
    String name = event.getJob().getName();
    return name.equals(FLOW_NAME) ? "" : name.substring(FLOW_NAME.length() + 1);
  }

  private record Expected(String jobSuffix, Set<String> inputs, Set<String> outputs) {}

  private static List<Expected> expected(
      String bronzeCustomers, String bronzeOrders, String silverCustomers, String totals) {
    String ctas =
        "atomic_replace_table_as_select.chimcobldhq2al35edq20lrfe9ln6s31cdiiasr9dhr6asivdhk2ap32ds_";
    return List.of(
        new Expected("collect_limit", set(bronzeCustomers), set()),
        new Expected("append_data_exec_v1.spark_catalog_customers", set(bronzeCustomers), set()),
        new Expected("columnar_to_row", set(bronzeCustomers), set()),
        // saveAsTable(mode=overwrite)
        new Expected(ctas + "customers", set(bronzeCustomers), set(silverCustomers)),
        // MERGE: join scan of source and target
        new Expected("adaptive_spark_plan", set(bronzeCustomers, silverCustomers), set()),
        new Expected("project", set(silverCustomers), set()),
        // MERGE: the producer reports no inputs, only column lineage on the output
        new Expected("execute_merge_into_command.customers", set(), set(silverCustomers)),
        new Expected("command_result", set(), set()),
        new Expected("append_data_exec_v1.spark_catalog_customer_totals", set(bronzeOrders), set()),
        // CREATE OR REPLACE TABLE ... AS SELECT
        new Expected(ctas + "customer_totals", set(bronzeOrders), set(totals)),
        new Expected("command_result", set(), set()),
        // application end
        new Expected("", set(), set()));
  }

  @Test
  public void testEventsMapToFabricOneLakeConnectorUrns() throws Exception {
    DatahubOpenlineageConfig config = gmsConfig(true);
    List<Expected> expected =
        expected(BRONZE_CUSTOMERS, BRONZE_ORDERS, SILVER_CUSTOMERS, SILVER_CUSTOMER_TOTALS);
    for (int i = 0; i < events.size(); i++) {
      OpenLineage.RunEvent event = events.get(i);
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config);
      assertEquals(jobSuffix(event), expected.get(i).jobSuffix(), "event " + i);
      assertEquals(urns(job.getInSet()), expected.get(i).inputs(), "inputs of event " + i);
      assertEquals(urns(job.getOutSet()), expected.get(i).outputs(), "outputs of event " + i);

      // The fabric-onelake source owns these entities' schema; the event's (read) schema must not
      // replace it. The dataset key/status aspects are still materialized.
      for (MetadataChangeProposal mcp : job.toMcps(config)) {
        if (Objects.requireNonNull(mcp.getEntityUrn()).toString().contains("fabric-onelake")) {
          assertNotEquals(
              mcp.getAspectName(),
              "schemaMetadata",
              "event " + i + " emitted schemaMetadata for " + mcp.getEntityUrn());
        }
      }
    }
  }

  @Test
  public void testMergeColumnLineage() throws Exception {
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(6), gmsConfig(true));
    DatahubDataset output = job.getOutSet().iterator().next();
    assertEquals(output.getUrn().toString(), SILVER_CUSTOMERS);

    Map<String, List<String>> fgl = new TreeMap<>();
    for (FineGrainedLineage entry :
        Objects.requireNonNull(output.getLineage().getFineGrainedLineages())) {
      assertEquals(Objects.requireNonNull(entry.getDownstreams()).size(), 1);
      fgl.put(
          entry.getDownstreams().get(0).toString(),
          Objects.requireNonNull(entry.getUpstreams()).stream()
              .map(Urn::toString)
              .collect(Collectors.toList()));
    }
    Map<String, List<String>> expected = new TreeMap<>();
    // The ON clause makes the target's own key an input of CustomerID.
    expected.put(
        field(SILVER_CUSTOMERS, "CustomerID"),
        List.of(field(BRONZE_CUSTOMERS, "CustomerID"), field(SILVER_CUSTOMERS, "CustomerID")));
    expected.put(
        field(SILVER_CUSTOMERS, "CustomerName"), List.of(field(BRONZE_CUSTOMERS, "CustomerName")));
    expected.put(field(SILVER_CUSTOMERS, "Region"), List.of(field(BRONZE_CUSTOMERS, "Region")));
    expected.put(
        field(SILVER_CUSTOMERS, "SignupDate"), List.of(field(BRONZE_CUSTOMERS, "SignupDate")));
    assertEquals(fgl, expected);

    // Dataset-level upstreams derived from the column lineage.
    assertEquals(
        output.getLineage().getUpstreams().stream()
            .map(u -> u.getDataset().toString())
            .collect(Collectors.toSet()),
        set(BRONZE_CUSTOMERS, SILVER_CUSTOMERS));

    // The job carries the column lineage (and the MERGE statement) even without input edges.
    MetadataChangeProposal inputOutput =
        job.toMcps(gmsConfig(true)).stream()
            .filter(mcp -> "dataJobInputOutput".equals(mcp.getAspectName()))
            .findFirst()
            .orElseThrow();
    String json = inputOutput.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(json.contains(field(BRONZE_CUSTOMERS, "CustomerName")), json);
    assertTrue(json.contains("MERGE INTO silver_lh.dbo.customers"), json);
  }

  @Test
  public void testMergeColumnLineageFollowsConnectorLowercasing() throws Exception {
    // The Fabric OneLake source's convert_urns_to_lowercase lowercases column names (field paths)
    // too, so column-level lineage must use lowercased fields to attach to the ingested schema.
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .captureColumnLevelLineage(true)
            .fabricOneLakeEnabled(true)
            .fabricOneLakeConvertUrnsToLowercase(true)
            .build();
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(6), config);
    DatahubDataset output = job.getOutSet().iterator().next();
    assertEquals(output.getUrn().toString(), SILVER_CUSTOMERS);

    Map<String, List<String>> fgl = new TreeMap<>();
    for (FineGrainedLineage entry :
        Objects.requireNonNull(output.getLineage().getFineGrainedLineages())) {
      fgl.put(
          Objects.requireNonNull(entry.getDownstreams()).get(0).toString(),
          Objects.requireNonNull(entry.getUpstreams()).stream()
              .map(Urn::toString)
              .collect(Collectors.toList()));
    }
    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(
        field(SILVER_CUSTOMERS, "customerid"),
        List.of(field(BRONZE_CUSTOMERS, "customerid"), field(SILVER_CUSTOMERS, "customerid")));
    expected.put(
        field(SILVER_CUSTOMERS, "customername"), List.of(field(BRONZE_CUSTOMERS, "customername")));
    expected.put(field(SILVER_CUSTOMERS, "region"), List.of(field(BRONZE_CUSTOMERS, "region")));
    expected.put(
        field(SILVER_CUSTOMERS, "signupdate"), List.of(field(BRONZE_CUSTOMERS, "signupdate")));
    assertEquals(fgl, expected);
  }

  @Test
  public void testCreateTableAsSelectCarriesNoColumnLineage() throws Exception {
    // openlineage-spark 1.26.0 on Fabric emits no columnLineage facet for the
    // atomic_replace_table_as_select events (saveAsTable overwrite and CREATE OR REPLACE TABLE AS
    // SELECT), so only table-level lineage is available for them.
    for (int i : new int[] {3, 9}) {
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(i), gmsConfig(true));
      assertNull(job.getOutSet().iterator().next().getLineage(), "event " + i);
    }
  }

  @Test
  public void testJobNamingAndFabricRunProperties() throws Exception {
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(6), gmsConfig(true));
    // The flow name is the snake-cased spark.app.name, which embeds a per-session id.
    assertEquals(
        job.getFlowUrn().toString(),
        "urn:li:dataFlow:(spark," + FLOW_NAME + ",fabric-test-workspace)");
    assertEquals(
        job.getJobUrn().toString(),
        "urn:li:dataJob:("
            + job.getFlowUrn()
            + ","
            + FLOW_NAME
            + ".execute_merge_into_command.customers)");
    // The Fabric notebook item is only identified through the captured Spark properties.
    Map<String, String> props = job.getJobInfo().getCustomProperties();
    assertEquals(props.get("trident.artifact.id"), NOTEBOOK);
    assertEquals(props.get("trident.workspace.id"), WS);
    assertEquals(props.get("spark.synapse.context.notebookname"), "nb_bronze_to_silver");
    assertEquals(props.get("openlineageAdapterVersion"), "1.26.0");
  }

  @Test
  public void testMappingDisabledKeepsAbsUrns() throws Exception {
    DatahubOpenlineageConfig config = gmsConfig(false);
    List<Expected> expected =
        expected(
            ABS_BRONZE_CUSTOMERS,
            ABS_BRONZE_ORDERS,
            ABS_SILVER_CUSTOMERS,
            ABS_SILVER_CUSTOMER_TOTALS);
    boolean schemaEmitted = false;
    for (int i = 0; i < events.size(); i++) {
      DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(events.get(i), config);
      assertEquals(urns(job.getInSet()), expected.get(i).inputs(), "inputs of event " + i);
      assertEquals(urns(job.getOutSet()), expected.get(i).outputs(), "outputs of event " + i);
      schemaEmitted |=
          job.toMcps(config).stream().anyMatch(mcp -> "schemaMetadata".equals(mcp.getAspectName()));
    }
    // abs datasets are OpenLineage-owned, so their schema is still emitted.
    assertTrue(schemaEmitted);

    DatahubJob merge = OpenLineageToDataHub.convertRunEventToJob(events.get(6), config);
    List<String> upstreams =
        Objects.requireNonNull(
                merge.getOutSet().iterator().next().getLineage().getFineGrainedLineages())
            .stream()
            .flatMap(f -> Objects.requireNonNull(f.getUpstreams()).stream())
            .map(Urn::toString)
            .collect(Collectors.toList());
    assertTrue(upstreams.contains(field(ABS_BRONZE_CUSTOMERS, "Region")), upstreams.toString());
    assertTrue(Collections.disjoint(upstreams, List.of(field(BRONZE_CUSTOMERS, "Region"))));
  }
}
