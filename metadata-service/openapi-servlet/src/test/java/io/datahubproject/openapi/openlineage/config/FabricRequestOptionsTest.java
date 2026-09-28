package io.datahubproject.openapi.openlineage.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.annotations.Test;

public class FabricRequestOptionsTest {

  private static final String WS = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
  private static final String ITEM = "aaaabbbb-cccc-dddd-eeee-ffff00001111";

  private static final DatahubOpenlineageConfig BASE =
      DatahubOpenlineageConfig.builder()
          .fabricType(FabricType.PROD)
          .materializeDataset(true)
          .includeSchemaMetadata(true)
          .captureColumnLevelLineage(true)
          .build();

  private static MockHttpServletRequest request() {
    return new MockHttpServletRequest("POST", "/openapi/openlineage/api/v1/lineage");
  }

  @Test
  public void testNoOptionsKeepsTheEndpointConfig() {
    DatahubOpenlineageConfig config = FabricRequestOptions.apply(BASE, request());
    assertSame(config, BASE);
    assertFalse(config.isFabricOneLakeEnabled());
    assertFalse(config.isFabricNotebookFlowNames());
  }

  @Test
  public void testQueryParameters() {
    MockHttpServletRequest request = request();
    request.setParameter("fabricOneLake", "true");
    request.setParameter("fabricOneLakeConvertUrnsToLowercase", "TRUE");
    request.setParameter("fabricOneLakePlatformInstance", "tenant_a");
    request.setParameter("fabricOneLakeItemIds", "Sales/bronze.Lakehouse=" + WS + "/" + ITEM);
    request.setParameter("fabricNotebookFlowNames", "true");
    DatahubOpenlineageConfig config = FabricRequestOptions.apply(BASE, request);
    assertTrue(config.isFabricOneLakeEnabled());
    assertTrue(config.isFabricOneLakeConvertUrnsToLowercase());
    assertEquals(config.getFabricOneLakePlatformInstance(), "tenant_a");
    assertEquals(config.getFabricOneLakeItemIds().get("Sales/bronze.Lakehouse"), WS + "/" + ITEM);
    assertTrue(config.isFabricNotebookFlowNames());
    // The rest of the endpoint's config is kept.
    assertTrue(config.isMaterializeDataset());
    assertEquals(config.getFabricType(), FabricType.PROD);
  }

  @Test
  public void testHeaders() {
    MockHttpServletRequest request = request();
    request.addHeader("X-DataHub-Fabric-OneLake", "true");
    request.addHeader("X-DataHub-Fabric-Notebook-Flow-Names", "true");
    DatahubOpenlineageConfig config = FabricRequestOptions.apply(BASE, request);
    assertTrue(config.isFabricOneLakeEnabled());
    assertFalse(config.isFabricOneLakeConvertUrnsToLowercase());
    assertTrue(config.isFabricNotebookFlowNames());
  }

  @Test
  public void testQueryParameterWinsOverHeader() {
    MockHttpServletRequest request = request();
    request.addHeader("X-DataHub-Fabric-OneLake", "true");
    request.setParameter("fabricOneLake", "false");
    assertFalse(FabricRequestOptions.apply(BASE, request).isFabricOneLakeEnabled());
  }

  @Test
  public void testNotebookFlowNamesWithoutOneLakeMapping() {
    MockHttpServletRequest request = request();
    request.setParameter("fabricNotebookFlowNames", "true");
    DatahubOpenlineageConfig config = FabricRequestOptions.apply(BASE, request);
    assertTrue(config.isFabricNotebookFlowNames());
    assertFalse(config.isFabricOneLakeEnabled());
  }

  @Test
  public void testMalformedOptionsAreRejected() {
    MockHttpServletRequest notBoolean = request();
    notBoolean.setParameter("fabricOneLake", "yes");
    assertThrows(
        IllegalArgumentException.class, () -> FabricRequestOptions.apply(BASE, notBoolean));

    MockHttpServletRequest empty = request();
    empty.addHeader("X-DataHub-Fabric-OneLake", " ");
    assertThrows(IllegalArgumentException.class, () -> FabricRequestOptions.apply(BASE, empty));

    // Mapping options without the mapping would silently do nothing.
    MockHttpServletRequest lowercaseOnly = request();
    lowercaseOnly.setParameter("fabricOneLakeConvertUrnsToLowercase", "true");
    assertThrows(
        IllegalArgumentException.class, () -> FabricRequestOptions.apply(BASE, lowercaseOnly));

    // Every item-id entry must parse, rather than dropping the malformed ones.
    MockHttpServletRequest badItemIds = request();
    badItemIds.setParameter("fabricOneLake", "true");
    badItemIds.setParameter(
        "fabricOneLakeItemIds",
        "Sales/bronze.Lakehouse=" + WS + "/" + ITEM + ",Sales/broken.Lakehouse=not-a-guid");
    assertThrows(
        IllegalArgumentException.class, () -> FabricRequestOptions.apply(BASE, badItemIds));
  }

  @Test
  public void testOptionsReachTheMapping() throws Exception {
    OpenLineage ol =
        new OpenLineage(
            java.net.URI.create(
                "https://github.com/OpenLineage/OpenLineage/tree/1.40.1/integration/spark"));
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .eventTime(java.time.ZonedDateTime.now())
            .run(ol.newRunBuilder().runId(java.util.UUID.randomUUID()).build())
            .job(ol.newJobBuilder().namespace("default").name("nb_test.write").build())
            .inputs(java.util.List.of())
            .outputs(
                java.util.List.of(
                    ol.newOutputDatasetBuilder()
                        .namespace("abfss://" + WS + "@onelake.dfs.fabric.microsoft.com")
                        .name("/" + ITEM + "/Tables/dbo/Customers")
                        .build()))
            .build();
    event = OpenLineageClientUtils.runEventFromJson(OpenLineageClientUtils.toJson(event));

    MockHttpServletRequest request = request();
    request.setParameter("fabricOneLake", "true");
    request.setParameter("fabricOneLakeConvertUrnsToLowercase", "true");
    Set<String> urns = datasetUrns(event, FabricRequestOptions.apply(BASE, request));
    assertTrue(
        urns.contains(
            "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
                + WS
                + "."
                + ITEM
                + ".dbo.customers,PROD)"),
        urns.toString());

    // Without options the same event keeps the abs URN.
    Set<String> defaults = datasetUrns(event, FabricRequestOptions.apply(BASE, request()));
    assertTrue(
        defaults.stream().allMatch(u -> u.contains("dataPlatform:abs")), defaults.toString());
    assertNull(BASE.getFabricOneLakePlatformInstance());
  }

  private static Set<String> datasetUrns(OpenLineage.RunEvent event, DatahubOpenlineageConfig c) {
    return new RunEventMapper()
        .map(event, RunEventMapper.MappingConfig.builder().datahubConfig(c).build())
        .map(MetadataChangeProposal::getEntityUrn)
        .map(Object::toString)
        .filter(u -> u.startsWith("urn:li:dataset:"))
        .collect(Collectors.toSet());
  }
}
