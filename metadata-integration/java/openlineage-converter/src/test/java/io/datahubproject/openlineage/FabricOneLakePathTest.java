package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.FabricOneLakePath;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class FabricOneLakePathTest {

  private static final String WS = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
  private static final String ITEM = "11112222-3333-4444-5555-666677778888";
  private static final String HOST = "onelake.dfs.fabric.microsoft.com";
  private static final String NAMESPACE = "abfss://" + WS + "@" + HOST;

  private static DatahubOpenlineageConfig defaultConfig() {
    return DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
  }

  private static String urn(String path, DatahubOpenlineageConfig config) throws Exception {
    return HdfsPathDataset.create(new URI(path), config).urn().toString();
  }

  private static String oneLakeUrn(String name, String env) {
    return "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake," + name + "," + env + ")";
  }

  @Test
  public void testGuidPathSchemasDisabledDefaultsToDbo() throws Exception {
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/customers", defaultConfig()),
        oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD"));
  }

  @Test
  public void testGuidPathSchemasEnabled() throws Exception {
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/sales/Customers", defaultConfig()),
        oneLakeUrn(WS + "." + ITEM + ".sales.Customers", "PROD"));
  }

  @Test
  public void testGuidsAreLowercasedButNamesKeepCaseByDefault() throws Exception {
    String path =
        "abfss://"
            + WS.toUpperCase()
            + "@"
            + HOST
            + "/"
            + ITEM.toUpperCase()
            + "/Tables/dbo/Orders";
    assertEquals(urn(path, defaultConfig()), oneLakeUrn(WS + "." + ITEM + ".dbo.Orders", "PROD"));
  }

  @Test
  public void testConvertUrnsToLowercase() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricOneLakeConvertUrnsToLowercase(true).build();
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/Sales/Customers", config),
        oneLakeUrn(WS + "." + ITEM + ".sales.customers", "PROD"));
  }

  @Test
  public void testGlobalLowerCaseDatasetUrnsIsCompatible() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().lowerCaseDatasetUrns(true).build();
    OpenLineage.Dataset dataset =
        new OpenLineage(URI.create("https://example.com"))
            .newInputDatasetBuilder()
            .namespace(NAMESPACE)
            .name("/" + ITEM + "/Tables/dbo/Customers")
            .build();
    Optional<DatasetUrn> result =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    assertEquals(result.get().toString(), oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD"));
  }

  @Test
  public void testTrailingSlashDeltaLogAndPartitionsAreStripped() throws Exception {
    String expected = oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD");
    assertEquals(urn(NAMESPACE + "/" + ITEM + "/Tables/customers/", defaultConfig()), expected);
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/customers/_delta_log", defaultConfig()), expected);
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/dbo/customers/region=west", defaultConfig()),
        expected);
  }

  @Test
  public void testEnvAndPlatformInstance() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.DEV)
            .commonDatasetPlatformInstance("shared")
            .fabricOneLakePlatformInstance("tenant_a")
            .build();
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/customers", config),
        oneLakeUrn("tenant_a." + WS + "." + ITEM + ".dbo.customers", "DEV"));

    DatahubOpenlineageConfig fallback =
        DatahubOpenlineageConfig.builder().commonDatasetPlatformInstance("shared").build();
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/customers", fallback),
        oneLakeUrn("shared." + WS + "." + ITEM + ".dbo.customers", "PROD"));
  }

  @Test
  public void testRegionalBlobAndPrivateLinkHosts() throws Exception {
    String expected = oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD");
    for (String host :
        new String[] {
          "westus-onelake.dfs.fabric.microsoft.com",
          "onelake.blob.fabric.microsoft.com",
          "api.onelake.fabric.microsoft.com",
          "westus-api.onelake.fabric.microsoft.com",
          WS.replace("-", "") + ".z0f.dfs.fabric.microsoft.com"
        }) {
      for (String scheme : new String[] {"abfss", "abfs", "wasbs"}) {
        assertEquals(
            urn(
                scheme + "://" + WS + "@" + host + "/" + ITEM + "/Tables/customers",
                defaultConfig()),
            expected,
            scheme + "://" + host);
      }
    }
  }

  @Test
  public void testFilesPathStaysAbs() throws Exception {
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Files/raw/customers.csv", defaultConfig()),
        "urn:li:dataset:(urn:li:dataPlatform:abs,"
            + WS
            + "@"
            + HOST
            + "/"
            + ITEM
            + "/Files/raw/customers.csv,PROD)");
  }

  @Test
  public void testNonOneLakeAbfssUnchanged() throws Exception {
    assertEquals(
        urn("abfss://data@myaccount.dfs.core.windows.net/Tables/customers", defaultConfig()),
        "urn:li:dataset:(urn:li:dataPlatform:abs,data@myaccount.dfs.core.windows.net/Tables/customers,PROD)");
  }

  @Test
  public void testDisabledKeepsAbs() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricOneLakeEnabled(false).build();
    assertEquals(
        urn(NAMESPACE + "/" + ITEM + "/Tables/customers", config),
        "urn:li:dataset:(urn:li:dataPlatform:abs,"
            + WS
            + "@"
            + HOST
            + "/"
            + ITEM
            + "/Tables/customers,PROD)");
  }

  @Test
  public void testFriendlyNamesWithoutMappingStayAbs() throws Exception {
    assertEquals(
        urn("abfss://Sales@" + HOST + "/bronze.Lakehouse/Tables/customers", defaultConfig()),
        "urn:li:dataset:(urn:li:dataPlatform:abs,Sales@"
            + HOST
            + "/bronze.Lakehouse/Tables/customers,PROD)");
  }

  @Test
  public void testFriendlyNamesWithMapping() throws Exception {
    Map<String, String> itemIds =
        FabricOneLakePath.parseItemIds(
            " Sales/bronze.Lakehouse = " + WS + "/" + ITEM + " , broken-entry ,");
    assertEquals(itemIds, Collections.singletonMap("Sales/bronze.Lakehouse", WS + "/" + ITEM));
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricOneLakeItemIds(itemIds).build();
    assertEquals(
        urn("abfss://Sales@" + HOST + "/bronze.Lakehouse/Tables/dbo/customers", config),
        oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD"));
    // lookup is case-insensitive (e.g. when lowerCaseDatasetUrns lowercased the path)
    assertEquals(
        urn("abfss://sales@" + HOST + "/bronze.lakehouse/tables/customers", config),
        oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD"));
    // unmapped item in the same workspace stays abs
    assertTrue(
        urn("abfss://Sales@" + HOST + "/silver.Lakehouse/Tables/customers", config)
            .contains("dataPlatform:abs"));
  }

  @Test
  public void testInvalidMappingValueStaysAbs() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricOneLakeItemIds(Collections.singletonMap("Sales/bronze.Lakehouse", "not-a-guid"))
            .build();
    assertTrue(
        urn("abfss://Sales@" + HOST + "/bronze.Lakehouse/Tables/customers", config)
            .contains("dataPlatform:abs"));
  }

  @Test
  public void testUnsupportedShapesAreNotMapped() {
    DatahubOpenlineageConfig config = defaultConfig();
    // Tables root itself, and too-deep paths that are not partitions
    assertFalse(
        FabricOneLakePath.toDatasetName(URI.create(NAMESPACE + "/" + ITEM + "/Tables"), config)
            .isPresent());
    assertFalse(
        FabricOneLakePath.toDatasetName(
                URI.create(NAMESPACE + "/" + ITEM + "/Tables/a/b/c"), config)
            .isPresent());
    // https URLs are not handled here
    assertFalse(
        FabricOneLakePath.toDatasetName(
                URI.create("https://" + HOST + "/" + WS + "/" + ITEM + "/Tables/customers"), config)
            .isPresent());
  }

  @Test
  public void testOneLakeLocationWinsOverCatalogSymlink() {
    DatahubOpenlineageConfig config = defaultConfig();
    OpenLineage ol = new OpenLineage(URI.create("https://example.com"));
    OpenLineage.SymlinksDatasetFacet symlinks =
        ol.newSymlinksDatasetFacetBuilder()
            .identifiers(
                Collections.singletonList(
                    ol.newSymlinksDatasetFacetIdentifiersBuilder()
                        .namespace(NAMESPACE + "/" + ITEM + "/Tables")
                        .name("silver_lh.customers")
                        .type("TABLE")
                        .build()))
            .build();
    OpenLineage.Dataset dataset =
        ol.newOutputDatasetBuilder()
            .namespace(NAMESPACE)
            .name("/" + ITEM + "/Tables/customers")
            .facets(ol.newDatasetFacetsBuilder().symlinks(symlinks).build())
            .build();

    String expected = oneLakeUrn(WS + "." + ITEM + ".dbo.customers", "PROD");
    assertEquals(
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config)
            .get()
            .toString(),
        expected);
    // the catalog-name URN is aliased to the OneLake URN
    assertEquals(
        config
            .getUrnAliases()
            .get("urn:li:dataset:(urn:li:dataPlatform:hive,silver_lh.customers,PROD)"),
        expected);

    // A non-OneLake location still resolves through the symlink as before.
    OpenLineage.Dataset adls =
        ol.newOutputDatasetBuilder()
            .namespace("abfss://data@myaccount.dfs.core.windows.net")
            .name("/warehouse/customers")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .symlinks(
                        ol.newSymlinksDatasetFacetBuilder()
                            .identifiers(
                                Collections.singletonList(
                                    ol.newSymlinksDatasetFacetIdentifiersBuilder()
                                        .namespace("hive://metastore:9083")
                                        .name("default.customers")
                                        .type("TABLE")
                                        .build()))
                            .build())
                    .build())
            .build();
    assertEquals(
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(adls, config).get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:hive,default.customers,PROD)");
  }
}
