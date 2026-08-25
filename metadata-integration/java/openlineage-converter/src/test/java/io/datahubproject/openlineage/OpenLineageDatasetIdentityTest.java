package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class OpenLineageDatasetIdentityTest {
  private static final String CATALOG_NAMESPACE = "https://catalog.example/api/catalog";
  private final OpenLineage openLineage =
      new OpenLineage(URI.create("https://github.com/OpenLineage/OpenLineage/spark"));

  @Test
  public void testIcebergCatalogSelectsIcebergPlatformCaseInsensitively() {
    OpenLineage.InputDataset dataset =
        tableDataset("IcEbErG", "rest", CATALOG_NAMESPACE, "table/Analytics/Events");
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .lowerCaseDatasetUrns(true)
            .build();

    assertEquals(
        convert(dataset, config),
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,analytics.events,PROD)");
  }

  @Test
  public void testIcebergCatalogPreservesConnectionInstanceAndEnvironment() {
    OpenLineage.InputDataset dataset =
        tableDataset("iceberg", "rest", CATALOG_NAMESPACE, "my_db.events");
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.QA)
            .connectionInstanceMap(
                Map.of(
                    CATALOG_NAMESPACE,
                    ConnectionInstanceDetail.builder()
                        .platformInstance(Optional.of("iceberg_prod"))
                        .env(Optional.of(FabricType.PROD))
                        .build()))
            .build();

    assertEquals(
        convert(dataset, config),
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,iceberg_prod.my_db.events,PROD)");
  }

  @Test
  public void testHiveRestAndGlueSymlinkPlatformsRemainUnchanged() {
    OpenLineage.InputDataset hive =
        tableDataset(null, null, "hive://metastore.example:9083", "my_db.events");
    OpenLineage.InputDataset restCatalog =
        tableDataset(null, "rest", CATALOG_NAMESPACE, "my_db.events");
    OpenLineage.InputDataset glueCatalog =
        tableDataset("iceberg", "glue", "arn:aws:glue:us-east-1:111122223333", "my_db.events");
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();

    assertEquals(
        convert(hive, config), "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)");
    assertEquals(
        convert(restCatalog, config),
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)");
    assertEquals(
        convert(glueCatalog, config),
        "urn:li:dataset:(urn:li:dataPlatform:glue,my_db.events,PROD)");
  }

  @Test
  public void testMissingLogicalNameKeepsPhysicalIdentity() {
    OpenLineage.InputDataset dataset = tableDataset("iceberg", "rest", CATALOG_NAMESPACE, "  ");
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();

    assertEquals(
        convert(dataset, config),
        "urn:li:dataset:(urn:li:dataPlatform:s3,warehouse-bucket/catalog/db/table,PROD)");
  }

  @Test
  public void testMixedPlatformJobKeepsIndependentDatasetIdentities() throws Exception {
    OpenLineage.InputDataset iceberg =
        tableDataset("iceberg", "rest", CATALOG_NAMESPACE, "my_db.events");
    OpenLineage.InputDataset postgres =
        openLineage
            .newInputDatasetBuilder()
            .namespace("postgres://database.example:5432")
            .name("my_db.public.users")
            .build();
    OpenLineage.RunEvent event =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.parse("2026-01-01T00:00:00Z"))
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(UUID.randomUUID())
                    .facets(openLineage.newRunFacetsBuilder().build())
                    .build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("spark")
                    .name("mixed-platform-job")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(List.of(iceberg, postgres))
            .outputs(Collections.emptyList())
            .build();

    DatahubJob job =
        OpenLineageToDataHub.convertRunEventToJob(
            event,
            DatahubOpenlineageConfig.builder()
                .fabricType(FabricType.PROD)
                .orchestrator("spark")
                .build());
    Set<String> inputUrns =
        job.getInSet().stream()
            .map(datahubDataset -> datahubDataset.getUrn().toString())
            .collect(Collectors.toSet());

    assertEquals(
        inputUrns,
        Set.of(
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,my_db.events,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,my_db.public.users,PROD)"));
  }

  @Test
  public void testPhysicalIdentityContinuesToResolveThroughIcebergAlias() {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
    OpenLineage.InputDataset logical =
        tableDataset("iceberg", "rest", CATALOG_NAMESPACE, "my_db.events");

    String logicalUrn = convert(logical, config);
    OpenLineage.InputDataset physical =
        openLineage
            .newInputDatasetBuilder()
            .namespace("s3://warehouse-bucket")
            .name("catalog/db/table")
            .build();

    assertEquals(convert(physical, config), logicalUrn);
  }

  private OpenLineage.InputDataset tableDataset(
      String framework, String type, String symlinkNamespace, String symlinkName) {
    OpenLineage.SymlinksDatasetFacet symlinks =
        openLineage.newSymlinksDatasetFacet(
            List.of(
                openLineage.newSymlinksDatasetFacetIdentifiers(
                    symlinkNamespace, symlinkName, "TABLE")));
    OpenLineage.DatasetFacetsBuilder facets =
        openLineage.newDatasetFacetsBuilder().symlinks(symlinks);
    if (framework != null || type != null) {
      facets.catalog(
          openLineage
              .newCatalogDatasetFacetBuilder()
              .framework(framework)
              .type(type)
              .name("catalog")
              .metadataUri(CATALOG_NAMESPACE)
              .warehouseUri("s3://warehouse-bucket")
              .build());
    }
    return openLineage
        .newInputDatasetBuilder()
        .namespace("s3://warehouse-bucket")
        .name("catalog/db/table")
        .facets(facets.build())
        .build();
  }

  private static String convert(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, mappingConfig);
    assertTrue(urn.isPresent());
    return urn.get().toString();
  }
}
