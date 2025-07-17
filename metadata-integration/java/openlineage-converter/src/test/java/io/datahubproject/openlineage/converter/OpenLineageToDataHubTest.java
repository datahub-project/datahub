package io.datahubproject.openlineage.converter;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.openlineage.client.OpenLineage;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OpenLineageToDataHubTest {

  private OpenLineage.StaticDatasetBuilder datasetBuilder = new OpenLineage.StaticDatasetBuilder();

  @Test
  public void testPlatformInstanceNamespaceMapping() throws URISyntaxException {
    // Setup platform instance mapping
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");
    platformInstanceMap.put("mysql://staging.company.com:3306", "staging-mysql");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .build();

    // Test postgres namespace mapping
    OpenLineage.Dataset postgresDataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> postgresUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(postgresDataset, config);
    Assert.assertTrue(postgresUrn.isPresent());
    Assert.assertEquals(
        postgresUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-postgres.users,PROD)");

    // Test mysql namespace mapping
    OpenLineage.Dataset mysqlDataset =
        datasetBuilder.namespace("mysql://staging.company.com:3306").name("orders").build();

    Optional<DatasetUrn> mysqlUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(mysqlDataset, config);
    Assert.assertTrue(mysqlUrn.isPresent());
    Assert.assertEquals(
        mysqlUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,staging-mysql.orders,PROD)");
  }

  @Test
  public void testEnvironmentNamespaceMapping() throws URISyntaxException {
    // Setup environment mapping
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");
    environmentMap.put("postgres://dev.company.com:5432", "dev");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.TEST) // Default fallback
            .environmentNamespaceMap(environmentMap)
            .build();

    // Test production environment mapping
    OpenLineage.Dataset prodDataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> prodUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(prodDataset, config);
    Assert.assertTrue(prodUrn.isPresent());
    Assert.assertEquals(
        prodUrn.get().toString(), "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)");

    // Test development environment mapping
    OpenLineage.Dataset devDataset =
        datasetBuilder.namespace("postgres://dev.company.com:5432").name("users").build();

    Optional<DatasetUrn> devUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(devDataset, config);
    Assert.assertTrue(devUrn.isPresent());
    Assert.assertEquals(
        devUrn.get().toString(), "urn:li:dataset:(urn:li:dataPlatform:postgres,users,DEV)");
  }

  @Test
  public void testCombinedPlatformInstanceAndEnvironmentMapping() throws URISyntaxException {
    // Setup both platform instance and environment mapping
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");
    platformInstanceMap.put("postgres://dev.company.com:5432", "dev-postgres");

    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");
    environmentMap.put("postgres://dev.company.com:5432", "dev");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .environmentNamespaceMap(environmentMap)
            .build();

    // Test production mapping
    OpenLineage.Dataset prodDataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> prodUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(prodDataset, config);
    Assert.assertTrue(prodUrn.isPresent());
    Assert.assertEquals(
        prodUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-postgres.users,PROD)");

    // Test development mapping
    OpenLineage.Dataset devDataset =
        datasetBuilder.namespace("postgres://dev.company.com:5432").name("users").build();

    Optional<DatasetUrn> devUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(devDataset, config);
    Assert.assertTrue(devUrn.isPresent());
    Assert.assertEquals(
        devUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,dev-postgres.users,DEV)");
  }

  @Test
  public void testFallbackToDefaultWhenNoNamespaceMapping() throws URISyntaxException {
    // Setup config with no namespace mappings
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .commonDatasetPlatformInstance("default-instance")
            .build();

    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("postgres://unknown.company.com:5432").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    Assert.assertEquals(
        urn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,default-instance.users,PROD)");
  }

  @Test
  public void testFallbackToDefaultWhenNoMatchingNamespaceMapping() throws URISyntaxException {
    // Setup config with mappings that don't match
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");

    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.TEST)
            .commonDatasetPlatformInstance("default-instance")
            .platformInstanceNamespaceMap(platformInstanceMap)
            .environmentNamespaceMap(environmentMap)
            .build();

    // Test with namespace that doesn't match any mapping
    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("postgres://different.company.com:5432").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    Assert.assertEquals(
        urn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,default-instance.users,TEST)");
  }

  @Test
  public void testNonUriNamespaceHandling() throws URISyntaxException {
    // Setup namespace mappings
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .build();

    // Test with non-URI namespace (should fall back to treating namespace as platform)
    OpenLineage.Dataset dataset = datasetBuilder.namespace("hive").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    Assert.assertEquals(
        urn.get().toString(), "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)");
  }

  @Test
  public void testInvalidUriInMapping() throws URISyntaxException {
    // Setup config with invalid URI in mapping
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("invalid-uri-format", "prod-postgres");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .commonDatasetPlatformInstance("fallback-instance")
            .platformInstanceNamespaceMap(platformInstanceMap)
            .build();

    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    // Should fall back to default since mapping URI is invalid
    Assert.assertEquals(
        urn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,fallback-instance.users,PROD)");
  }

  @Test
  public void testInvalidEnvironmentValueInMapping() throws URISyntaxException {
    // Setup config with invalid environment value
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "invalid-env");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.TEST)
            .environmentNamespaceMap(environmentMap)
            .build();

    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    // Should fall back to default fabricType since environment mapping is invalid
    Assert.assertEquals(
        urn.get().toString(), "urn:li:dataset:(urn:li:dataPlatform:postgres,users,TEST)");
  }

  @Test
  public void testMultiplePlatformTypes() throws URISyntaxException {
    // Setup mappings for different platform types
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");
    platformInstanceMap.put("mysql://prod.company.com:3306", "prod-mysql");
    platformInstanceMap.put("trino://prod.company.com:8080", "prod-trino");

    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");
    environmentMap.put("mysql://prod.company.com:3306", "prod");
    environmentMap.put("trino://prod.company.com:8080", "prod");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.DEV)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .environmentNamespaceMap(environmentMap)
            .build();

    // Test PostgreSQL
    OpenLineage.Dataset postgresDataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> postgresUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(postgresDataset, config);
    Assert.assertTrue(postgresUrn.isPresent());
    Assert.assertEquals(
        postgresUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod-postgres.users,PROD)");

    // Test MySQL
    OpenLineage.Dataset mysqlDataset =
        datasetBuilder.namespace("mysql://prod.company.com:3306").name("orders").build();

    Optional<DatasetUrn> mysqlUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(mysqlDataset, config);
    Assert.assertTrue(mysqlUrn.isPresent());
    Assert.assertEquals(
        mysqlUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,prod-mysql.orders,PROD)");

    // Test Trino
    OpenLineage.Dataset trinoDataset =
        datasetBuilder
            .namespace("trino://prod.company.com:8080")
            .name("catalog.schema.table")
            .build();

    Optional<DatasetUrn> trinoUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(trinoDataset, config);
    Assert.assertTrue(trinoUrn.isPresent());
    Assert.assertEquals(
        trinoUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:trino,prod-trino.catalog.schema.table,PROD)");
  }

  @Test
  public void testDifferentPortsInNamespace() throws URISyntaxException {
    // Setup mappings with different ports
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://db.company.com:5432", "postgres-main");
    platformInstanceMap.put("postgres://db.company.com:5433", "postgres-replica");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .build();

    // Test main database
    OpenLineage.Dataset mainDbDataset =
        datasetBuilder.namespace("postgres://db.company.com:5432").name("users").build();

    Optional<DatasetUrn> mainDbUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(mainDbDataset, config);
    Assert.assertTrue(mainDbUrn.isPresent());
    Assert.assertEquals(
        mainDbUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres-main.users,PROD)");

    // Test replica database
    OpenLineage.Dataset replicaDbDataset =
        datasetBuilder.namespace("postgres://db.company.com:5433").name("users").build();

    Optional<DatasetUrn> replicaDbUrn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(replicaDbDataset, config);
    Assert.assertTrue(replicaDbUrn.isPresent());
    Assert.assertEquals(
        replicaDbUrn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres-replica.users,PROD)");
  }

  @Test
  public void testPlatformMappingWithAwsAthena() throws URISyntaxException {
    // Test with platform mapping for AWS Athena
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("awsathena://prod.company.com:443", "prod-athena");

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(platformInstanceMap)
            .build();

    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("awsathena://prod.company.com:443").name("database.table").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    // Should use the platform mapping awsathena -> athena
    Assert.assertEquals(
        urn.get().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:athena,prod-athena.database.table,PROD)");
  }

  @Test
  public void testEmptyNamespaceMappings() throws URISyntaxException {
    // Test with empty namespace mappings
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .platformInstanceNamespaceMap(new HashMap<>())
            .environmentNamespaceMap(new HashMap<>())
            .build();

    OpenLineage.Dataset dataset =
        datasetBuilder.namespace("postgres://prod.company.com:5432").name("users").build();

    Optional<DatasetUrn> urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(dataset, config);
    Assert.assertTrue(urn.isPresent());
    Assert.assertEquals(
        urn.get().toString(), "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)");
  }
}
