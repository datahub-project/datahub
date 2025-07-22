package io.datahubproject.openlineage.config;

import com.linkedin.common.FabricType;
import java.util.HashMap;
import java.util.Map;

/**
 * Example configuration class demonstrating how to set up namespace URI mappings for both platform
 * instances and environments.
 */
public class NamespaceMappingConfigurationExample {

  /** Example configuration for production environment with multiple database instances. */
  public static DatahubOpenlineageConfig createProductionConfig() {
    // Platform instance mappings
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod-primary.company.com:5432", "prod-postgres-primary");
    platformInstanceMap.put("postgres://prod-replica.company.com:5432", "prod-postgres-replica");
    platformInstanceMap.put("mysql://prod.company.com:3306", "prod-mysql");
    platformInstanceMap.put("trino://prod.company.com:8080", "prod-trino");

    // Environment mappings - map namespace URI to environment name
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod-primary.company.com:5432", "prod");
    environmentMap.put("mysql://prod.company.com:3306", "prod");
    environmentMap.put("trino://prod.company.com:8080", "prod");

    return DatahubOpenlineageConfig.builder()
        .platformInstanceNamespaceMap(platformInstanceMap)
        .environmentNamespaceMap(environmentMap)
        .fabricType(FabricType.PROD)
        .build();
  }

  /** Example configuration for development environment with different database instances. */
  public static DatahubOpenlineageConfig createDevelopmentConfig() {
    // Platform instance mappings
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://dev.company.com:5432", "dev-postgres");
    platformInstanceMap.put("mysql://dev.company.com:3306", "dev-mysql");

    // Environment mappings
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://dev.company.com:5432", "dev");
    environmentMap.put("mysql://dev.company.com:3306", "dev");

    return DatahubOpenlineageConfig.builder()
        .platformInstanceNamespaceMap(platformInstanceMap)
        .environmentNamespaceMap(environmentMap)
        .fabricType(FabricType.DEV)
        .build();
  }

  /** Example configuration for staging environment with mixed environments. */
  public static DatahubOpenlineageConfig createStagingConfig() {
    // Platform instance mappings
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://staging.company.com:5432", "staging-postgres");
    platformInstanceMap.put("mysql://staging.company.com:3306", "staging-mysql");

    // Environment mappings
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://staging.company.com:5432", "test");
    environmentMap.put("mysql://staging.company.com:3306", "test");

    return DatahubOpenlineageConfig.builder()
        .platformInstanceNamespaceMap(platformInstanceMap)
        .environmentNamespaceMap(environmentMap)
        .fabricType(FabricType.TEST)
        .build();
  }

  /**
   * Example configuration that combines namespace mappings with existing PathSpec configuration.
   */
  public static DatahubOpenlineageConfig createHybridConfig() {
    // Platform instance mappings for database connections
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");
    platformInstanceMap.put("mysql://prod.company.com:3306", "prod-mysql");

    // Environment mappings
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");
    environmentMap.put("mysql://prod.company.com:3306", "prod");

    return DatahubOpenlineageConfig.builder()
        .platformInstanceNamespaceMap(platformInstanceMap)
        .environmentNamespaceMap(environmentMap)
        .fabricType(FabricType.PROD)
        .commonDatasetPlatformInstance("default-instance") // Fallback for non-matching URIs
        // PathSpecs can still be used for other platforms like S3, GCS, etc.
        .build();
  }

  /** Example configuration with error handling scenarios. */
  public static DatahubOpenlineageConfig createConfigWithErrorHandling() {
    // Platform instance mappings with potential invalid URIs
    Map<String, String> platformInstanceMap = new HashMap<>();
    platformInstanceMap.put("postgres://prod.company.com:5432", "prod-postgres");
    platformInstanceMap.put("not-a-valid-uri", "invalid-instance"); // Will be ignored

    // Environment mappings with potential invalid environment values
    Map<String, String> environmentMap = new HashMap<>();
    environmentMap.put("postgres://prod.company.com:5432", "prod");
    environmentMap.put("postgres://other.company.com:5432", "invalid-env"); // Will be ignored

    return DatahubOpenlineageConfig.builder()
        .platformInstanceNamespaceMap(platformInstanceMap)
        .environmentNamespaceMap(environmentMap)
        .fabricType(FabricType.PROD) // Fallback for invalid environment mappings
        .commonDatasetPlatformInstance(
            "fallback-instance") // Fallback for invalid platform mappings
        .build();
  }

  /** Example usage demonstrating how different namespaces would be processed. */
  public static void demonstrateUsage() {
    DatahubOpenlineageConfig config = createProductionConfig();

    // Example namespaces and their expected processing:
    System.out.println("Configuration examples:");
    System.out.println(
        "1. postgres://prod-primary.company.com:5432 -> Platform Instance: prod-postgres-primary, Environment: PROD");
    System.out.println(
        "2. mysql://prod.company.com:3306 -> Platform Instance: prod-mysql, Environment: PROD");
    System.out.println(
        "3. trino://prod.company.com:8080 -> Platform Instance: prod-trino, Environment: PROD");
    System.out.println(
        "4. postgres://unknown.company.com:5432 -> Fallback to default configuration");
    System.out.println(
        "Note: Environment mappings should use unique keys per namespace URI to avoid Map key conflicts");
  }
}
