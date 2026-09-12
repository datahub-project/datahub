package io.datahubproject.openapi.openlineage.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.PathSpec;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/** Tests for OpenLineageServletConfig parsing and configuration */
public class OpenLineageServletConfigTest extends AbstractTestNGSpringContextTests {

  /** Test configuration with valid environment */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigDev.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=DEV"})
  public static class ValidEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testValidEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.DEV);
    }
  }

  /** Test configuration with invalid environment (should default to PROD with warning) */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigInvalid.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=INVALID_ENV"})
  public static class InvalidEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testInvalidEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      // Should default to PROD when invalid value is provided
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.PROD);
    }
  }

  /** Test configuration with empty environment (should default to PROD) */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigEmpty.class})
  public static class EmptyEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testEmptyEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      // Should default to PROD when no value is provided
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.PROD);
    }
  }

  /** Test orchestrator configuration */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigOrchestrator.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.orchestrator=my-custom-orchestrator",
        "datahub.openlineage.env=STG"
      })
  public static class OrchestratorTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testOrchestratorConfig() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertEquals(mappingConfig.getDatahubConfig().getOrchestrator(), "my-custom-orchestrator");
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.STG);
    }
  }

  /** Test that null orchestrator is handled correctly */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigNoOrchestrator.class})
  public static class NoOrchestratorTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testNoOrchestrator() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertNull(mappingConfig.getDatahubConfig().getOrchestrator());
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.PROD);
    }
  }

  /** Test all config properties together */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigComplete.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=TEST",
        "datahub.openlineage.orchestrator=airflow",
        "datahub.openlineage.platform-instance=us-west-2",
        "datahub.openlineage.materialize-dataset=false",
        "datahub.openlineage.include-schema-metadata=false",
        "datahub.openlineage.capture-column-level-lineage=false"
      })
  public static class CompleteConfigTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testCompleteConfig() {
      assertNotNull(mappingConfig);
      DatahubOpenlineageConfig config = mappingConfig.getDatahubConfig();
      assertNotNull(config);

      // Verify all configured values
      assertEquals(config.getFabricType(), FabricType.TEST);
      assertEquals(config.getOrchestrator(), "airflow");
      assertEquals(config.getPlatformInstance(), "us-west-2");
      assertEquals(config.isMaterializeDataset(), false);
      assertEquals(config.isIncludeSchemaMetadata(), false);
      assertEquals(config.isCaptureColumnLevelLineage(), false);
    }
  }

  /** Test case-insensitive env parsing */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigCaseInsensitive.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=stg" // lowercase
      })
  public static class CaseInsensitiveTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testCaseInsensitiveEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      // Should handle lowercase and convert to STG
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.STG);
    }
  }

  /** Test that env sets both DataFlow cluster and Dataset fabricType by default */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigEnvSetsCluster.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=DEV"})
  public static class EnvSetsClusterTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testEnvSetsCluster() {
      assertNotNull(mappingConfig);
      DatahubOpenlineageConfig config = mappingConfig.getDatahubConfig();
      assertNotNull(config);

      // Env should set Dataset fabricType
      assertEquals(config.getFabricType(), FabricType.DEV);

      // Env should also set DataFlow cluster (via platformInstance)
      assertEquals(
          config.getPlatformInstance(), "dev", "env should default DataFlow cluster to 'dev'");
    }
  }

  /** Test that platformInstance overrides env for DataFlow cluster */
  @SpringBootTest(
      classes = {OpenLineageServletConfig.class, TestConfigPlatformInstanceOverride.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=PROD",
        "datahub.openlineage.platform-instance=prod-us-west-2"
      })
  public static class PlatformInstanceOverrideTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testPlatformInstanceOverride() {
      assertNotNull(mappingConfig);
      DatahubOpenlineageConfig config = mappingConfig.getDatahubConfig();
      assertNotNull(config);

      // Dataset should use PROD
      assertEquals(config.getFabricType(), FabricType.PROD);

      // DataFlow cluster should use the override
      assertEquals(
          config.getPlatformInstance(),
          "prod-us-west-2",
          "platformInstance should override env for DataFlow cluster");
    }
  }

  /** Test that commonDatasetEnv overrides env for Dataset fabricType */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigCommonDatasetEnv.class})
  @TestPropertySource(
      properties = {"datahub.openlineage.env=PROD", "datahub.openlineage.common-dataset-env=DEV"})
  public static class CommonDatasetEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testCommonDatasetEnv() {
      assertNotNull(mappingConfig);
      DatahubOpenlineageConfig config = mappingConfig.getDatahubConfig();
      assertNotNull(config);

      // Dataset should use DEV (override)
      assertEquals(config.getFabricType(), FabricType.DEV);

      // DataFlow cluster should use prod (from env)
      assertEquals(config.getPlatformInstance(), "prod", "DataFlow cluster should use env");

      // Config should have commonDatasetEnv set
      assertEquals(config.getCommonDatasetEnv(), "DEV");
    }
  }

  // Test configuration classes - each needs to provide DatahubOpenlineageProperties bean
  @Configuration
  static class TestConfigDev {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("DEV");
      return props;
    }
  }

  @Configuration
  static class TestConfigInvalid {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("INVALID_ENV");
      return props;
    }
  }

  @Configuration
  static class TestConfigEmpty {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      return new DatahubOpenlineageProperties();
    }
  }

  @Configuration
  static class TestConfigOrchestrator {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("STG");
      props.setOrchestrator("my-custom-orchestrator");
      return props;
    }
  }

  @Configuration
  static class TestConfigNoOrchestrator {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("PROD");
      return props;
    }
  }

  @Configuration
  static class TestConfigComplete {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("TEST");
      props.setOrchestrator("airflow");
      props.setPlatformInstance("us-west-2");
      props.setMaterializeDataset(false);
      props.setIncludeSchemaMetadata(false);
      props.setCaptureColumnLevelLineage(false);
      return props;
    }
  }

  @Configuration
  static class TestConfigCaseInsensitive {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("stg"); // lowercase
      return props;
    }
  }

  @Configuration
  static class TestConfigEnvSetsCluster {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("DEV");
      return props;
    }
  }

  @Configuration
  static class TestConfigPlatformInstanceOverride {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("PROD");
      props.setPlatformInstance("prod-us-west-2");
      return props;
    }
  }

  @Configuration
  static class TestConfigCommonDatasetEnv {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("PROD");
      props.setCommonDatasetEnv("DEV");
      return props;
    }
  }

  /**
   * Unlike the cases above, which hand-build the properties bean and so never exercise binding,
   * this one enables real {@code @ConfigurationProperties} binding. It covers both the {@code
   * .domains(...)} wiring in {@link OpenLineageServletConfig#mappingConfig()} and {@code
   * DATAHUB_OPENLINEAGE_DOMAINS} parsing as a comma-separated list.
   */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=PROD",
        "datahub.openlineage.domains=urn:li:domain:finance,urn:li:domain:reporting"
      })
  public static class BoundDomainsTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testCommaSeparatedDomainsBindAndReachConverterConfig() {
      assertEquals(
          mappingConfig.getDatahubConfig().getDomains(),
          List.of("urn:li:domain:finance", "urn:li:domain:reporting"));
    }
  }

  /**
   * OpenLineage producers split a run's metadata across START, RUNNING and a terminal event, so the
   * endpoint has to apply lineage additively or the terminal event overwrites what the earlier ones
   * declared.
   */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=PROD"})
  public static class UsePatchDefaultTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testUsePatchDefaultsToTrue() {
      assertTrue(mappingConfig.getDatahubConfig().isUsePatch());
    }
  }

  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(
      properties = {"datahub.openlineage.env=PROD", "datahub.openlineage.use-patch=false"})
  public static class UsePatchOverrideTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testUsePatchCanBeDisabled() {
      assertFalse(mappingConfig.getDatahubConfig().isUsePatch());
    }
  }

  /**
   * The converter's PathSpec and ConnectionInstanceDetail use final fields and {@code Optional}, so
   * they cannot be bound directly — these cover the translation. Connection keys contain {@code :}
   * and {@code /}, so they need bracket syntax; the backslashes escape the colons, which inline
   * {@code @TestPropertySource} entries would otherwise read as a properties-format key/value
   * separator.
   */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=PROD",
        "datahub.openlineage.connections[snowflake\\://my-account].platform-instance=my_instance",
        "datahub.openlineage.connections[snowflake\\://my-account].env=DEV",
        "datahub.openlineage.path-specs.s3[0].alias=events",
        "datahub.openlineage.path-specs.s3[0].platform=s3",
        "datahub.openlineage.path-specs.s3[0].path-spec-list=s3\\://my-bucket/{table}",
        "datahub.openlineage.path-specs.s3[0].platform-instance=my_s3"
      })
  public static class BoundNestedConfigTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testConnectionsBindAndReachConverterConfig() {
      ConnectionInstanceDetail detail =
          mappingConfig.getDatahubConfig().getConnectionInstanceMap().get("snowflake://my-account");
      assertNotNull(detail, "connection entry should bind under its namespace authority");
      assertEquals(detail.getPlatformInstance(), Optional.of("my_instance"));
      // env is validated to a FabricType at startup, not left as a raw string.
      assertEquals(detail.getEnv(), Optional.of(FabricType.DEV));
    }

    @Test
    public void testPathSpecsBindAndReachConverterConfig() {
      List<PathSpec> specs = mappingConfig.getDatahubConfig().getPathSpecs().get("s3");
      assertNotNull(specs, "path specs should bind under their platform key");
      assertEquals(specs.size(), 1);
      assertEquals(specs.get(0).getAlias(), "events");
      assertEquals(specs.get(0).getPathSpecList(), List.of("s3://my-bucket/{table}"));
      assertEquals(specs.get(0).getPlatformInstance(), Optional.of("my_s3"));
    }
  }

  /** An invalid connection env must not silently become the global env without a warning. */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=PROD",
        "datahub.openlineage.connections[postgres\\://my-host\\:5432].env=NOT_A_FABRIC_TYPE"
      })
  public static class InvalidConnectionEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testInvalidConnectionEnvFallsBack() {
      ConnectionInstanceDetail detail =
          mappingConfig
              .getDatahubConfig()
              .getConnectionInstanceMap()
              .get("postgres://my-host:5432");
      assertNotNull(detail);
      assertEquals(detail.getEnv(), Optional.empty());
    }
  }

  /** The URN-shaping and advanced flags were declared on the converter but never wired. */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigBoundProperties.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=PROD",
        "datahub.openlineage.pipeline-name=my_pipeline",
        "datahub.openlineage.lower-case-dataset-urns=true",
        "datahub.openlineage.hive-platform-alias=glue",
        "datahub.openlineage.disable-symlink-resolution=true",
        "datahub.openlineage.remove-legacy-lineage=true",
        "datahub.openlineage.include-indirect-column-lineage=false",
        "datahub.openlineage.enhanced-merge-into-extraction=true"
      })
  public static class AdvancedFlagsTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testAdvancedFlagsReachConverterConfig() {
      DatahubOpenlineageConfig config = mappingConfig.getDatahubConfig();
      assertEquals(config.getPipelineName(), "my_pipeline");
      assertTrue(config.isLowerCaseDatasetUrns());
      assertEquals(config.getHivePlatformAlias(), "glue");
      assertTrue(config.isDisableSymlinkResolution());
      assertTrue(config.isRemoveLegacyLineage());
      assertFalse(config.isIncludeIndirectColumnLineage());
      assertTrue(config.isEnhancedMergeIntoExtraction());
    }
  }

  @Configuration
  @EnableConfigurationProperties(DatahubOpenlineageProperties.class)
  static class TestConfigBoundProperties {}
}
