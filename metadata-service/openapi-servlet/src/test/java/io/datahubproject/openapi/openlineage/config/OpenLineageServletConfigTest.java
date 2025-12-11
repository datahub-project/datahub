/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.openlineage.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.FabricType;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import org.springframework.beans.factory.annotation.Autowired;
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
}
