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

  /** Test configuration with PROD environment */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigProd.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=PROD"})
  public static class ProdEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testProdEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.PROD);
    }
  }

  /** Test configuration with DEV environment */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigDev.class})
  @TestPropertySource(properties = {"datahub.openlineage.env=DEV"})
  public static class DevEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testDevEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.DEV);
    }
  }

  /** Test configuration with all valid environment types */
  @SpringBootTest(classes = {OpenLineageServletConfig.class, TestConfigQa.class})
  @TestPropertySource(
      properties = {
        "datahub.openlineage.env=QA",
        "datahub.openlineage.orchestrator=test-orchestrator"
      })
  public static class AllValidEnvTest extends AbstractTestNGSpringContextTests {

    @Autowired private RunEventMapper.MappingConfig mappingConfig;

    @Test
    public void testQaEnv() {
      assertNotNull(mappingConfig);
      assertNotNull(mappingConfig.getDatahubConfig());
      assertEquals(mappingConfig.getDatahubConfig().getFabricType(), FabricType.QA);
      assertEquals(mappingConfig.getDatahubConfig().getOrchestrator(), "test-orchestrator");
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

  // Test configuration classes - each needs to provide DatahubOpenlineageProperties bean
  @Configuration
  static class TestConfigProd {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("PROD");
      return props;
    }
  }

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
  static class TestConfigQa {
    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties props = new DatahubOpenlineageProperties();
      props.setEnv("QA");
      props.setOrchestrator("test-orchestrator");
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
}
