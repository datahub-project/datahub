package com.linkedin.datahub.upgrade.system.cdc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCSourceSetupTest {

  @Mock private OperationContext mockOpContext;
  @Mock private UpgradeStep mockStep1;
  @Mock private UpgradeStep mockStep2;

  private CDCSourceConfiguration cdcSourceConfig;

  private static class TestCDCSourceSetup extends CDCSourceSetup {
    private final List<UpgradeStep> steps;
    private final boolean shouldThrowException;

    public TestCDCSourceSetup(
        OperationContext opContext,
        CDCSourceConfiguration cdcSourceConfig,
        List<UpgradeStep> steps,
        boolean shouldThrowException) {
      super(opContext, cdcSourceConfig);
      this.steps = steps;
      this.shouldThrowException = shouldThrowException;
    }

    @Override
    public String id() {
      return "TestCDCSourceSetup";
    }

    @Override
    protected List<UpgradeStep> createSteps() {
      if (shouldThrowException) {
        throw new RuntimeException("Test exception");
      }
      return steps;
    }
  }

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    setupValidConfigurations();
  }

  private void setupValidConfigurations() {
    // Create valid CDCSourceConfiguration
    cdcSourceConfig = new CDCSourceConfiguration();
    cdcSourceConfig.setEnabled(true);
    cdcSourceConfig.setConfigureSource(true);
    cdcSourceConfig.setType("debezium");

    DebeziumConfiguration debeziumConfig = new DebeziumConfiguration();
    debeziumConfig.setName("test-connector");
    debeziumConfig.setUrl("http://localhost:8083");
    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    connectorConfig.put("database.include.list", "testdb");
    debeziumConfig.setConfig(connectorConfig);
    cdcSourceConfig.setCdcImplConfig(debeziumConfig);
  }

  @Test
  public void testConstructorAndBasicMethods() {
    List<UpgradeStep> expectedSteps = Arrays.asList(mockStep1, mockStep2);
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, expectedSteps, false);

    assertEquals(setup.getCdcType(), "debezium");
    assertNotNull(setup.steps());
    assertEquals(setup.steps().size(), 2);
    assertTrue(setup.canRun());
  }

  @Test
  public void testCanRunValidConfiguration() {
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertTrue(setup.canRun());
  }

  @Test
  public void testCanRunNullCdcSourceConfig() {
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, null, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunCdcDisabled() {
    cdcSourceConfig.setEnabled(false);

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunConfigureSourceDisabled() {
    cdcSourceConfig.setConfigureSource(false);

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunNullCdcType() {
    cdcSourceConfig.setType(null);

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunEmptyCdcType() {
    cdcSourceConfig.setType("");

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunWhitespaceCdcType() {
    cdcSourceConfig.setType("   ");

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testCanRunNullCdcImplConfig() {
    cdcSourceConfig.setCdcImplConfig(null);

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertFalse(setup.canRun());
  }

  @Test
  public void testStepsExceptionHandling() {
    TestCDCSourceSetup setup = new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, null, true);

    List<UpgradeStep> steps = setup.steps();
    assertNotNull(steps);
    assertEquals(steps.size(), 0);
  }

  @Test
  public void testGetCdcTypeWithNullConfig() {
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, null, Arrays.asList(mockStep1), false);

    assertEquals(setup.getCdcType(), null);
  }

  @Test
  public void testGetCdcTypeWithValidConfig() {
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertEquals(setup.getCdcType(), "debezium");
  }

  @Test
  public void testStepsWithMultipleSteps() {
    List<UpgradeStep> expectedSteps = Arrays.asList(mockStep1, mockStep2);
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, expectedSteps, false);

    List<UpgradeStep> actualSteps = setup.steps();
    assertEquals(actualSteps.size(), 2);
    assertEquals(actualSteps.get(0), mockStep1);
    assertEquals(actualSteps.get(1), mockStep2);
  }

  @Test
  public void testStepsWithEmptySteps() {
    List<UpgradeStep> expectedSteps = Arrays.asList();
    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, expectedSteps, false);

    List<UpgradeStep> actualSteps = setup.steps();
    assertEquals(actualSteps.size(), 0);
  }

  @Test
  public void testCanRunWithDifferentCdcTypes() {
    cdcSourceConfig.setType("debezium-kafka-connector");

    TestCDCSourceSetup setup =
        new TestCDCSourceSetup(mockOpContext, cdcSourceConfig, Arrays.asList(mockStep1), false);

    assertTrue(setup.canRun());
    assertEquals(setup.getCdcType(), "debezium-kafka-connector");
  }
}
