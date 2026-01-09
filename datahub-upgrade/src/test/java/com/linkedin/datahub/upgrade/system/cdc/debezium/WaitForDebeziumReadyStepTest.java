package com.linkedin.datahub.upgrade.system.cdc.debezium;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WaitForDebeziumReadyStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  @Mock private UpgradeContext mockUpgradeContext;

  private DebeziumConfiguration debeziumConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  private static class TestableWaitForDebeziumReadyStep extends WaitForDebeziumReadyStep {
    private final HttpClient mockHttpClient;

    public TestableWaitForDebeziumReadyStep(
        OperationContext opContext,
        DebeziumConfiguration debeziumConfig,
        KafkaConfiguration kafkaConfiguration,
        KafkaProperties kafkaProperties,
        HttpClient mockHttpClient) {
      super(opContext, debeziumConfig, kafkaConfiguration, kafkaProperties);
      this.mockHttpClient = mockHttpClient;
    }

    @Override
    protected HttpClient createHttpClient() {
      return mockHttpClient;
    }
  }

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    debeziumConfig = new DebeziumConfiguration();
    debeziumConfig.setUrl("http://localhost:8083");
    debeziumConfig.setRequestTimeoutMillis(30000);

    kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");

    kafkaProperties = new KafkaProperties();
  }

  @Test
  public void testStepBasics() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties);

    assertEquals(step.id(), "WaitForDebeziumReadyStep");
    assertEquals(step.retryCount(), 20);
    assertNotNull(step.executable());
  }

  @Test
  public void testSuccessfulConnection() throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(mockResponse);

    TestableWaitForDebeziumReadyStep step =
        new TestableWaitForDebeziumReadyStep(
            OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties, mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.stepId(), "WaitForDebeziumReadyStep");
  }

  @Test
  public void testConnectionFailureNon200Status() throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);

    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn("Internal Server Error");
    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(mockResponse);

    TestableWaitForDebeziumReadyStep step =
        new TestableWaitForDebeziumReadyStep(
            OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties, mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testConnectionException() throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);

    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenThrow(new RuntimeException("Connection refused"));

    TestableWaitForDebeziumReadyStep step =
        new TestableWaitForDebeziumReadyStep(
            OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties, mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testMissingConnectUrl() {
    debeziumConfig.setUrl(null);

    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testEmptyConnectUrl() {
    debeziumConfig.setUrl("");

    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testInterruptedException() throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);

    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              throw new InterruptedException("Thread interrupted");
            });

    TestableWaitForDebeziumReadyStep step =
        new TestableWaitForDebeziumReadyStep(
            OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties, mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testCreateHttpClient() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(OP_CONTEXT, debeziumConfig, kafkaConfig, kafkaProperties);

    HttpClient client = step.createHttpClient();
    assertNotNull(client);
  }
}
