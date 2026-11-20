package com.linkedin.gms.factory.clientwrite;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClientWriteRecorderTest {

  private static final ClientWrite CUSTOM_CODE =
      new ClientWrite("openapi", "unknown", "none", "none", "curl/8.4.0");
  private static final ClientWrite PYTHON_CLI =
      new ClientWrite("restli", "python", "INGESTION", "1.7.0", "DataHub-Client/1.0");

  private SimpleMeterRegistry registry;
  private AtomicLong clock;
  private List<String> summaries;

  @BeforeMethod
  public void setUp() {
    registry = new SimpleMeterRegistry();
    clock = new AtomicLong(0);
    summaries = new ArrayList<>();
  }

  private ClientWriteRecorder recorder(int maxTrackedKeys) {
    return new ClientWriteRecorder(
        registry, Duration.ofHours(1), maxTrackedKeys, clock::get, summaries::add);
  }

  @Test
  public void testMetricIsTaggedWithClientButNotActor() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.record(PYTHON_CLI, "urn:li:corpuser:executor", false);
    recorder.record(PYTHON_CLI, "urn:li:corpuser:someone", false);

    Counter counter =
        registry
            .get(ClientWriteRecorder.METRIC_NAME)
            .tags(
                "client", "python",
                "client_mode", "INGESTION",
                "client_version", "1.7.0",
                "api", "restli",
                "system_actor", "false")
            .counter();
    assertEquals(counter.count(), 2.0);
    assertTrue(
        registry.getMeters().stream().allMatch(meter -> meter.getId().getTag("actor") == null));
  }

  @Test
  public void testSummaryAttributesWritesPerActorOncePerInterval() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);
    recorder.record(PYTHON_CLI, null, false);
    assertTrue(summaries.isEmpty());

    clock.set(Duration.ofHours(1).toMillis());
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);

    assertEquals(summaries.size(), 2);
    String custom =
        summaries.stream().filter(s -> s.contains("svc_loader")).findFirst().orElseThrow();
    assertTrue(custom.startsWith(ClientWriteRecorder.SUMMARY_PREFIX));
    assertTrue(custom.contains("client=unknown"));
    assertTrue(custom.contains("count=3"));
    assertTrue(custom.contains("user_agent=\"curl/8.4.0\""));
    assertTrue(summaries.stream().anyMatch(s -> s.contains("actor=anonymous")));

    // Counts restart after a summary.
    summaries.clear();
    clock.set(Duration.ofHours(2).toMillis());
    recorder.record(PYTHON_CLI, null, false);
    assertEquals(summaries.size(), 1);
    assertTrue(summaries.get(0).contains("count=1"));
  }

  @Test
  public void testCombinationsBeyondTheCapAreReportedAsUntracked() {
    ClientWriteRecorder recorder = recorder(1);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:a", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:b", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:c", false);

    clock.set(Duration.ofHours(1).toMillis());
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:a", false);

    assertTrue(summaries.stream().anyMatch(s -> s.contains("untracked=2")));
    assertEquals(
        registry.get(ClientWriteRecorder.METRIC_NAME).counter().count(),
        4.0,
        "the metric counts every write, tracked or not");
  }

  @Test
  public void testVersionTagValuesAreCapped() {
    ClientWriteRecorder recorder = recorder(100);
    for (int i = 0; i <= ClientWriteRecorder.MAX_VERSION_TAGS; i++) {
      recorder.record(new ClientWrite("restli", "python", "SDK", "9.9." + i, "ua"), null, false);
    }
    assertEquals(
        registry
            .get(ClientWriteRecorder.METRIC_NAME)
            .tag("client_version", ClientWriteClassifier.OTHER)
            .counter()
            .count(),
        1.0);
  }
}
