package com.linkedin.gms.factory.clientwrite;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
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
      new ClientWrite("restli", "python", "ingestion", "1.7.0", "DataHub-Client/1.0");

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
    return new ClientWriteRecorder(registry, true, maxTrackedKeys, 20, clock::get, summaries::add);
  }

  private static ClientWriteTrackingConfiguration configuration(
      boolean enabled, int summaryIntervalSeconds, int maxTrackedKeys) {
    ClientWriteTrackingConfiguration configuration = new ClientWriteTrackingConfiguration();
    configuration.setEnabled(enabled);
    configuration.setSummaryIntervalSeconds(summaryIntervalSeconds);
    configuration.setMaxTrackedKeys(maxTrackedKeys);
    return configuration;
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
                "client_mode", "ingestion",
                "client_version", "1.7.0",
                "api", "restli",
                "system_actor", "false")
            .counter();
    assertEquals(counter.count(), 2.0);
    assertTrue(
        registry.getMeters().stream().allMatch(meter -> meter.getId().getTag("actor") == null));
  }

  @Test
  public void testFlushAttributesWritesPerActorAndRecordNeverLogs() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);
    recorder.record(PYTHON_CLI, null, false);
    clock.set(Duration.ofHours(1).toMillis());
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_loader", false);
    assertTrue(summaries.isEmpty(), "record() must not write the summary");

    recorder.flush();

    assertEquals(summaries.size(), 2);
    String custom =
        summaries.stream().filter(s -> s.contains("svc_loader")).findFirst().orElseThrow();
    assertTrue(custom.startsWith(ClientWriteRecorder.SUMMARY_PREFIX));
    assertTrue(custom.contains("client=unknown"));
    assertTrue(custom.contains("count=3"));
    assertTrue(custom.contains("window_seconds=3600"));
    assertTrue(custom.contains("user_agent=\"curl/8.4.0\""));
    assertTrue(summaries.stream().anyMatch(s -> s.contains("actor=anonymous")));

    // Counts restart after a flush, and an empty window writes nothing.
    summaries.clear();
    recorder.flush();
    assertTrue(summaries.isEmpty());
    recorder.record(PYTHON_CLI, null, false);
    recorder.flush();
    assertEquals(summaries.size(), 1);
    assertTrue(summaries.get(0).contains("count=1"));
  }

  @Test
  public void testCloseFlushesTheFinalWindowOnce() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.record(PYTHON_CLI, "urn:li:corpuser:a", false);

    recorder.close();
    recorder.close();

    assertEquals(summaries.size(), 1);
    assertTrue(summaries.get(0).contains("actor=urn:li:corpuser:a"));
  }

  @Test
  public void testDisabledSummaryStillCountsTheMetric() {
    ClientWriteRecorder recorder =
        new ClientWriteRecorder(registry, false, 100, 20, clock::get, summaries::add);
    recorder.record(PYTHON_CLI, "urn:li:corpuser:a", false);
    recorder.flush();

    assertTrue(summaries.isEmpty());
    assertEquals(registry.get(ClientWriteRecorder.METRIC_NAME).counter().count(), 1.0);
  }

  @Test
  public void testCombinationsBeyondTheCapAreReportedAsUntracked() {
    ClientWriteRecorder recorder = recorder(1);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:a", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:b", false);
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:c", false);

    recorder.flush();

    assertTrue(summaries.stream().anyMatch(s -> s.contains("untracked=2")));
    assertEquals(
        registry.get(ClientWriteRecorder.METRIC_NAME).counter().count(),
        3.0,
        "the metric counts every write, tracked or not");
  }

  @Test
  public void testSystemActorIsTagged() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.record(PYTHON_CLI, "urn:li:corpuser:__datahub_system", true);

    assertEquals(
        registry.get(ClientWriteRecorder.METRIC_NAME).tag("system_actor", "true").counter().count(),
        1.0);
  }

  @Test
  public void testVaryingUserAgentsDoNotExhaustTrackedKeys() {
    ClientWriteRecorder recorder = recorder(2);
    for (int i = 0; i < 50; i++) {
      recorder.record(
          new ClientWrite("openapi", "unknown", "none", "none", "custom-agent/" + i),
          "urn:li:corpuser:svc_rotating",
          false);
    }
    recorder.record(CUSTOM_CODE, "urn:li:corpuser:svc_other", false);

    recorder.flush();

    assertEquals(summaries.size(), 2, String.join("\n", summaries));
    String rotating =
        summaries.stream().filter(s -> s.contains("svc_rotating")).findFirst().orElseThrow();
    assertTrue(rotating.contains("count=50"));
    assertTrue(rotating.contains("user_agent=\"custom-agent/0\""), "keeps the first User-Agent");
    assertTrue(summaries.stream().anyMatch(s -> s.contains("actor=urn:li:corpuser:svc_other")));
    assertTrue(summaries.stream().noneMatch(s -> s.contains("untracked")));
  }

  @Test
  public void testWritesAfterCloseCountInTheMetricOnly() {
    ClientWriteRecorder recorder = recorder(100);
    recorder.close();
    recorder.record(PYTHON_CLI, "urn:li:corpuser:a", false);
    recorder.flush();

    assertTrue(summaries.isEmpty());
    assertEquals(registry.get(ClientWriteRecorder.METRIC_NAME).counter().count(), 1.0);
  }

  @Test
  public void testConfigurationStartsTheSummaryThreadOnlyWhenTheSummaryIsOn() {
    ClientWriteRecorder scheduled =
        new ClientWriteRecorder(registry, configuration(true, 3600, 100));
    try {
      assertTrue(scheduled.isSummaryScheduled());
    } finally {
      scheduled.close();
    }

    for (ClientWriteTrackingConfiguration off :
        List.of(
            configuration(true, 0, 100),
            configuration(true, -1, 100),
            configuration(true, 3600, 0),
            configuration(true, 3600, -5),
            configuration(false, 3600, 100))) {
      ClientWriteRecorder recorder = new ClientWriteRecorder(registry, off);
      try {
        assertFalse(recorder.isSummaryScheduled(), off.toString());
      } finally {
        recorder.close();
      }
    }
  }

  @Test
  public void testVersionTagValuesAreCapped() {
    ClientWriteRecorder recorder =
        new ClientWriteRecorder(registry, true, 100, 2, clock::get, summaries::add);
    for (String version : List.of("1.7.0", "1.8.0", "1.9.0", "1.7.0")) {
      recorder.record(new ClientWrite("restli", "python", "sdk", version, "ua"), null, false);
    }
    assertEquals(
        registry
            .get(ClientWriteRecorder.METRIC_NAME)
            .tag("client_version", "1.7.0")
            .counter()
            .count(),
        2.0,
        "a version seen before the cap was reached keeps its tag");
    assertEquals(
        registry
            .get(ClientWriteRecorder.METRIC_NAME)
            .tag("client_version", ClientWriteClassifier.OTHER)
            .counter()
            .count(),
        1.0);
  }
}
