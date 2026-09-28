package controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

public class ProxyAdmissionTest {

  @Test
  void hysteresis_notReadyAtHighWater_readyAgainAtLowWater() {
    ProxyAdmission admission = new ProxyAdmission(10, null);
    assertEquals(9, admission.highWater());
    assertEquals(7, admission.lowWater());
    assertTrue(admission.isAcceptingTraffic());

    for (int i = 0; i < 8; i++) {
      assertTrue(admission.tryAcquire());
    }
    assertTrue(admission.isAcceptingTraffic());

    assertTrue(admission.tryAcquire());
    assertFalse(admission.isAcceptingTraffic());

    admission.release();
    assertEquals(8, admission.inFlight());
    assertFalse(admission.isAcceptingTraffic());

    admission.release();
    assertEquals(7, admission.inFlight());
    assertTrue(admission.isAcceptingTraffic());
  }

  @Test
  void tryAcquire_atCap_rejectsWithoutConsumingAnotherPermit() {
    ProxyAdmission admission = new ProxyAdmission(2, null);
    assertTrue(admission.tryAcquire());
    assertTrue(admission.tryAcquire());
    assertFalse(admission.tryAcquire());
    assertFalse(admission.isAcceptingTraffic());
    assertEquals(2, admission.inFlight());

    admission.release();
    admission.release();
    assertTrue(admission.isAcceptingTraffic());
    assertTrue(admission.tryAcquire());
  }

  @Test
  void admit_sharesOneBudgetAndReleasesAfterTheCall() {
    ProxyAdmission admission = new ProxyAdmission(1, null);
    AtomicInteger calls = new AtomicInteger();
    Result ok =
        ProxyAdmission.admit(
            admission,
            () -> {
              calls.incrementAndGet();
              return Results.ok("ok");
            });
    assertEquals(200, ok.status());
    assertEquals(1, calls.get());
    assertEquals(0, admission.inFlight());

    assertTrue(admission.tryAcquire());
    Result rejected =
        ProxyAdmission.admit(
            admission,
            () -> {
              calls.incrementAndGet();
              return Results.ok("should-not-run");
            });
    assertEquals(503, rejected.status());
    assertEquals("1", rejected.headers().get(Http.HeaderNames.RETRY_AFTER));
    assertEquals(1, calls.get());
    assertEquals(1, admission.inFlight());
  }

  @Test
  void resolveMaxInFlight_readsNumericStringAndFallsBackWhenInvalid() {
    assertEquals(32, ProxyAdmission.resolveMaxInFlight(configWithMax("32")));
    assertEquals(
        ProxyAdmission.DEFAULT_MAX_IN_FLIGHT,
        ProxyAdmission.resolveMaxInFlight(configWithMax("0")));
    assertEquals(
        ProxyAdmission.DEFAULT_MAX_IN_FLIGHT,
        ProxyAdmission.resolveMaxInFlight(configWithMax("nope")));
    assertEquals(
        ProxyAdmission.DEFAULT_MAX_IN_FLIGHT,
        ProxyAdmission.resolveMaxInFlight(ConfigFactory.parseMap(Map.of())));
  }

  private static Config configWithMax(String raw) {
    Map<String, Object> values = new HashMap<>();
    values.put(ProxyAdmission.CONFIG_PATH, raw);
    return ConfigFactory.parseMap(values);
  }
}
