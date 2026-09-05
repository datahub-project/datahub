package com.linkedin.metadata.config.flags;

import static org.testng.Assert.assertEquals;

import dev.openfeature.sdk.ImmutableContext;
import java.util.Map;
import org.testng.annotations.Test;

public class EnvironmentFeatureProviderTest {

  private static final ImmutableContext CTX = new ImmutableContext();
  private static final String KEY = "example.someFlag";

  private static EnvironmentFeatureProvider providerOf(final Map<String, String> properties) {
    return new EnvironmentFeatureProvider(properties::get);
  }

  @Test
  public void testConfiguredValuesAreServedByType() {
    assertEquals(
        providerOf(Map.of(KEY, "true")).getBooleanEvaluation(KEY, false, CTX).getValue(),
        Boolean.TRUE);
    assertEquals(
        providerOf(Map.of(KEY, "250")).getIntegerEvaluation(KEY, 10, CTX).getValue(),
        Integer.valueOf(250));
    assertEquals(
        providerOf(Map.of(KEY, "0.5")).getDoubleEvaluation(KEY, 1.0, CTX).getValue(),
        Double.valueOf(0.5));
    assertEquals(
        providerOf(Map.of(KEY, "text")).getStringEvaluation(KEY, "dflt", CTX).getValue(), "text");
  }

  @Test
  public void testUnsetPropertyFallsBackToCallerDefault() {
    final EnvironmentFeatureProvider provider = providerOf(Map.of());

    assertEquals(provider.getStringEvaluation(KEY, "fallback", CTX).getValue(), "fallback");
    assertEquals(provider.getBooleanEvaluation(KEY, true, CTX).getValue(), Boolean.TRUE);
    assertEquals(provider.getIntegerEvaluation(KEY, 7, CTX).getValue(), Integer.valueOf(7));
  }

  @Test
  public void testTargetingIsIgnored() {
    final EnvironmentFeatureProvider provider = providerOf(Map.of(KEY, "shared"));

    assertEquals(
        provider.getStringEvaluation(KEY, "dflt", new ImmutableContext("target-a")).getValue(),
        "shared");
    assertEquals(
        provider.getStringEvaluation(KEY, "dflt", new ImmutableContext("target-b")).getValue(),
        "shared");
  }

  /**
   * Records today's lenient behaviour rather than endorsing it: {@code "1"} is {@code true} to
   * Spring's converter and {@code false} here, so this test is what has to change when prerequisite
   * 3 on the class is implemented.
   */
  @Test
  public void testRelaxedBooleanSpellingsAreNotYetAccepted() {
    assertEquals(
        providerOf(Map.of(KEY, "1")).getBooleanEvaluation(KEY, false, CTX).getValue(),
        Boolean.FALSE);
    assertEquals(
        providerOf(Map.of(KEY, "ture")).getBooleanEvaluation(KEY, false, CTX).getValue(),
        Boolean.FALSE);
  }
}
