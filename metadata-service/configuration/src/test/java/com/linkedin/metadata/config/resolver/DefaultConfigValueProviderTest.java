package com.linkedin.metadata.config.resolver;

import static org.testng.Assert.assertEquals;

import dev.openfeature.sdk.Reason;
import dev.openfeature.sdk.Value;
import org.testng.annotations.Test;

public class DefaultConfigValueProviderTest {

  private final DefaultConfigValueProvider provider = new DefaultConfigValueProvider();

  @Test
  public void everyEvaluationReturnsTheDefaultValue() {
    assertEquals(provider.getBooleanEvaluation("key", true, null).getValue(), Boolean.TRUE);
    assertEquals(provider.getStringEvaluation("key", "value", null).getValue(), "value");
    assertEquals(provider.getIntegerEvaluation("key", 7, null).getValue(), Integer.valueOf(7));
    assertEquals(provider.getDoubleEvaluation("key", 1.5, null).getValue(), 1.5);
    assertEquals(
        provider.getObjectEvaluation("key", new Value("obj"), null).getValue().asString(), "obj");
    assertEquals(
        provider.getStringEvaluation("key", "value", null).getReason(), Reason.DEFAULT.name());
    assertEquals(provider.getMetadata().getName(), "DefaultConfigValueProvider");
  }
}
