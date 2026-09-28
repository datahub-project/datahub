package com.linkedin.metadata.config.usage.cigate;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HandlerInstrumentationSurfaceTest {

  @Test
  public void testUninstrumentedWithExemptionPasses() {
    HandlerInstrumentationSurface.HandlerEntry handler =
        new HandlerInstrumentationSurface.HandlerEntry("Demo.java", 10, false, null);
    HandlerInstrumentationSurface current = new HandlerInstrumentationSurface(List.of(handler));
    HandlerExemptionSnapshot exemptions =
        new HandlerExemptionSnapshot(
            List.of(new HandlerExemptionSnapshot.Exemption("Demo.java", 10, "internal admin API")));
    Assert.assertTrue(current.uninstrumentedWithoutExemption(exemptions, h -> false).isEmpty());
  }

  @Test
  public void testUninstrumentedWithoutExemptionFails() {
    HandlerInstrumentationSurface.HandlerEntry handler =
        new HandlerInstrumentationSurface.HandlerEntry("Demo.java", 10, false, null);
    HandlerInstrumentationSurface current = new HandlerInstrumentationSurface(List.of(handler));
    Assert.assertEquals(
        current.uninstrumentedWithoutExemption(HandlerExemptionSnapshot.empty(), h -> false).size(),
        1);
  }

  @Test
  public void testInstrumentedHandlerNeverFails() {
    HandlerInstrumentationSurface.HandlerEntry handler =
        new HandlerInstrumentationSurface.HandlerEntry("Demo.java", 10, true, "metadata_read");
    HandlerInstrumentationSurface current = new HandlerInstrumentationSurface(List.of(handler));
    Assert.assertTrue(
        current
            .uninstrumentedWithoutExemption(HandlerExemptionSnapshot.empty(), h -> false)
            .isEmpty());
  }
}
