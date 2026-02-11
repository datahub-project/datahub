package com.linkedin.metadata.search.rescore.functions;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

/** Tests for division-by-zero guards in custom exp4j functions. */
public class CustomFunctionsTest {

  @Test
  public void testSigmoidZeroOrNegativeCapReturnsMin() {
    SigmoidFunction sigmoid = new SigmoidFunction();

    // Zero cap should return min (guards against division by zero)
    assertEquals(sigmoid.apply(100.0, 0.0, 1.0, 2.0), 1.0);

    // Negative cap should also return min
    assertEquals(sigmoid.apply(100.0, -50.0, 1.0, 2.0), 1.0);
  }

  @Test
  public void testLinearDecayZeroOrNegativeScaleReturnsMin() {
    LinearDecayFunction decay = new LinearDecayFunction();

    // Zero scale should return min (guards against division by zero)
    assertEquals(decay.apply(10.0, 0.0, 0.7, 1.2), 0.7);

    // Negative scale should also return min
    assertEquals(decay.apply(10.0, -100.0, 0.7, 1.2), 0.7);
  }
}
