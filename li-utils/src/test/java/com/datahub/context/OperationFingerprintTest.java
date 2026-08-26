package com.datahub.context;

import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

public class OperationFingerprintTest {

  @Test
  public void testEmptyFingerprintIsNotSystemAuth() {
    // Shims and test fingerprints must be untrusted by default: only a real
    // OperationContext (which overrides isSystemAuth with the canonical check)
    // may claim system trust, so authorization gates fail closed everywhere else.
    assertFalse(OperationFingerprint.EMPTY.isSystemAuth());
  }
}
