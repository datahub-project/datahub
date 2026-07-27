package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class InternalEvictionPolicyTest {

  @Test
  public void isPolicyMatchesCaseInsensitively() {
    assertTrue(InternalEvictionPolicy.MAX_SIZE.isPolicy("max_size"));
    assertTrue(InternalEvictionPolicy.SIZE_AND_TTL.isPolicy("Size_And_Ttl"));
    assertTrue(InternalEvictionPolicy.NEVER.isPolicy("never"));
    assertTrue(InternalEvictionPolicy.TTL.isPolicy("TTL"));
  }

  @Test
  public void isPolicyRejectsUnknownPolicy() {
    assertFalse(InternalEvictionPolicy.MAX_SIZE.isPolicy("UNKNOWN"));
  }
}
