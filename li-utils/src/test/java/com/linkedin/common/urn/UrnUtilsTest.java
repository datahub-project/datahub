package com.linkedin.common.urn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class UrnUtilsTest {

  private static final String VALID_URN = "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)";

  @Test
  public void testRequireUrn_valid() throws Exception {
    Urn urn = UrnUtils.requireUrn(VALID_URN);
    assertEquals(urn.toString(), VALID_URN);
  }

  @Test
  public void testRequireUrn_empty() {
    assertThrows(IllegalArgumentException.class, () -> UrnUtils.requireUrn(""));
  }

  @Test
  public void testRequireUrn_blank() {
    assertThrows(IllegalArgumentException.class, () -> UrnUtils.requireUrn("   "));
  }

  @Test
  public void testRequireUrn_null() {
    assertThrows(IllegalArgumentException.class, () -> UrnUtils.requireUrn(null));
  }

  @Test
  public void testRequireUrn_invalid() {
    assertThrows(IllegalArgumentException.class, () -> UrnUtils.requireUrn("not-a-valid-urn"));
  }
}
