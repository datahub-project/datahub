package com.linkedin.metadata.restli;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class RestliClientSslConfigTest {

  @Test
  public void emptyHasNoCustomMaterial() {
    RestliClientSslConfig c = RestliClientSslConfig.empty();
    assertFalse(c.hasCustomSslMaterial());
    assertNull(c.getTruststorePath());
    assertNull(c.getKeystorePath());
  }

  @Test
  public void fromNullableStringsTrimsAndTreatsBlankAsUnset() {
    RestliClientSslConfig c =
        RestliClientSslConfig.fromNullableStrings(
            "  /t.p12 ", "  p ", "  PKCS12 ", "  ", null, "  ", null);
    assertTrue(c.hasCustomSslMaterial());
    assertEquals(c.getTruststorePath(), "/t.p12");
    assertEquals(c.getTruststorePassword(), "p");
    assertEquals(c.getTruststoreType(), "PKCS12");
    assertNull(c.getKeystorePath());
  }

  @Test
  public void fromNullableStringsKeystoreOnly() {
    RestliClientSslConfig c =
        RestliClientSslConfig.fromNullableStrings(
            null, null, null, "/k.p12", "kp", "PKCS12", "keyp");
    assertTrue(c.hasCustomSslMaterial());
    assertNull(c.getTruststorePath());
    assertEquals(c.getKeystorePath(), "/k.p12");
    assertEquals(c.getKeyPassword(), "keyp");
  }

  @Test
  public void fromNullableStringsAllNullYieldsNoMaterial() {
    RestliClientSslConfig c =
        RestliClientSslConfig.fromNullableStrings(null, null, null, null, null, null, null);
    assertFalse(c.hasCustomSslMaterial());
  }
}
