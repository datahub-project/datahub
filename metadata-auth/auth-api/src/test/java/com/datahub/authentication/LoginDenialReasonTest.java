package com.datahub.authentication;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class LoginDenialReasonTest {

  @Test
  public void routineDenialsLogAtInfo() {
    assertFalse(LoginDenialReason.INVALID_CREDENTIALS.logsAtWarn());
    assertFalse(LoginDenialReason.HARD_DELETED.logsAtWarn());
    assertFalse(LoginDenialReason.NOT_PROVISIONED.logsAtWarn());
  }

  @Test
  public void ambiguousDenialsLogAtWarn() {
    assertTrue(LoginDenialReason.UNKNOWN.logsAtWarn());
    assertTrue(LoginDenialReason.SESSION_TOKEN_DENIED.logsAtWarn());
  }
}
