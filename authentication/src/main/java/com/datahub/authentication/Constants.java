package com.datahub.authentication;

import java.util.Collections;


public class Constants {
  public static final AuthenticationResult FAILURE_AUTHENTICATION_RESULT = new AuthenticationResult(
      AuthenticationResult.Type.FAILURE,
      null,
      Collections.emptySet(),
      Collections.emptyMap()
  );

  private Constants() { }
}
