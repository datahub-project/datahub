package com.datahub.authentication;

import java.util.Collections;


public class Constants {
  public static final String SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"; // DataHub internal service principal.

  public static final AuthenticationResult FAILURE_AUTHENTICATION_RESULT = new AuthenticationResult(
      AuthenticationResult.Type.FAILURE,
      new Authentication(
          "",
          null,
          null,
          Collections.emptySet(),
          Collections.emptyMap()
      )
  );

  private Constants() { }
}
