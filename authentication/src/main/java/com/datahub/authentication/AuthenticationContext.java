package com.datahub.authentication;

import java.util.Map;


/**
 * Context provided during authentication
 */
public interface AuthenticationContext {

  /**
   * Returns the headers associated with the inbound request
   */
  Map<String, String> headers();

}
