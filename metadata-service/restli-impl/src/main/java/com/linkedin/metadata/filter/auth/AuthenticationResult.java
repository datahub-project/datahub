package com.linkedin.metadata.filter.auth;

import com.linkedin.metadata.filter.auth.Principal;


public interface AuthenticationResult {
  /**
   * The authenticated principal.
   */
  Principal principal();
}
