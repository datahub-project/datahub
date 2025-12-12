package com.datahub.authorization;

import java.util.Map;
import lombok.Value;

@Value
public class BatchAuthorizationResult {
  /** The original batch authorization request */
  BatchAuthorizationRequest request;

  /**
   * Results per individual privilege. The {@link Map} MUST support only {@link Map#get} and {@link
   * Map#containsKey} methods. Other methods may or may not behave correctly
   */
  Map<String, AuthorizationResult> results;
}
