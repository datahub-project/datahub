package com.datahub.authentication;

import lombok.Data;

@Data
/** Configurations for DataHub token service */
public class TokenServiceConfiguration {
  private String signingKey;
  private String salt;
  private String issuer;
  private String signingAlgorithm;
}
