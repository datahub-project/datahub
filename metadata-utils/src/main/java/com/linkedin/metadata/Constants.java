package com.linkedin.metadata;

/**
 * Static class containing commonly-used constants across DataHub services.
 */
public class Constants {
  public static final String ACTOR_HEADER_NAME = "X-DataHub-Actor";
  public static final String DATAHUB_ACTOR = "urn:li:corpuser:datahub"; // Super user.
  public static final String SYSTEM_ACTOR = "urn:li:principal:datahub"; // DataHub internal service principal.
  public static final String UNKNOWN_ACTOR = "urn:li:principal:UNKNOWN"; // Unknown principal.
  public static final Long ASPECT_LATEST_VERSION = 0L;
  public static final String OWNERSHIP_ASPECT_NAME = "ownership";
  private Constants() { }
}
