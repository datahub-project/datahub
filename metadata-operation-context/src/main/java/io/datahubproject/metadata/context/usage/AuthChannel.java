package io.datahubproject.metadata.context.usage;

/** How the caller authenticated to GMS (landed as dimensions.auth_channel). */
public enum AuthChannel {
  SESSION,
  PAT,
  OAUTH,
  SYSTEM,
  ANONYMOUS,
  UNKNOWN;

  public String dimensionValue() {
    return name().toLowerCase();
  }
}
