package com.datahub.authentication;

/** Stable machine-readable reasons for login / session token denial (logs and service JSON). */
public enum LoginDenialReason {
  /**
   * No {@code corpUserKey} row in metadata. Does not distinguish never-provisioned from hard-purged
   * users. (Name is historical; not proof the user was hard-deleted.)
   */
  HARD_DELETED,
  SOFT_DELETED,
  SUSPENDED,
  INACTIVE,
  INVALID_CREDENTIALS,
  /**
   * Corp user key exists but the user is not sufficiently provisioned for login (e.g. key-only or
   * missing corp profile aspects). Implies the URN has at least a key record in metadata.
   */
  NOT_PROVISIONED,
  /** GMS refused a session token; used when the auth API omits a structured reason. */
  SESSION_TOKEN_DENIED,
  /** JSON contained a {@code loginDenialReason} string that does not match this enum. */
  UNKNOWN;

  /**
   * When true, login-denial audit lines use WARN (unexpected API shape, version skew, or generic
   * GMS refusal). Routine user/account outcomes use INFO.
   */
  public boolean logsAtWarn() {
    return this == UNKNOWN || this == SESSION_TOKEN_DENIED;
  }
}
