package client;

import com.datahub.authentication.LoginDenialReason;
import javax.annotation.Nullable;

/** GMS refused to issue a session token (e.g. disabled user). */
public class SessionTokenDeniedException extends RuntimeException {

  @Nullable private final LoginDenialReason loginDenialReason;

  public SessionTokenDeniedException(
      @Nullable final LoginDenialReason loginDenialReason, @Nullable final Throwable cause) {
    super("Session token denied by Metadata Service", cause);
    this.loginDenialReason = loginDenialReason;
  }

  @Nullable
  public LoginDenialReason getLoginDenialReason() {
    return loginDenialReason;
  }
}
