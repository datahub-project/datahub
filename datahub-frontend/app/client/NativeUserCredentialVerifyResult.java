package client;

import com.datahub.authentication.LoginDenialReason;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Result of GMS native password verification, including optional session eligibility denial. */
public final class NativeUserCredentialVerifyResult {
  private final boolean passwordMatches;
  @Nullable private final LoginDenialReason loginDenialReason;

  public NativeUserCredentialVerifyResult(
      final boolean passwordMatches, @Nullable final LoginDenialReason loginDenialReason) {
    this.passwordMatches = passwordMatches;
    this.loginDenialReason = loginDenialReason;
  }

  public boolean isPasswordMatches() {
    return passwordMatches;
  }

  @Nonnull
  public Optional<LoginDenialReason> getLoginDenialReason() {
    return Optional.ofNullable(loginDenialReason);
  }

  /** True when the user may receive a session (password ok and no eligibility denial from GMS). */
  public boolean allowsSessionCreation() {
    return passwordMatches && loginDenialReason == null;
  }
}
