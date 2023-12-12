package auth.sso;

import javax.annotation.Nonnull;

/** Singleton class that stores & serves reference to a single {@link SsoProvider} if one exists. */
public class SsoManager {

  private SsoProvider<?> _provider; // Only one active provider at a time.

  public SsoManager() {}

  /**
   * Returns true if SSO is enabled, meaning a non-null {@link SsoProvider} has been provided to the
   * manager.
   *
   * @return true if SSO logic is enabled, false otherwise.
   */
  public boolean isSsoEnabled() {
    return _provider != null;
  }

  /**
   * Sets or replace a SsoProvider.
   *
   * @param provider the new {@link SsoProvider} to be used during authentication.
   */
  public void setSsoProvider(@Nonnull final SsoProvider<?> provider) {
    _provider = provider;
  }

  /**
   * Gets the active {@link SsoProvider} instance.
   *
   * @return the {@SsoProvider} that should be used during authentication and on IdP callback, or
   *     null if SSO is not enabled.
   */
  public SsoProvider<?> getSsoProvider() {
    return _provider;
  }
}
