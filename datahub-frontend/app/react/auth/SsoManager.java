package react.auth;

import javax.annotation.Nonnull;


/**
 * Singleton class that stores & serves reference to a single {@link SsoProvider} if one exists.
 *
 * This object serves the SSO client logic during both authentication and upon a callback
 * to DataHub post authentication with a third-party Identity Provider.
 */
public class SsoManager {

  // Singleton instance of the SsoManager.
  private static final SsoManager _instance = new SsoManager();

  // The active SSO provider. There can only be 1 registered.
  private SsoProvider<?> _provider;

  public SsoManager() { }

  /**
   * Returns a singleton instance of the {@link SsoManager} serving as the source
   * of truth for SSO related logic, but on the authentication and callback paths.
   */
  public static SsoManager instance() {
    return _instance;
  }

  /**
   * Returns true if SSO is enabled, meaning a non-null {@link SsoProvider} has been
   * provided to the manager.
   *
   * @return true if SSO logic should be enabled, false otherwise.
   */
  public boolean isSsoEnabled() {
    return _provider != null;
  }

  /**
   * Sets or replace a SsoProvider.
   *
   * @param provider the new {@link SsoProvider} to be used during authentication.
   */
  public void setSsoProvider(@Nonnull final SsoProvider provider) {
    _provider = provider;
  }

  /**
   * Gets the active {@link SsoProvider} instance.
   *
   * @return the {@SsoProvider} that should be used during authentication and on
   * IdP callback, or null if SSO is not enabled.
   */
  public SsoProvider getSsoProvider() {
    return _provider;
  }
}
