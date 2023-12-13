package security;

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * This LoginModule performs dummy authentication. Any username and password can work for
 * authentication
 */
public class DummyLoginModule implements LoginModule {

  public void initialize(
      final Subject subject,
      final CallbackHandler callbackHandler,
      final Map<String, ?> sharedState,
      final Map<String, ?> options) {}

  public boolean login() throws LoginException {
    return true;
  }

  public boolean commit() throws LoginException {
    return true;
  }

  public boolean abort() throws LoginException {
    return true;
  }

  public boolean logout() throws LoginException {
    return true;
  }
}
