package security;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationManager {
  private static final Logger log = LoggerFactory.getLogger(AuthenticationManager.class);

  private AuthenticationManager() {} // Prevent instantiation

  public static void authenticateJaasUser(@Nonnull String userName, @Nonnull String password)
      throws Exception {
    Preconditions.checkArgument(!StringUtils.isAnyEmpty(userName), "Username cannot be empty");

    try {
      // Create a login context with our custom callback handler
      LoginContext loginContext =
          new LoginContext("WHZ-Authentication", new WHZCallbackHandler(userName, password));

      // Attempt login
      loginContext.login();

      // If we get here, authentication succeeded
      log.debug("Authentication succeeded for user: {}", userName);

    } catch (LoginException le) {
      log.info("Authentication failed for user {}: {}", userName, le.getMessage());
      AuthenticationException authenticationException =
          new AuthenticationException(le.getMessage());
      authenticationException.setRootCause(le);
      throw authenticationException;
    }
  }

  private static class WHZCallbackHandler implements CallbackHandler {
    private final String password;
    private final String username;

    private WHZCallbackHandler(@Nonnull String username, @Nonnull String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(@Nonnull Callback[] callbacks) {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          nc.setName(username);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(password.toCharArray());
        }
      }
    }
  }
}
