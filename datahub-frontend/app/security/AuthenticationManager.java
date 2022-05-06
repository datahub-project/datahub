package security;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import play.Logger;

import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class AuthenticationManager {

  private AuthenticationManager() {

  }

  public static void authenticateUser(@Nonnull String userName, @Nonnull String password) throws NamingException {
    Preconditions.checkArgument(!StringUtils.isAnyEmpty(userName), "Username cannot be empty");
    try {
      LoginContext lc = new LoginContext("WHZ-Authentication", new WHZCallbackHandler(userName, password));
      lc.login();
    } catch (LoginException le) {
      throw new AuthenticationException(le.toString());
    }
  }

  private static class WHZCallbackHandler implements CallbackHandler {
    private String password;
    private String username;

    private WHZCallbackHandler(@Nonnull String username, @Nonnull String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(@Nonnull Callback[] callbacks) {
      NameCallback nc = null;
      PasswordCallback pc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
          nc.setName(this.username);
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
          pc.setPassword(this.password.toCharArray());
        } else {
          Logger.warn("The submitted callback is unsupported! ", callback);
        }
      }
    }
  }
}
