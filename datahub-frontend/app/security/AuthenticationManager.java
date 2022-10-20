package security;

import com.google.common.base.Preconditions;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.jaas.PropertyUserStoreManager;
import play.Logger;


public class AuthenticationManager {

  private AuthenticationManager() {

  }

  public static void authenticateJaasUser(@Nonnull String userName, @Nonnull String password) throws Exception {
    Preconditions.checkArgument(!StringUtils.isAnyEmpty(userName), "Username cannot be empty");
    JAASLoginService jaasLoginService = new JAASLoginService("WHZ-Authentication");
    PropertyUserStoreManager propertyUserStoreManager = new PropertyUserStoreManager();
    propertyUserStoreManager.start();
    jaasLoginService.setBeans(Collections.singletonList(propertyUserStoreManager));
    JAASLoginService.INSTANCE.set(jaasLoginService);
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
        Logger.error("The submitted callback is of type: " + callback.getClass() + " : " + callback);
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
