package security;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.security.UserPrincipal;
import org.eclipse.jetty.util.security.Credential;

public class AuthenticationManager {
  private AuthenticationManager() {} // Prevent instantiation

  public static void authenticateJaasUser(@Nonnull String userName, @Nonnull String password)
      throws Exception {
    Preconditions.checkArgument(!StringUtils.isAnyEmpty(userName), "Username cannot be empty");

    try {
      // Create and configure credentials for authentication
      UserPrincipal userPrincipal = new UserPrincipal(userName, Credential.getCredential(password));

      // Verify credentials
      if (!userPrincipal.authenticate(password)) {
        throw new AuthenticationException("Invalid credentials for user: " + userName);
      }

    } catch (Exception e) {
      AuthenticationException authenticationException =
          new AuthenticationException("Authentication failed");
      authenticationException.setRootCause(e);
      throw authenticationException;
    }
  }
}
