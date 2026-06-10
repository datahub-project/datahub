package auth.sso.oidc;

/** Exception thrown when a user does not belong to any of the required groups for OIDC login. */
public class RequiredGroupsException extends RuntimeException {
  public RequiredGroupsException(String message) {
    super(message);
  }
}
