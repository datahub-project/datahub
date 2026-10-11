package utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Nonnull;
import org.pac4j.core.exception.http.FoundAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes/decodes the redirect URL for the OIDC post-login redirect cookie.
 *
 * <p>The decoded URL is validated to be a relative path (no scheme, no authority, no
 * protocol-relative prefix) to prevent open-redirect attacks via cookie tampering and avoids direct
 * object serialization.
 */
public class SerializationUtils {

  private static final Logger log = LoggerFactory.getLogger(SerializationUtils.class);
  private static final FoundAction DEFAULT_REDIRECT = new FoundAction("/");

  private SerializationUtils() {}

  /** Encodes a {@link FoundAction}'s redirect URL as a Base64 string safe for cookie storage. */
  public static String serializeFoundAction(@Nonnull final FoundAction foundAction) {
    return Base64.getEncoder()
        .encodeToString(foundAction.getLocation().getBytes(StandardCharsets.UTF_8));
  }

  /** Decodes a Base64-encoded redirect URL and wraps it in a {@link FoundAction}. */
  public static FoundAction deserializeFoundAction(@Nonnull final String serialized) {
    try {
      String location = new String(Base64.getDecoder().decode(serialized), StandardCharsets.UTF_8);
      if (!isRelativeUrl(location)) {
        log.warn("Rejected non-relative redirect URL from cookie");
        return DEFAULT_REDIRECT;
      }
      return new FoundAction(location);
    } catch (IllegalArgumentException e) {
      log.warn("Failed to decode redirect URL cookie, using default redirect", e);
      return DEFAULT_REDIRECT;
    }
  }

  /**
   * Returns true only for relative URLs (no scheme, no authority, no protocol-relative prefix).
   * Mirrors the validation in {@code AuthenticationController.authenticate()}.
   */
  static boolean isRelativeUrl(@Nonnull final String url) {
    if (url.trim().startsWith("//")) {
      return false;
    }
    try {
      URI uri = new URI(url);
      return uri.getScheme() == null && uri.getAuthority() == null;
    } catch (URISyntaxException e) {
      return false;
    }
  }
}
