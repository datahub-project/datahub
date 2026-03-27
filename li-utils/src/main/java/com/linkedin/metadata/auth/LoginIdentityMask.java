package com.linkedin.metadata.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Masks user identifiers (username, email-shaped id, corpuser URN string) for log lines so
 * operators can correlate events without emitting raw PII at INFO.
 */
public final class LoginIdentityMask {

  private static final int PREFIX_HEX_CHARS = 12;

  private LoginIdentityMask() {}

  /**
   * Returns a stable opaque token derived from SHA-256 of the normalized input, e.g. {@code
   * sha256:a1b2c3d4e5f6}.
   */
  @Nonnull
  public static String mask(@Nullable final String userIdOrUrn) {
    if (userIdOrUrn == null || userIdOrUrn.isEmpty()) {
      return "";
    }
    final String normalized = userIdOrUrn.trim().toLowerCase();
    try {
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      final byte[] digest = md.digest(normalized.getBytes(StandardCharsets.UTF_8));
      final StringBuilder sb = new StringBuilder(PREFIX_HEX_CHARS + 8);
      sb.append("sha256:");
      for (int i = 0; i < Math.min(PREFIX_HEX_CHARS / 2, digest.length); i++) {
        sb.append(String.format("%02x", digest[i]));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      return "sha256:error";
    }
  }
}
