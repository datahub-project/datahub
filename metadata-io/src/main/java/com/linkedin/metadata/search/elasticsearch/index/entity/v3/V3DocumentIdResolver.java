package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Maps explain / lookup document ids onto V3 hashed {@code _id}s when the caller still passes a raw
 * URN or a V2 URL-encoded URN.
 */
public final class V3DocumentIdResolver {

  private static final String URN_PREFIX = "urn:";

  private V3DocumentIdResolver() {}

  /**
   * If {@code documentId} is a URN or URL-encoded URN, return {@link
   * EntityDocumentIdHasher#documentId}. Otherwise treat it as already a V3 id (SHA-256 hex) and
   * return it unchanged.
   */
  @Nonnull
  public static String resolveExplainDocumentId(
      @Nonnull OperationFingerprint operation,
      @Nonnull EntityDocumentIdHasher hasher,
      @Nonnull String documentId) {
    return parseUrnOrUrlEncodedUrn(documentId)
        .map(urn -> hasher.documentId(operation, urn))
        .orElse(documentId);
  }

  static Optional<Urn> parseUrnOrUrlEncodedUrn(@Nonnull String documentId) {
    Optional<String> urnString = asUrnString(documentId);
    if (urnString.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Urn.createFromString(urnString.get()));
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }

  private static Optional<String> asUrnString(@Nonnull String documentId) {
    if (documentId.startsWith(URN_PREFIX)) {
      return Optional.of(documentId);
    }
    String decoded = URLDecoder.decode(documentId, StandardCharsets.UTF_8);
    if (decoded.startsWith(URN_PREFIX) && !decoded.equals(documentId)) {
      return Optional.of(decoded);
    }
    return Optional.empty();
  }
}
