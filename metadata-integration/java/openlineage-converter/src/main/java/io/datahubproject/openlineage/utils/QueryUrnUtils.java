package io.datahubproject.openlineage.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Query URNs for statements captured from OpenLineage SQL facets. */
public class QueryUrnUtils {

  private QueryUrnUtils() {}

  /**
   * Deterministic query URN for a statement.
   *
   * <p>SHA-256 of the raw statement text, matching the Python SDK's {@code
   * datahub.sql_parsing.fingerprint_utils.generate_hash}, so the same statement emitted by this
   * listener and by the SDK collapses to one entity. Deliberately NOT the ingestion aggregator's
   * sqlglot-generalised fingerprint: cross-language parity with sqlglot is fragile. Consequence: a
   * statement that templates a literal per run yields one query per run.
   */
  public static Urn queryUrnForStatement(String statementText) {
    return UrnUtils.getUrn("urn:li:query:" + sha256Hex(statementText));
  }

  private static String sha256Hex(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16));
        hex.append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is mandated by the JDK spec; unreachable on any supported runtime.
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
