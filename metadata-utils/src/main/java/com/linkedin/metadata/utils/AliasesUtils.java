package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import java.util.Locale;

public class AliasesUtils {

  private AliasesUtils() {}

  /**
   * The case-insensitive lookup key for an entity URN: the whole URN lowercased. Entity-agnostic by
   * design — one rule covers every entity type, including nested URNs such as the dataset embedded
   * in a schemaField URN.
   *
   * <p>The result is a key, not a URN. Every component is lowercased, so the platform no longer
   * names an existing dataPlatform entity and the FabricType is not a valid enum symbol.
   *
   * <p>Ingestion derives this key client-side to look entities up by it, so the derivation is a
   * wire contract: a key computed one way finds nothing indexed under a key computed the other, and
   * the mismatch surfaces as zero hits rather than an error. {@code Locale.ROOT} keeps the result
   * independent of the JVM's default locale so it matches Python's {@code str.lower()}.
   */
  public static String lowercasedUrnKey(Urn urn) {
    return urn.toString().toLowerCase(Locale.ROOT);
  }
}
