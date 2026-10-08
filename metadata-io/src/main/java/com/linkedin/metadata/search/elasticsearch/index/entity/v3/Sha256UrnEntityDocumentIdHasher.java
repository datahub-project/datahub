package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Default V3 document-id hasher: SHA-256 hex of {@code urn.toString()}. The operation fingerprint
 * is ignored so MAE, GMS, and {@link OperationFingerprint#EMPTY} produce the same id for the same
 * URN.
 */
public class Sha256UrnEntityDocumentIdHasher implements EntityDocumentIdHasher {

  @Nonnull
  @Override
  public String documentId(@Nonnull OperationFingerprint operation, @Nonnull Urn urn) {
    return DigestUtils.sha256Hex(urn.toString());
  }
}
