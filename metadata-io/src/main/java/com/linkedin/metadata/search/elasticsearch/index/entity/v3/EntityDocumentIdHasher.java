package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * Produces Elasticsearch/OpenSearch {@code _id} values for Search V3 entity documents.
 *
 * <p>OSS ships a URN-only SHA-256 implementation. An extension module may register a replacement
 * bean that also reads {@link OperationFingerprint#getEnrichment(Class)} without this interface
 * declaring which enrichment types exist.
 */
public interface EntityDocumentIdHasher {

  /**
   * Stable document id for {@code urn} under {@code operation}.
   *
   * @param operation fingerprint of the in-flight operation ({@link OperationFingerprint#EMPTY} on
   *     bootstrap / tests)
   * @param urn entity urn; never used as the raw {@code _id}
   * @return ES-safe document id; never {@code null}
   */
  @Nonnull
  String documentId(@Nonnull OperationFingerprint operation, @Nonnull Urn urn);
}
