package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * Additive hook for extra root fields on a V3 search document. OSS ships no implementations.
 * Extension modules register beans; {@code UpdateIndicesV3Strategy} merges them at write time.
 *
 * <p>Contributors run only when the strategy is emitting a real V3 document (searchable aspects
 * present). They must add new fields only; strategy-owned keys such as {@code urn}, {@code
 * _entityType}, {@code _aspects}, and {@code structuredProperties} are rejected.
 */
public interface V3SearchDocumentContributor {

  void contribute(
      @Nonnull OperationFingerprint operation, @Nonnull Urn urn, @Nonnull ObjectNode document);
}
