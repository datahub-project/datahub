package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.index.query.QueryBuilders;

/**
 * Documents of one entity type in a V3 index. A V3 index can hold several entity types, so a V2
 * index is compared with the documents of its own type, not with the whole V3 index. The index is
 * only the target of the request; the entity type comes from the caller.
 */
@Slf4j
public final class EntityTypeDocumentCount {

  private EntityTypeDocumentCount() {}

  /** Document count, or null when the count cannot be read. */
  @Nullable
  public static Long count(
      @Nonnull SearchClientShim<?> client, @Nonnull String indexName, @Nonnull String entityType) {
    try {
      return client
          .count(
              OperationFingerprint.EMPTY,
              new CountRequest(indexName)
                  .query(QueryBuilders.termQuery(MappingConstants.ENTITY_TYPE_FIELD, entityType)),
              RequestOptions.DEFAULT)
          .getCount();
    } catch (IOException | RuntimeException e) {
      log.warn("Cannot count {} documents in index {}", entityType, indexName, e);
      return null;
    }
  }
}
