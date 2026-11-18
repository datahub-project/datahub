package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Stored search-template aspects read from the entity index. A URN that is absent was not in the
 * index. A document whose properties are null does not yet carry the template body. {@code
 * modifiedAt} is that aspect's {@code systemMetadata.aspectModified} time, or {@code aspectCreated}
 * when the row has no later write.
 */
public interface SearchTemplateDocuments {
  record Document(@Nullable SearchTemplateProperties properties, long modifiedAt) {}

  @Nonnull
  Map<String, Document> read(@Nonnull OperationContext opContext, @Nonnull Collection<String> urns);
}
