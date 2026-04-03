package com.linkedin.metadata.service.docimport;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * A document ready to be imported, regardless of source.
 *
 * <p>All import sources (GitHub, file upload, etc.) produce these as output. The
 * DocumentImportService consumes them to create Document entities.
 */
@Value
@Builder
public class DocumentCandidate {
  @Nonnull String title;
  @Nonnull String text;
  @Nonnull String sourceId;
  @Nonnull @Builder.Default Map<String, String> customProperties = Collections.emptyMap();

  /**
   * Source ID of this candidate's parent in the import hierarchy. Used by GitHub imports to
   * reconstruct folder structure. The import service resolves this to a real document URN during
   * import. Null means "use the user-specified root parent" (or no parent).
   */
  @Nullable String parentSourceId;
}
