package com.linkedin.metadata.service.docimport;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** A document ready to be imported from the UI file-upload flow. */
@Value
@Builder
public class DocumentCandidate {
  @Nonnull String title;
  @Nonnull String text;
  @Nonnull String sourceId;
  @Nonnull @Builder.Default Map<String, String> customProperties = Collections.emptyMap();
}
