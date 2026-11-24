package com.linkedin.metadata.search.transformer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Adds fields to one aspect's search document after the shared transformer has projected it. OSS
 * registers none.
 */
public interface SearchDocumentEnricher {
  void enrich(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate aspect,
      @Nonnull ObjectNode document,
      boolean forDelete);
}
