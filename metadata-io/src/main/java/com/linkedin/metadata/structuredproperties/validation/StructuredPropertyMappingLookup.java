package com.linkedin.metadata.structuredproperties.validation;

import com.datahub.context.OperationFingerprint;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Reads the active search-index mappings to determine whether a structured-property field already
 * exists.
 *
 * <p>Implementations must query the search backend directly rather than use document search. This
 * check runs only during structured-property definition creation, so callers intentionally do not
 * cache the result.
 *
 * <p>Visibility is limited to what is already mapped: a definition that exists only in the primary
 * store (before MAE applies mappings, or when {@code ENABLE_STRUCTURED_PROPERTIES_HOOK=false}) is
 * not visible here.
 */
public interface StructuredPropertyMappingLookup {

  boolean fieldExists(
      @Nonnull OperationFingerprint operationContext, @Nonnull String elasticsearchFieldName)
      throws IOException;
}
