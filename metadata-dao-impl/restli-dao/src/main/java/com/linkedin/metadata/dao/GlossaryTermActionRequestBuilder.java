package com.linkedin.metadata.dao;

import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;


/**
 * An action request builder for corp user info entities.
 */
public class GlossaryTermActionRequestBuilder extends BaseActionRequestBuilder<GlossaryTermSnapshot, GlossaryTermUrn> {

  private static final String BASE_URI_TEMPLATE = "glossaryTerms";

  public GlossaryTermActionRequestBuilder() {
    super(GlossaryTermSnapshot.class, GlossaryTermUrn.class, BASE_URI_TEMPLATE);
  }
}