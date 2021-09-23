package com.linkedin.metadata.dao;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.metadata.snapshot.GlossaryNodeSnapshot;


/**
 * An action request builder for business node info entities.
 */
public class GlossaryNodeActionRequestBuilder extends BaseActionRequestBuilder<GlossaryNodeSnapshot, GlossaryNodeUrn> {

  private static final String BASE_URI_TEMPLATE = "glossaryNodes";

  public GlossaryNodeActionRequestBuilder() {
    super(GlossaryNodeSnapshot.class, GlossaryNodeUrn.class, BASE_URI_TEMPLATE);
  }
}