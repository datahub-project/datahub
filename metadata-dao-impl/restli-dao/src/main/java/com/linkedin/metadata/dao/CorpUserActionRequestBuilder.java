package com.linkedin.metadata.dao;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;


/**
 * An action request builder for corp user info entities.
 */
public class CorpUserActionRequestBuilder extends BaseActionRequestBuilder<CorpUserSnapshot, CorpuserUrn> {

  private static final String BASE_URI_TEMPLATE = "corpUsers";

  public CorpUserActionRequestBuilder() {
    super(CorpUserSnapshot.class, CorpuserUrn.class, BASE_URI_TEMPLATE);
  }
}