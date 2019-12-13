package com.linkedin.metadata.dao;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;


/**
 * An action request builder for corp group info entities.
 */
public class CorpGroupActionRequestBuilder extends BaseActionRequestBuilder<CorpGroupSnapshot, CorpGroupUrn> {

  private static final String BASE_URI_TEMPLATE = "corpGroups";

  public CorpGroupActionRequestBuilder() {
    super(CorpGroupSnapshot.class, CorpGroupUrn.class, BASE_URI_TEMPLATE);
  }
}