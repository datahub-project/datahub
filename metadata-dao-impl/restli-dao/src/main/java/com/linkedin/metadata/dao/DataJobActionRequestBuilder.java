package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.metadata.snapshot.DataJobSnapshot;


/**
 * An action request builder for datajob entities.
 */
public class DataJobActionRequestBuilder extends BaseActionRequestBuilder<DataJobSnapshot, DataJobUrn> {

  private static final String BASE_URI_TEMPLATE = "datajobs";

  public DataJobActionRequestBuilder() {
    super(DataJobSnapshot.class, DataJobUrn.class, BASE_URI_TEMPLATE);
  }
}
