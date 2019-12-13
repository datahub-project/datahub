package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.snapshot.DatasetSnapshot;


/**
 * An action request builder for corp user info entities.
 */
public class DatasetActionRequestBuilder extends BaseActionRequestBuilder<DatasetSnapshot, DatasetUrn> {

  private static final String BASE_URI_TEMPLATE = "datasets";

  public DatasetActionRequestBuilder() {
    super(DatasetSnapshot.class, DatasetUrn.class, BASE_URI_TEMPLATE);
  }
}