package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;


/**
 * An action request builder for dataflow entities.
 */
public class DataFlowActionRequestBuilder extends BaseActionRequestBuilder<DataFlowSnapshot, DataFlowUrn> {

  private static final String BASE_URI_TEMPLATE = "dataflows";

  public DataFlowActionRequestBuilder() {
    super(DataFlowSnapshot.class, DataFlowUrn.class, BASE_URI_TEMPLATE);
  }
}
