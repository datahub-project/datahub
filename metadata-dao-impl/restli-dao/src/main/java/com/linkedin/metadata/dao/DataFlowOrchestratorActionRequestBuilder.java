
package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DataFlowOrchestratorUrn;
import com.linkedin.metadata.snapshot.DataFlowOrchestratorSnapshot;


/**
 * An action request builder for data flow orchestrator entities.
 */
public class DataFlowOrchestratorActionRequestBuilder extends BaseActionRequestBuilder<DataFlowOrchestratorSnapshot, DataFlowOrchestratorUrn> {

  private static final String BASE_URI_TEMPLATE = "dataFlowOrchestrators";

  public DataFlowOrchestratorActionRequestBuilder() {
    super(DataFlowOrchestratorSnapshot.class, DataFlowOrchestratorUrn.class, BASE_URI_TEMPLATE);
  }
}