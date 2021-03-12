package com.linkedin.datajob.client;

import com.linkedin.data.template.StringArray;
import com.linkedin.datajob.DataFlowOrchestrator;
import com.linkedin.datajob.DataFlowOrchestratorInfo;
import com.linkedin.datajob.DataFlowOrchestratorsRequestBuilders;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;


public class DataFlowOrchestrators extends BaseClient {

  private static final DataFlowOrchestratorsRequestBuilders ORCHESTRATOR_REQUEST_BUILDERS = new DataFlowOrchestratorsRequestBuilders();

  public DataFlowOrchestrators(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Get orchestrator details by name
   * @param orchestratorName String
   * @return DataFlowOrchestratorInfo
   * @throws RemoteInvocationException
   */
  @Nonnull
  public DataFlowOrchestratorInfo getOrchestratorByName(@Nonnull String orchestratorName) throws RemoteInvocationException {
    final GetRequest<DataFlowOrchestrator> req = ORCHESTRATOR_REQUEST_BUILDERS.get()
        .id(orchestratorName)
        .aspectsParam(new StringArray(DataFlowOrchestratorInfo.class.getCanonicalName()))
        .build();
    return _client.sendRequest(req).getResponse().getEntity().getDataFlowOrchestratorInfo();
  }

  /**
   * Get all orchestrators
   * @return List<DataFlowOrchestratorInfo>
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<DataFlowOrchestrator> getAllOrchestrators() throws RemoteInvocationException {
    final GetAllRequest<DataFlowOrchestrator> req = ORCHESTRATOR_REQUEST_BUILDERS.getAll().build();
    return new ArrayList<>(_client.sendRequest(req)
            .getResponse()
            .getEntity()
            .getElements());
  }
}
