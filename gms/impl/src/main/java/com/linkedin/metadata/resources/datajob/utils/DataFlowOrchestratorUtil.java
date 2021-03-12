package com.linkedin.metadata.resources.datajob.utils;

import com.linkedin.common.urn.DataFlowOrchestratorUrn;
import com.linkedin.datajob.DataFlowOrchestratorInfo;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.json.simple.parser.ParseException;


public class DataFlowOrchestratorUtil {

  private DataFlowOrchestratorUtil() {
  }

  private static final Map<DataFlowOrchestratorUrn, DataFlowOrchestratorInfo> DATA_FLOW_ORCHESTRATOR_MAP =
      loadAspectsFromResource("DataFlowOrchestrator.json");

  public static Map<DataFlowOrchestratorUrn, DataFlowOrchestratorInfo> getDataFlowOrchestratorInfoMap() {
    return DATA_FLOW_ORCHESTRATOR_MAP;
  }

  private static Map<DataFlowOrchestratorUrn, DataFlowOrchestratorInfo> loadAspectsFromResource(
      @Nonnull final String resource) {
    try {
      return ImmutableLocalDAO.loadAspects(DataFlowOrchestratorInfo.class,
          DataFlowOrchestratorUtil.class.getClassLoader().getResourceAsStream(resource));
    } catch (ParseException | IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isValidDataFlowOrchestrator(String orchestratorName) {
    return DATA_FLOW_ORCHESTRATOR_MAP.containsKey(toDataFlowOrchestratorUrn(orchestratorName));
  }

  @Nonnull
  public static Optional<DataFlowOrchestratorInfo> get(@Nonnull String orchestratorName) {
    return Optional.ofNullable(DATA_FLOW_ORCHESTRATOR_MAP.get(toDataFlowOrchestratorUrn(orchestratorName)));
  }

  private static DataFlowOrchestratorUrn toDataFlowOrchestratorUrn(@Nonnull String orchestratorName) {
    return new DataFlowOrchestratorUrn(orchestratorName);
  }
}
