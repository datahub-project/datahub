package com.linkedin.metadata.resources.datafloworchestrator;

import com.linkedin.common.urn.DataFlowOrchestratorUrn;
import com.linkedin.datajob.DataFlowOrchestrator;
import com.linkedin.datajob.DataFlowOrchestratorInfo;
import com.linkedin.metadata.aspect.DataFlowOrchestratorAspect;
import com.linkedin.metadata.aspect.DataFlowOrchestratorAspectArray;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.restli.BaseEntityResource;
import com.linkedin.metadata.snapshot.DataFlowOrchestratorSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * Resource provides information about various data flow orchestrators.
 */
@RestLiCollection(name = "dataFlowOrchestrators", namespace = "com.linkedin.datajob", keyName = "orchestratorName")
public class DataFlowOrchestrators extends BaseEntityResource<
    // @formatter:off
    String,
    DataFlowOrchestrator,
    DataFlowOrchestratorUrn,
    DataFlowOrchestratorSnapshot,
    DataFlowOrchestratorAspect> {
  // @formatter:on

  public DataFlowOrchestrators() {
    super(DataFlowOrchestratorSnapshot.class, DataFlowOrchestratorAspect.class);
  }

  @Inject
  @Named("dataFlowOrchestratorLocalDAO")
  private BaseLocalDAO<DataFlowOrchestratorAspect, DataFlowOrchestratorUrn> _localDAO;

  /**
   * Get orchestrator.
   *
   * @param orchestratorName name of the orchestrator.
   * @param aspectNames list of aspects to be retrieved. Null to retrieve all aspects of the dataFlowOrchestrators.
   * @return {@link DataFlowOrchestrator} orchestrator value.
   */
  @Nonnull
  @Override
  @RestMethod.Get
  public Task<DataFlowOrchestrator> get(@Nonnull String orchestratorName,
      @QueryParam(PARAM_ASPECTS) @Nullable String[] aspectNames) {
    return super.get(orchestratorName, aspectNames);
  }

  /**
   * Get all data orchestrators.
   *
   * @param pagingContext paging context used for paginating through the results.
   * @return list of all data flow orchestrators.
   */
  @RestMethod.GetAll
  public Task<List<DataFlowOrchestrator>> getAllDataFlowOrchestrators(
      @Nonnull @PagingContextParam(defaultCount = 100) PagingContext pagingContext) {
    return Task.value(_localDAO.list(DataFlowOrchestratorInfo.class, pagingContext.getStart(), pagingContext.getCount())
            .getValues()
            .stream()
            .map(info -> {
              final DataFlowOrchestrator orchestrator = new DataFlowOrchestrator();
              orchestrator.setDataFlowOrchestratorInfo(info);
              orchestrator.setName(info.getName());
              return orchestrator;
            })
            .collect(Collectors.toList())
    );
  }

  /**
   * Get the snapshot of data orchestrator.
   *
   * @param urnString data orchestrator urn.
   * @param aspectNames list of aspects to be returned. null, when all aspects are to be returned.
   * @return snapshot of data orchestrator with the requested aspects.
   */
  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DataFlowOrchestratorSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.getSnapshot(urnString, aspectNames);
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DataFlowOrchestratorAspect, DataFlowOrchestratorUrn> getLocalDAO() {
    return _localDAO;
  }

  @Nonnull
  @Override
  protected DataFlowOrchestratorUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DataFlowOrchestratorUrn.deserialize(urnString);
  }

  @Nonnull
  @Override
  protected DataFlowOrchestratorUrn toUrn(@Nonnull String orchestratorName) {
    return new DataFlowOrchestratorUrn(orchestratorName);
  }

  @Nonnull
  @Override
  protected String toKey(@Nonnull DataFlowOrchestratorUrn urn) {
    return urn.getOrchestratorNameEntity();
  }

  @Nonnull
  @Override
  protected DataFlowOrchestrator toValue(@Nonnull DataFlowOrchestratorSnapshot dataFlowOrchestratorSnapshot) {
    final DataFlowOrchestrator dataFlowOrchestrator = new DataFlowOrchestrator();
    dataFlowOrchestrator.setName(dataFlowOrchestratorSnapshot.getUrn().getOrchestratorNameEntity());
    ModelUtils.getAspectsFromSnapshot(dataFlowOrchestratorSnapshot).forEach(aspect -> {
      if (aspect instanceof DataFlowOrchestratorInfo) {
        dataFlowOrchestrator.setDataFlowOrchestratorInfo((DataFlowOrchestratorInfo) aspect);
      }
    });

    return dataFlowOrchestrator;
  }

  @Nonnull
  @Override
  protected DataFlowOrchestratorSnapshot toSnapshot(@Nonnull DataFlowOrchestrator dataFlowOrchestrator, @Nonnull DataFlowOrchestratorUrn urn) {
    final DataFlowOrchestratorSnapshot dataFlowOrchestratorSnapshot = new DataFlowOrchestratorSnapshot();
    final DataFlowOrchestratorAspectArray aspects = new DataFlowOrchestratorAspectArray();
    dataFlowOrchestratorSnapshot.setUrn(urn);
    dataFlowOrchestratorSnapshot.setAspects(aspects);
    if (dataFlowOrchestrator.getDataFlowOrchestratorInfo() != null) {
      aspects.add(ModelUtils.newAspectUnion(DataFlowOrchestratorAspect.class, dataFlowOrchestrator.getDataFlowOrchestratorInfo()));
    }
    return dataFlowOrchestratorSnapshot;
  }
}
