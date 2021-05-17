package com.linkedin.metadata.resources.datajob;

import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.datajob.DataJobsLineage;
import com.linkedin.datajob.DataFlowKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseQueryDAO;
import com.linkedin.metadata.entity.DataJobEntity;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.relationship.IsPartOf;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.Neo4jUtil.createRelationshipFilter;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


/**
 * Rest.li entry point: /dataFlows/{dataFlowKey}/dataJobs
 */
@RestLiSimpleResource(name = "dataJobs", namespace = "com.linkedin.datajob", parent = DataFlows.class)
public final class DataJobsResource extends SimpleResourceTemplate<DataJobsLineage> {

  private static final String DATAFLOW_KEY = DataFlows.class.getAnnotation(RestLiCollection.class).keyName();
  private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
  private static final Integer MAX_JOBS_CNT = 100;

  @Inject
  @Named("datasetDao")
  private BaseLocalDAO _localDAO;

  @Inject
  @Named("datasetQueryDao")
  private BaseQueryDAO _queryDAO;

  public DataJobsResource() {
    super();
  }

  @Nonnull
  @RestMethod.Get
  public Task<DataJobsLineage> get(@PathKeysParam @Nonnull PathKeys keys) {
    final DataFlowUrn dataFlowUrn = getUrn(keys);

    return RestliUtils.toTask(() -> {
      System.out.println("DataJobsResource----: " + dataFlowUrn.toString());
      final List<DataJobUrn> dataJobUrns = _queryDAO
          .findEntities(DataJobEntity.class, newFilter("dataFlow", dataFlowUrn.toString()),
              DataJobEntity.class, EMPTY_FILTER,
              IsPartOf.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
            0, MAX_JOBS_CNT)
          .stream().map(entity -> ((DataJobEntity) entity).getUrn()).collect(Collectors.toList());
      System.out.println("DataJobsResource----: " + Arrays.toString(dataJobUrns.toArray()));
      final DataJobUrnArray dataJobUrnArray = new DataJobUrnArray(new ArrayList<>(dataJobUrns));
      return new DataJobsLineage().setDataJobs(dataJobUrnArray);
    });
  }

  @Nonnull
  private DataFlowUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
    DataFlowKey key = keys.<ComplexResourceKey<DataFlowKey, EmptyRecord>>get(DATAFLOW_KEY).getKey();
    return new DataFlowUrn(key.getOrchestrator(), key.getFlowId(), key.getCluster());
  }
}
