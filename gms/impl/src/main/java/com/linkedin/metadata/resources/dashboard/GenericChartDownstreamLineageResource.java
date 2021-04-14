package com.linkedin.metadata.resources.dashboard;

import com.linkedin.common.relationships.EntityRelationship;
import com.linkedin.common.relationships.EntityRelationshipArray;
import com.linkedin.common.relationships.GenericLineage;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.ChartKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseQueryDAO;
import com.linkedin.metadata.entity.ChartEntity;
import com.linkedin.metadata.entity.DashboardEntity;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.relationship.Contains;
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
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.Neo4jUtil.createRelationshipFilter;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


/**
 * Rest.li entry point: /chart/{chartKey}/genericDownstreamLineage
 */
@RestLiSimpleResource(name = "genericDownstreamLineage", namespace = "com.linkedin.chart", parent = Charts.class)
public final class GenericChartDownstreamLineageResource extends SimpleResourceTemplate<GenericLineage> {

  private static final String CHART_KEY = Charts.class.getAnnotation(RestLiCollection.class).keyName();
  private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
  private static final Integer MAX_DOWNSTREAM_CNT = 100;

  @Inject
  @Named("dashboardDAO")
  private BaseLocalDAO _dashboardDAO;

  @Inject
  @Named("datasetQueryDao")
  private BaseQueryDAO _datasetQueryDAO;

  public GenericChartDownstreamLineageResource() {
    super();
  }

  @Nonnull
  @RestMethod.Get
  public Task<GenericLineage> get(@PathKeysParam @Nonnull PathKeys keys) {
    final ChartUrn chartUrn = getUrn(keys);

    return RestliUtils.toTask(() -> {
      final List<DashboardUrn> downstreamDashboards = _datasetQueryDAO
          .findEntities(ChartEntity.class, newFilter("urn", chartUrn.toString()),
              DashboardEntity.class, EMPTY_FILTER,
              Contains.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              0, MAX_DOWNSTREAM_CNT)
          .stream().map(entity -> ((DashboardEntity) entity).getUrn()).collect(Collectors.toList());

      final EntityRelationshipArray downstreamArray = new EntityRelationshipArray(downstreamDashboards.stream()
          .map(ds -> {
            return new EntityRelationship()
                .setEntity(ds);
          })
          .collect(Collectors.toList())
      );

      return new GenericLineage().setEntities(downstreamArray);
    });
  }

  @Nonnull
  private ChartUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
    ChartKey key = keys.<ComplexResourceKey<ChartKey, EmptyRecord>>get(CHART_KEY).getKey();
    return new ChartUrn(key.getTool(), key.getChartId());
  }
}
