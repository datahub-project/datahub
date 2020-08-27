package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.Downstream;
import com.linkedin.dataset.DownstreamArray;
import com.linkedin.dataset.DownstreamLineage;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseQueryDAO;
import com.linkedin.metadata.entity.DatasetEntity;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.relationship.DownstreamOf;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.*;


/**
 * Rest.li entry point: /datasets/{datasetKey}/downstreamLineage
 */
@RestLiSimpleResource(name = "downstreamLineage", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class DownstreamLineageResource extends SimpleResourceTemplate<DownstreamLineage> {

  private static final String DATASET_KEY = Datasets.class.getAnnotation(RestLiCollection.class).keyName();
  private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
  private static final Integer MAX_DOWNSTREAM_CNT = 100;

  @Inject
  @Named("datasetDao")
  private BaseLocalDAO _localDAO;

  @Inject
  @Named("datasetQueryDao")
  private BaseQueryDAO _queryDAO;

  public DownstreamLineageResource() {
    super();
  }

  @Nonnull
  @RestMethod.Get
  public Task<DownstreamLineage> get(@PathKeysParam @Nonnull PathKeys keys) {
    final DatasetUrn datasetUrn = getUrn(keys);
    final Filter filter = newFilter(Collections.singletonMap("upstreams", datasetUrn.toString()));

    return RestliUtils.toTask(() -> {
      final List<DatasetUrn> downstreamDatasets = _queryDAO
          .findEntities(DatasetEntity.class, newFilter("urn", datasetUrn.toString()),
              DatasetEntity.class, EMPTY_FILTER,
              DownstreamOf.class, createRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              0, MAX_DOWNSTREAM_CNT)
          .stream().map(entity -> ((DatasetEntity) entity).getUrn()).collect(Collectors.toList());

      final DownstreamArray downstreamArray = new DownstreamArray(downstreamDatasets.stream()
          .map(ds -> {
            final UpstreamLineage upstreamLineage = (UpstreamLineage) _localDAO.get(UpstreamLineage.class, ds).get();
            final List<Upstream> upstreams = upstreamLineage.getUpstreams().stream()
                .filter(us -> us.getDataset().equals(datasetUrn))
                .collect(Collectors.toList());
            if (upstreams.size() != 1) {
              throw new RuntimeException(String.format("There is no relation or more than 1 relation between the datasets!"));
            }
            return new Downstream()
                .setDataset(ds)
                .setType(upstreams.get(0).getType())
                .setAuditStamp(upstreams.get(0).getAuditStamp());
          })
          .collect(Collectors.toList())
      );
      return new DownstreamLineage().setDownstreams(downstreamArray);
    });
  }

  @Nonnull
  private DatasetUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
    DatasetKey key = keys.<ComplexResourceKey<DatasetKey, EmptyRecord>>get(DATASET_KEY).getKey();
    return new DatasetUrn(key.getPlatform(), key.getName(), key.getOrigin());
  }
}
