package com.linkedin.dataset.rest.resources;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.*;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.*;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Rest.li entry point: /datasets/{datasetKey}/downstreamLineage
 */
@RestLiSimpleResource(name = "downstreamLineage", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class DownstreamLineageResource extends SimpleResourceTemplate<DownstreamLineage> {

  private static final String DATASET_KEY = Datasets.class.getAnnotation(RestLiCollection.class).keyName();

  @Inject
  @Named("datasetSearchDao")
  private BaseSearchDAO _searchDAO;

  @Inject
  @Named("datasetDao")
  private BaseLocalDAO _localDAO;

  public DownstreamLineageResource() {
    super();
  }

  @Nonnull
  @RestMethod.Get
  public Task<DownstreamLineage> get(@PathKeysParam @Nonnull PathKeys keys) {
    final DatasetUrn datasetUrn = getUrn(keys);
    final Filter filter = SearchUtils.getFilter(Collections.singletonMap("upstreams", datasetUrn.toString()));

    return RestliUtils.toTask(() -> {
      final SearchResult<DatasetDocument> searchResult = _searchDAO.search("*", filter, 0, Integer.MAX_VALUE);
      final Set<DatasetUrn> downstreamDatasets = searchResult.getDocumentList()
              .stream()
              .map(d -> (DatasetUrn) ModelUtils.getUrnFromDocument(d))
              .collect(Collectors.toSet());
      final DownstreamArray downstreamArray = new DownstreamArray(downstreamDatasets.stream()
              .map(ds -> {
                final UpstreamLineage upstreamLineage = (UpstreamLineage) _localDAO.get(UpstreamLineage.class, ds).get();
                final Upstream upstream = upstreamLineage.getUpstreams().stream()
                        .filter(us -> us.getDataset().equals(datasetUrn))
                        .collect(Collectors.toList())
                        .get(0);
                return new Downstream().setDataset(ds).setType(upstream.getType()).setAuditStamp(upstream.getAuditStamp());
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
