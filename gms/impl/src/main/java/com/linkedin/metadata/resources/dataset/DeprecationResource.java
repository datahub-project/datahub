package com.linkedin.metadata.resources.dataset;

import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /datasets/{datasetKey}/deprecation
 */
@Slf4j
@RestLiCollection(name = "deprecation", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class DeprecationResource extends BaseDatasetVersionedAspectResource<DatasetDeprecation> {
  public DeprecationResource() {
    super(DatasetDeprecation.class);
  }

  @RestMethod.Get
  @Nonnull
  @Override
  public Task<DatasetDeprecation> get(@Nonnull Long version) {
    return super.get(version);
  }

  @RestMethod.Create
  @Nonnull
  @Override
  public Task<CreateResponse> create(@Nonnull DatasetDeprecation datasetDeprecation) {
    return super.create(datasetDeprecation);
  }
}