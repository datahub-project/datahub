package com.linkedin.dataset.rest.resources;

import com.linkedin.dataset.DownstreamArray;
import com.linkedin.dataset.DownstreamLineage;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTaskTemplate;
import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /datasets/{datasetKey}/downstreamLineage
 */
@RestLiSimpleResource(name = "downstreamLineage", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class DownstreamLineageResource extends SimpleResourceTaskTemplate<DownstreamLineage> {

  public DownstreamLineageResource() {
    super();
  }

  @Nonnull
  @Override
  @RestMethod.Get
  public Task<DownstreamLineage> get() {
    return RestliUtils.toTask(() -> new DownstreamLineage().setDownstreams(new DownstreamArray(0)));
  }
}
