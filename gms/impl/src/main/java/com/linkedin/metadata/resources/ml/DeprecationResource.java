package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.common.Deprecation;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;


/**
 * Rest.li entry point: /mlmodels/{mlModelKey}/deprecation
 */
@Slf4j
@RestLiCollection(name = "deprecation", namespace = "com.linkedin.ml", parent = MLModels.class)
public class DeprecationResource extends BaseMLModelsAspectResource<Deprecation> {
  public DeprecationResource() {
    super(Deprecation.class);
  }

  @RestMethod.Get
  @Nonnull
  @Override
  public Task<Deprecation> get(@Nonnull Long version) {
    return super.get(version);
  }

  @RestMethod.Create
  @Nonnull
  @Override
  public Task<CreateResponse> create(@Nonnull Deprecation deprecation) {
    return super.create(deprecation);
  }
}