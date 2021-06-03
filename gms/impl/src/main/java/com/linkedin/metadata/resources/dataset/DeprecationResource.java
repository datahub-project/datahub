package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;


/**
 * Deprecated! Use {@link EntityResource} instead.
 *
 * Rest.li entry point: /datasets/{datasetKey}/deprecation
 */
@Slf4j
@Deprecated
@RestLiCollection(name = "deprecation", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class DeprecationResource extends BaseDatasetVersionedAspectResource<DatasetDeprecation> {
  public DeprecationResource() {
    super(DatasetDeprecation.class);
  }

  @RestMethod.Get
  @Nonnull
  @Override
  public Task<DatasetDeprecation> get(@Nonnull Long version) {
    return RestliUtils.toTask(() -> {
      final Urn urn = getUrn(getContext().getPathKeys());
      final RecordDataSchema aspectSchema = new DatasetDeprecation().schema();

      final RecordTemplate maybeAspect = getEntityService().getAspect(
          urn,
          PegasusUtils.getAspectNameFromSchema(aspectSchema),
          version
      );
      if (maybeAspect != null) {
        return new DatasetDeprecation(maybeAspect.data());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.Create
  @Nonnull
  @Override
  public Task<CreateResponse> create(@Nonnull DatasetDeprecation datasetDeprecation) {
    return RestliUtils.toTask(() -> {
      final Urn urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getEntityService().ingestAspect(
          urn,
          PegasusUtils.getAspectNameFromSchema(datasetDeprecation.schema()),
          datasetDeprecation,
          auditStamp);
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }
}