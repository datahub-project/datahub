package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.dataset.UpstreamLineageDelta;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import javax.annotation.Nonnull;


/**
 * Deprecated! Use {@link EntityResource} instead.
 *
 * Rest.li entry point: /datasets/{datasetKey}/upstreamLineage
 */
@Deprecated
@RestLiCollection(name = "upstreamLineage", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class UpstreamLineageResource extends BaseDatasetVersionedAspectResource<UpstreamLineage> {

    public UpstreamLineageResource() {
        super(UpstreamLineage.class);
    }

    @Nonnull
    @Override
    @RestMethod.Get
    public Task<UpstreamLineage> get(@Nonnull Long version) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final RecordDataSchema aspectSchema = new UpstreamLineage().schema();

            final RecordTemplate maybeAspect = getEntityService().getAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(aspectSchema),
                version
            );
            if (maybeAspect != null) {
                return new UpstreamLineage(maybeAspect.data());
            }
            throw RestliUtils.resourceNotFoundException();
        });
    }

    @Nonnull
    @Override
    @RestMethod.Create
    public Task<CreateResponse> create(@Nonnull UpstreamLineage upstreamLineage) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
            getEntityService().ingestAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(upstreamLineage.schema()),
                upstreamLineage,
                auditStamp);
            return new CreateResponse(HttpStatus.S_201_CREATED);
        });
    }

    @Nonnull
    @Action(name = "deltaUpdate")
    public UpstreamLineage deltaUpdate(@ActionParam("delta") @Nonnull UpstreamLineageDelta delta) {
        throw new UnsupportedOperationException();
    }
}