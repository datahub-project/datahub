package com.linkedin.metadata.resources.ml;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.restli.common.HttpStatus;
import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.CaveatsAndRecommendations;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Deprecated! Use {@link EntityResource} instead.
 *
 * Rest.li entry point: /mlModels/{mlModelKey}/caveatsAndRecommendations
 *
 */
@Slf4j
@Deprecated
@RestLiCollection(name = "caveatsAndRecommendations", namespace = "com.linkedin.ml", parent = MLModels.class)
public class CaveatsAndRecommendationsResource extends BaseMLModelsAspectResource<CaveatsAndRecommendations> {
    public CaveatsAndRecommendationsResource() {
        super(CaveatsAndRecommendations.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<CaveatsAndRecommendations> get(@Nonnull Long version) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final RecordDataSchema aspectSchema = new CaveatsAndRecommendations().schema();

            final RecordTemplate maybeAspect = getEntityService().getAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(aspectSchema),
                version
            );
            if (maybeAspect != null) {
                return new CaveatsAndRecommendations(maybeAspect.data());
            }
            throw RestliUtils.resourceNotFoundException();
        });
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull CaveatsAndRecommendations caveatsAndRecommendations) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
            getEntityService().ingestAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(caveatsAndRecommendations.schema()),
                caveatsAndRecommendations,
                auditStamp);
            return new CreateResponse(HttpStatus.S_201_CREATED);
        });
    }
}