package com.linkedin.metadata.resources.ml;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.restli.common.HttpStatus;
import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Deprecated! Use {@link EntityResource} instead.
 *
 * Rest.li entry point: /mlModels/{mlModelKey}/evaluationData
 */
@Slf4j
@Deprecated
@RestLiCollection(name = "evaluationData", namespace = "com.linkedin.ml", parent = MLModels.class)
public class EvaluationDataResource extends BaseMLModelsAspectResource<EvaluationData> {
    public EvaluationDataResource() {
        super(EvaluationData.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<EvaluationData> get(@Nonnull Long version) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final RecordDataSchema aspectSchema = new EvaluationData().schema();

            final RecordTemplate maybeAspect = getEntityService().getAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(aspectSchema),
                version
            );
            if (maybeAspect != null) {
                return new EvaluationData(maybeAspect.data());
            }
            throw RestliUtils.resourceNotFoundException();
        });
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull EvaluationData evaluationData) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
            getEntityService().ingestAspect(
                urn,
                PegasusUtils.getAspectNameFromSchema(evaluationData.schema()),
                evaluationData,
                auditStamp);
            return new CreateResponse(HttpStatus.S_201_CREATED);
        });
    }
}