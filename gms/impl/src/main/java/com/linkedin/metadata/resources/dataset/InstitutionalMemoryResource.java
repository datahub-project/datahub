package com.linkedin.metadata.resources.dataset;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EntitySpecUtils;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/institutionalMemory
 */
@Slf4j
@RestLiCollection(name = "institutionalMemory", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class InstitutionalMemoryResource extends BaseDatasetVersionedAspectResource<InstitutionalMemory> {
    public InstitutionalMemoryResource() {
        super(InstitutionalMemory.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<InstitutionalMemory> get(@Nonnull Long version) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final RecordDataSchema aspectSchema = new InstitutionalMemory().schema();

            final Map<Urn, List<RecordTemplate>> urnToAspectsMap = getEntityService().batchGetAspectRecordLists(
                ImmutableSet.of(urn),
                ImmutableSet.of(EntitySpecUtils.getAspectNameFromSchema(aspectSchema))
            );

            if (urnToAspectsMap.containsKey(urn)) {
                // Aspect does exist.
                final RecordTemplate aspect = urnToAspectsMap.get(urn).stream()
                    .filter(aspectRecord -> aspectRecord.schema().getFullName().equals(aspectSchema.getFullName()))
                    .findFirst()
                    .get();
                return new InstitutionalMemory(aspect.data());
            }
            throw RestliUtils.resourceNotFoundException();
        });    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull InstitutionalMemory institutionalMemory) {
        return RestliUtils.toTask(() -> {
            final Urn urn = getUrn(getContext().getPathKeys());
            final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
            getEntityService().ingestAspect(
                urn,
                EntitySpecUtils.getAspectNameFromSchema(institutionalMemory.schema()),
                institutionalMemory,
                auditStamp);
            return new CreateResponse(HttpStatus.S_201_CREATED);
        });
    }
}
