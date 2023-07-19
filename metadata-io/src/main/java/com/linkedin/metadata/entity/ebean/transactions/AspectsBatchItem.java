package com.linkedin.metadata.entity.ebean.transactions;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class AspectsBatchItem {
    // urn an urn associated with the new aspect
    private final Urn urn;
    // aspectName name of the aspect being inserted
    private final String aspectName;
    private final SystemMetadata systemMetadata;
    // updateLambda Function to apply to the latest version of the aspect to get the updated version
    private final Function<Optional<RecordTemplate>, RecordTemplate> lambda;

    private final MetadataChangeProposal mcp;

    // derived
    private final EntitySpec entitySpec;
    private final AspectSpec aspectSpec;

    public RecordTemplate getAspect() {
        return lambda.apply(null);
    }

    public EntityAspect toLatestEntityAspect(AuditStamp auditStamp) {
        EntityAspect latest = new EntityAspect();
        latest.setAspect(aspectName);
        latest.setMetadata(EntityUtils.toJsonAspect(getAspect()));
        latest.setUrn(urn.toString());
        latest.setVersion(ASPECT_LATEST_VERSION);
        latest.setCreatedOn(new Timestamp(auditStamp.getTime()));
        latest.setCreatedBy(auditStamp.getActor().toString());
        return latest;
    }

    public static class AspectsBatchItemBuilder {
        public AspectsBatchItemBuilder value(final RecordTemplate recordTemplate) {
            this.lambda = ignored -> recordTemplate;
            return this;
        }

        public AspectsBatchItem build(EntityRegistry entityRegistry) {
            EntityUtils.validateUrn(entityRegistry, this.urn);
            log.debug("entity type = {}", this.urn.getEntityType());

            entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
            log.debug("entity spec = {}", this.entitySpec);

            aspectSpec(AspectUtils.validate(this.entitySpec, this.aspectName));
            log.debug("aspect spec = {}", this.aspectSpec);

            if (this.lambda != null) {
                RecordTemplate aspect = this.lambda.apply(null);
                AspectUtils.validateRecordTemplate(entityRegistry, this.entitySpec, this.urn, aspect);
            }

            return new AspectsBatchItem(this.urn, this.aspectName, generateSystemMetadataIfEmpty(this.systemMetadata),
                    this.lambda, this.mcp, this.entitySpec, this.aspectSpec);
        }

        private AspectsBatchItemBuilder entitySpec(EntitySpec entitySpec) {
            this.entitySpec = entitySpec;
            return this;
        }

        private AspectsBatchItemBuilder aspectSpec(AspectSpec aspectSpec) {
            this.aspectSpec = aspectSpec;
            return this;
        }

        @Nonnull
        private SystemMetadata generateSystemMetadataIfEmpty(@Nullable SystemMetadata systemMetadata) {
            if (systemMetadata == null) {
                systemMetadata = new SystemMetadata();
                systemMetadata.setRunId(EntityService.DEFAULT_RUN_ID);
                systemMetadata.setLastObserved(System.currentTimeMillis());
            }
            return systemMetadata;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AspectsBatchItem that = (AspectsBatchItem) o;

        if (!urn.equals(that.urn)) {
            return false;
        }
        if (!aspectName.equals(that.aspectName)) {
            return false;
        }
        if (!systemMetadata.equals(that.systemMetadata)) {
            return false;
        }
        if (!Objects.equals(lambda, that.lambda)) {
            return false;
        }
        return mcp.equals(that.mcp);
    }

    @Override
    public int hashCode() {
        int result = urn.hashCode();
        result = 31 * result + aspectName.hashCode();
        result = 31 * result + systemMetadata.hashCode();
        result = 31 * result + (lambda != null ? lambda.hashCode() : 0);
        result = 31 * result + mcp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AspectsBatchItem{"
                + "urn=" + urn
                + ", aspectName='"
                + aspectName + '\''
                + ", systemMetadata=" + systemMetadata
                + ", lambda=" + lambda
                + ", mcp=" + mcp
                + '}';
    }
}
