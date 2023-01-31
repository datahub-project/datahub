package com.linkedin.metadata.entity.ebean.transactions;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityAspect;
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
    //updateLambda Function to apply to the latest version of the aspect to get the updated version
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
            validateUrn(this.urn);
            log.debug("entity type = {}", this.urn.getEntityType());

            entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
            log.debug("entity spec = {}", this.entitySpec);

            aspectSpec(validateAspect(this.aspectName, this.entitySpec));
            log.debug("aspect spec = {}", this.aspectSpec);

            return new AspectsBatchItem(this.urn, this.aspectName, this.systemMetadata, this.lambda, this.mcp,
                    this.entitySpec, this.aspectSpec);
        }

        private AspectsBatchItemBuilder entitySpec(EntitySpec entitySpec) {
            this.entitySpec = entitySpec;
            return this;
        }

        private AspectsBatchItemBuilder aspectSpec(AspectSpec aspectSpec) {
            this.aspectSpec = aspectSpec;
            return this;
        }

        private static void validateUrn(@Nonnull final Urn urn) {
            if (!urn.toString().trim().equals(urn.toString())) {
                throw new IllegalArgumentException("Error: cannot provide an URN with leading or trailing whitespace");
            }
        }

        private static AspectSpec validateAspect(String aspectName, EntitySpec entitySpec) {
            if (aspectName == null || aspectName.isEmpty()) {
                throw new UnsupportedOperationException("Aspect name is required for create and update operations");
            }

            AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);

            if (aspectSpec == null) {
                throw new RuntimeException(
                        String.format("Unknown aspect %s for entity %s", aspectName, entitySpec.getName()));
            }

            return aspectSpec;
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
        return urn.equals(that.urn) && aspectName.equals(that.aspectName)
                && Objects.equals(systemMetadata, that.systemMetadata)
                && lambda.apply(null).equals(that.lambda.apply(null))
                && Objects.equals(mcp, that.mcp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(urn, aspectName, systemMetadata, lambda.apply(null), mcp);
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
