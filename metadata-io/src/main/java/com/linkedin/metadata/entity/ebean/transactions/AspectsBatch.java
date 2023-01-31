package com.linkedin.metadata.entity.ebean.transactions;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.events.metadata.ChangeType;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class AspectsBatch {
    private final List<AspectsBatchItem> items;

    public Map<String, Set<String>> getUrnAspectsMap() {
        return items.stream()
                .map(aspect -> Map.entry(aspect.getUrn().toString(), aspect.getAspectName()))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
    }

    public static class AspectsBatchBuilder {

        /**
         * Just one aspect record template
         * @param data aspect data
         * @return builder
         */
        public AspectsBatchBuilder aspect(AspectsBatchItem data) {
            this.items = List.of(data);
            return this;
        }

        public AspectsBatchBuilder mcps(List<MetadataChangeProposal> mcps, EntityRegistry entityRegistry) {
            this.items = mcps.stream().map(mcp -> toAspectBatchItem(mcp, entityRegistry)).collect(Collectors.toList());
            return this;
        }

        private static AspectsBatchItem toAspectBatchItem(MetadataChangeProposal mcp, EntityRegistry entityRegistry) {
            log.debug("entity type = {}", mcp.getEntityType());
            EntitySpec entitySpec = entityRegistry.getEntitySpec(mcp.getEntityType());
            AspectSpec aspectSpec = validateAspect(mcp, entitySpec);
            RecordTemplate aspect = convertToRecordTemplate(mcp, aspectSpec);
            log.debug("aspect = {}", aspect);

            if (!isValidChangeType(mcp.getChangeType(), aspectSpec)) {
                throw new UnsupportedOperationException("ChangeType not supported: " + mcp.getChangeType()
                        + " for aspect " + mcp.getAspectName());
            }

            Urn urn = mcp.getEntityUrn();
            if (urn == null) {
                urn = EntityKeyUtils.getUrnFromProposal(mcp, aspectSpec);
            }

            return AspectsBatchItem.builder()
                    .urn(urn)
                    .aspectName(mcp.getAspectName())
                    .systemMetadata(mcp.getSystemMetadata())
                    .mcp(mcp)
                    .value(aspect)
                    .build(entityRegistry);
        }

        private static RecordTemplate convertToRecordTemplate(MetadataChangeProposal mcp, AspectSpec aspectSpec) {
            RecordTemplate aspect;
            try {
                aspect = GenericRecordUtils.deserializeAspect(mcp.getAspect().getValue(),
                        mcp.getAspect().getContentType(), aspectSpec);
                ValidationUtils.validateOrThrow(aspect);
            } catch (ModelConversionException e) {
                throw new RuntimeException(
                        String.format("Could not deserialize %s for aspect %s", mcp.getAspect().getValue(),
                                mcp.getAspectName()));
            }
            log.debug("aspect = {}", aspect);
            return aspect;
        }

        private static AspectSpec validateAspect(MetadataChangeProposal mcp, EntitySpec entitySpec) {
            if (!mcp.hasAspectName() || !mcp.hasAspect()) {
                throw new UnsupportedOperationException("Aspect and aspect name is required for create and update operations");
            }

            AspectSpec aspectSpec = entitySpec.getAspectSpec(mcp.getAspectName());

            if (aspectSpec == null) {
                throw new RuntimeException(
                        String.format("Unknown aspect %s for entity %s", mcp.getAspectName(),
                                mcp.getEntityType()));
            }

            return aspectSpec;
        }

        /**
         * Validates that a change type is valid for the given aspect
         * @param changeType
         * @param aspectSpec
         * @return
         */
        private static boolean isValidChangeType(ChangeType changeType, AspectSpec aspectSpec) {
            if (aspectSpec.isTimeseries()) {
                // Timeseries aspects only support UPSERT
                return ChangeType.UPSERT.equals(changeType);
            } else {
                return (ChangeType.UPSERT.equals(changeType) || ChangeType.PATCH.equals(changeType));
            }
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
        AspectsBatch that = (AspectsBatch) o;
        return items.equals(that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items);
    }

    @Override
    public String toString() {
        return "AspectsBatch{"
                + "items="
                + items
                + '}';
    }
}
