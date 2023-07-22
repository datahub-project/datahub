package com.linkedin.metadata.entity.ebean.transactions;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
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
    private final List<? extends AbstractBatchItem> items;

    public Map<String, Set<String>> getUrnAspectsMap() {
        return items.stream()
                .map(aspect -> Map.entry(aspect.getUrn().toString(), aspect.getAspectName()))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
    }

    public boolean containsDuplicateAspects() {
        return items.stream().map(i -> String.format("%s_%s", i.getClass().getName(), i.hashCode()))
                .distinct().count() != items.size();
    }

    public static class AspectsBatchBuilder {
        /**
         * Just one aspect record template
         * @param data aspect data
         * @return builder
         */
        public AspectsBatchBuilder one(AbstractBatchItem data) {
            this.items = List.of(data);
            return this;
        }

        public AspectsBatchBuilder mcps(List<MetadataChangeProposal> mcps, EntityRegistry entityRegistry) {
            this.items = mcps.stream().map(mcp -> {
                if (mcp.getChangeType().equals(ChangeType.PATCH)) {
                    return PatchBatchItem.PatchBatchItemBuilder.build(mcp, entityRegistry);
                } else {
                    return UpsertBatchItem.UpsertBatchItemBuilder.build(mcp, entityRegistry);
                }
            }).collect(Collectors.toList());
            return this;
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
