package com.linkedin.metadata.entity.ebean.transactions;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class AspectsBatchImpl implements AspectsBatch {
  private final List<? extends AbstractBatchItem> items;

  public static class AspectsBatchImplBuilder {
    /**
     * Just one aspect record template
     *
     * @param data aspect data
     * @return builder
     */
    public AspectsBatchImplBuilder one(AbstractBatchItem data) {
      this.items = List.of(data);
      return this;
    }

    public AspectsBatchImplBuilder mcps(
        List<MetadataChangeProposal> mcps, EntityRegistry entityRegistry) {
      this.items =
          mcps.stream()
              .map(
                  mcp -> {
                    if (mcp.getChangeType().equals(ChangeType.PATCH)) {
                      return PatchBatchItem.PatchBatchItemBuilder.build(mcp, entityRegistry);
                    } else {
                      return UpsertBatchItem.UpsertBatchItemBuilder.build(mcp, entityRegistry);
                    }
                  })
              .collect(Collectors.toList());
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
    AspectsBatchImpl that = (AspectsBatchImpl) o;
    return Objects.equals(items, that.items);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }

  @Override
  public String toString() {
    return "AspectsBatchImpl{" + "items=" + items + '}';
  }
}
