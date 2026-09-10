package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Describes cache binding and live fallbacks for hierarchy reads on one entity graph. */
@Value
@Builder
public class HierarchyReadSpec {

  @Nonnull EntityGraphBinding binding;

  @Nonnull @Builder.Default Map<String, ParentAspectSpec> parentAspectsByEntityType = Map.of();

  @Nonnull @Builder.Default Set<String> scrollSourceEntityTypes = Set.of();

  @Nonnull @Builder.Default Set<String> scrollDestinationEntityTypes = Set.of();

  @Nonnull @Builder.Default String relationshipType = "IsPartOf";

  @Nullable
  public ParentAspectSpec parentAspectFor(@Nonnull Urn urn) {
    return parentAspectsByEntityType.get(urn.getEntityType());
  }
}
