package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME;

import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;

/** Built-in {@link MembershipReadSpec} factories for the membership entity graph. */
@UtilityClass
public class MembershipReadSpecs {

  private static final Set<String> GROUP_RELATIONSHIP_TYPES =
      Set.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME, IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME);

  @Nonnull
  public static Optional<MembershipReadSpec> forKnownGraph(
      @Nonnull KnownEntityGraph known, @Nonnull EntityGraphBinding binding) {
    return forKnownGraph(known, binding, MembershipReadSpec.DEFAULT_RELATED_TYPE_FILTER_FETCH_CAP);
  }

  @Nonnull
  public static Optional<MembershipReadSpec> forKnownGraph(
      @Nonnull KnownEntityGraph known,
      @Nonnull EntityGraphBinding binding,
      int relatedTypeFilterFetchCap) {
    if (known != KnownEntityGraph.MEMBERSHIP) {
      return Optional.empty();
    }
    return Optional.of(membership(binding, relatedTypeFilterFetchCap));
  }

  @Nonnull
  public static MembershipReadSpec membership(@Nonnull EntityGraphBinding binding) {
    return membership(binding, MembershipReadSpec.DEFAULT_RELATED_TYPE_FILTER_FETCH_CAP);
  }

  @Nonnull
  public static MembershipReadSpec membership(
      @Nonnull EntityGraphBinding binding, int relatedTypeFilterFetchCap) {
    return MembershipReadSpec.builder()
        .binding(binding)
        .groupRelationshipTypes(GROUP_RELATIONSHIP_TYPES)
        .relatedTypeFilterFetchCap(relatedTypeFilterFetchCap)
        .build();
  }
}
