package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME;

import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Describes cache binding and live fallbacks for membership reads on the membership graph. */
@Value
@Builder
public class MembershipReadSpec {

  /**
   * Fallback when {@code bounds.maxEdges} is unset on the membership graph definition (matches
   * {@code EntityGraphRegistry}'s default of {@code maxVertices * 1.5} with {@code maxVertices}
   * default 10000).
   */
  public static final int DEFAULT_RELATED_TYPE_FILTER_FETCH_CAP = 15_000;

  @Nonnull EntityGraphBinding binding;

  @Nonnull @Builder.Default Set<String> groupRelationshipTypes = Set.of();

  @Nonnull @Builder.Default
  Set<String> roleRelationshipTypes = Set.of(IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME);

  @Nonnull @Builder.Default Set<String> scrollUserEntityTypes = Set.of(CORP_USER_ENTITY_NAME);

  @Nonnull @Builder.Default Set<String> scrollGroupEntityTypes = Set.of(CORP_GROUP_ENTITY_NAME);

  @Nonnull @Builder.Default Set<String> scrollRoleEntityTypes = Set.of(DATAHUB_ROLE_ENTITY_NAME);

  /**
   * Max neighbors to fetch before client-side {@code relatedEntityTypes} filtering. Taken from the
   * membership graph's {@code bounds.maxEdges} when configured.
   */
  @Builder.Default int relatedTypeFilterFetchCap = DEFAULT_RELATED_TYPE_FILTER_FETCH_CAP;
}
