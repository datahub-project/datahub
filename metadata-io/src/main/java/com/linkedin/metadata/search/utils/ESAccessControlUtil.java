package com.linkedin.metadata.search.utils;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ESAccessControlUtil {
  private ESAccessControlUtil() {}

  /**
   * Given an OperationContext and SearchResult, mark the restricted entities. Currently, the entire
   * entity is marked as restricted using the key aspect name.
   *
   * @param searchResult restricted search result
   */
  public static void restrictSearchResult(
      @Nonnull OperationContext opContext, @Nonnull SearchResult searchResult) {
    restrictSearchResult(opContext, searchResult.getEntities());
  }

  public static Collection<SearchEntity> restrictSearchResult(
      @Nonnull OperationContext opContext, Collection<SearchEntity> searchEntities) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      final EntityRegistry entityRegistry = Objects.requireNonNull(opContext.getEntityRegistry());
      final RestrictedService restrictedService =
          Objects.requireNonNull(opContext.getServicesRegistryContext()).getRestrictedService();

      if (opContext.getSearchContext().isRestrictedSearch()) {
        prefetchOwnership(opContext, searchEntities);
        for (SearchEntity searchEntity : searchEntities) {
          final String entityType = searchEntity.getEntity().getEntityType();
          final com.linkedin.metadata.models.EntitySpec entitySpec =
              entityRegistry.getEntitySpec(entityType);

          if (VIEW_RESTRICTED_ENTITY_TYPES.contains(entityType)
              && !canViewEntity(opContext, searchEntity.getEntity())) {

            // Not authorized && restricted response requested
            if (opContext.getSearchContext().isRestrictedSearch()) {
              // Restrict entity
              searchEntity.setRestrictedAspects(
                  new StringArray(List.of(entitySpec.getKeyAspectName())));

              searchEntity.setEntity(
                  restrictedService.encryptRestrictedUrn(searchEntity.getEntity()));
            }
          }
        }
      }
    }
    return searchEntities;
  }

  public static boolean restrictUrn(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      return !canViewEntity(opContext, urn);
    }
    return false;
  }

  /**
   * When ownership-based view policies are in play, batch-warm ownership for all restricted results
   * up front so the per-result {@link #canViewEntity} checks hit the request-scoped cache instead
   * of fetching ownership one result at a time. Gated behind the {@code ownershipPrefetchEnabled}
   * feature flag and the presence of an owner-scoped policy; best-effort, falling back to lazy
   * per-result resolution on any failure.
   */
  private static void prefetchOwnership(
      @Nonnull OperationContext opContext, @Nonnull Collection<SearchEntity> searchEntities) {
    OwnershipPrefetchUtil.prefetchOwnershipForUrns(
        opContext,
        searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  private static boolean canViewEntity(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (QUERY_ENTITY_NAME.equals(urn.getEntityType())) {
      return EntityAspectAuthorizationUtils.canViewQueryEntity(
          opContext, opContext, opContext.getAspectRetriever(), urn);
    }
    return AuthUtil.canViewEntity(opContext, urn);
  }
}
