package com.linkedin.metadata.search.utils;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
        for (SearchEntity searchEntity : searchEntities) {
          final String entityType = searchEntity.getEntity().getEntityType();
          final com.linkedin.metadata.models.EntitySpec entitySpec =
              entityRegistry.getEntitySpec(entityType);

          if (VIEW_RESTRICTED_ENTITY_TYPES.contains(entityType)
              && !AuthUtil.canViewEntity(opContext, searchEntity.getEntity())) {

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
      return !AuthUtil.canViewEntity(opContext, urn);
    }
    return false;
  }
}
