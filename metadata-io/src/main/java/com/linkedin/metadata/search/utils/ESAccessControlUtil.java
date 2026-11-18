package com.linkedin.metadata.search.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ESAccessControlUtil {
  private ESAccessControlUtil() {}

  /**
   * Given an OperationContext and SearchResult, mark the restricted entities, or leave them out
   * when the request hides entities the caller can't view. See {@link
   * #restrictSearchResult(OperationContext, Collection)}.
   *
   * @param searchResult restricted search result
   */
  public static void restrictSearchResult(
      @Nonnull OperationContext opContext, @Nonnull SearchResult searchResult) {
    final Collection<SearchEntity> restricted =
        restrictSearchResult(opContext, searchResult.getEntities());
    final int removed = searchResult.getEntities().size() - restricted.size();
    if (removed > 0) {
      searchResult.setEntities(new SearchEntityArray(restricted));
      searchResult.setNumEntities(Math.max(0, searchResult.getNumEntities() - removed));
    }
  }

  /**
   * The search entities as the caller may see them. When a restricted search was requested,
   * entities the caller can't view are kept with an encrypted urn and only their key aspect. When
   * instead the request hides entities the caller can't view, they are left out, as entities that
   * don't exist; callers lower their totals by the number left out.
   */
  public static Collection<SearchEntity> restrictSearchResult(
      @Nonnull OperationContext opContext, Collection<SearchEntity> searchEntities) {
    if (!opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        || opContext.isSystemAuth()) {
      return searchEntities;
    }
    final boolean restrictedSearch = opContext.getSearchContext().isRestrictedSearch();
    if (!restrictedSearch && !hidesUnviewableEntities(opContext)) {
      return searchEntities;
    }

    final Set<Urn> unviewable =
        EntityAuthorizationUtils.unviewableUrns(
            opContext,
            searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
    if (unviewable.isEmpty()) {
      return searchEntities;
    }
    if (!restrictedSearch) {
      return searchEntities.stream()
          .filter(searchEntity -> !unviewable.contains(searchEntity.getEntity()))
          .collect(Collectors.toList());
    }

    final EntityRegistry entityRegistry = Objects.requireNonNull(opContext.getEntityRegistry());
    final RestrictedService restrictedService =
        Objects.requireNonNull(opContext.getServicesRegistryContext()).getRestrictedService();
    for (SearchEntity searchEntity : searchEntities) {
      if (unviewable.contains(searchEntity.getEntity())) {
        searchEntity.setRestrictedAspects(
            new StringArray(
                List.of(
                    entityRegistry
                        .getEntitySpec(searchEntity.getEntity().getEntityType())
                        .getKeyAspectName())));
        searchEntity.setEntity(restrictedService.encryptRestrictedUrn(searchEntity.getEntity()));
      }
    }
    return searchEntities;
  }

  public static boolean restrictUrn(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      return !EntityAuthorizationUtils.canViewEntity(opContext, urn);
    }
    return false;
  }

  private static boolean hidesUnviewableEntities(@Nonnull OperationContext opContext) {
    return opContext.getRequestContext() != null
        && opContext.getRequestContext().hidesUnviewableEntities();
  }
}
