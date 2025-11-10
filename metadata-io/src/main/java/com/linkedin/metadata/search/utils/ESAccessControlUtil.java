package com.linkedin.metadata.search.utils;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
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

      for (SearchEntity searchEntity : searchEntities) {
        final String entityType = searchEntity.getEntity().getEntityType();
        final com.linkedin.metadata.models.EntitySpec entitySpec =
            entityRegistry.getEntitySpec(entityType);

        // Add canViewEntityPage to extraFields
        addCanViewEntityPageField(opContext, searchEntity);

        if (opContext.getSearchContext().isRestrictedSearch()) {
          if (VIEW_RESTRICTED_ENTITY_TYPES.contains(entityType)
              && !AuthUtil.canViewEntity(opContext, searchEntity.getEntity())) {

            // Not authorized && restricted response requested
            // Restrict entity
            searchEntity.setRestrictedAspects(
                new StringArray(List.of(entitySpec.getKeyAspectName())));

            searchEntity.setEntity(
                restrictedService.encryptRestrictedUrn(searchEntity.getEntity()));
          }
        }
      }
    } else {
      for (SearchEntity searchEntity : searchEntities) {
        addDefaultCanViewEntityPageField(searchEntity);
      }
    }
    return searchEntities;
  }

  private static void addCanViewEntityPageField(
      @Nonnull OperationContext opContext, @Nonnull SearchEntity searchEntity) {
    try {
      Urn entityUrn = searchEntity.getEntity();
      EntitySpec entitySpec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
      boolean canView =
          opContext.authorize("VIEW_ENTITY_PAGE", entitySpec).getType()
              == com.datahub.authorization.AuthorizationResult.Type.ALLOW;

      StringMap extraFields = searchEntity.getExtraFields();
      if (extraFields == null) {
        extraFields = new StringMap();
      }
      extraFields.put("canViewEntityPage", String.valueOf(canView));
      searchEntity.setExtraFields(extraFields);
    } catch (Exception e) {
      log.warn(
          "Failed to check VIEW_ENTITY_PAGE permission for entity {}, defaulting to true",
          searchEntity.getEntity(),
          e);
      addDefaultCanViewEntityPageField(searchEntity);
    }
  }

  private static void addDefaultCanViewEntityPageField(@Nonnull SearchEntity searchEntity) {
    StringMap extraFields = searchEntity.getExtraFields();
    if (extraFields == null) {
      extraFields = new StringMap();
    }
    extraFields.put("canViewEntityPage", "true");
    searchEntity.setExtraFields(extraFields);
  }

  public static boolean restrictUrn(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      return !AuthUtil.canViewEntity(opContext, urn);
    }
    return false;
  }
}
