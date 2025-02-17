package com.linkedin.datahub.graphql.authorization;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;
import static com.datahub.authorization.AuthUtil.canViewEntity;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.MANAGE_ACCESS_TOKENS;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.codehaus.plexus.util.StringUtils;

@Slf4j
public class AuthorizationUtils {

  private static final String GRAPHQL_GENERATED_PACKAGE = "com.linkedin.datahub.graphql.generated";

  public static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  public static boolean canManageUsersAndGroups(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(),
        MANAGE,
        List.of(CORP_USER_ENTITY_NAME, CORP_GROUP_ENTITY_NAME));
  }

  public static boolean canManagePolicies(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(POLICY_ENTITY_NAME));
  }

  public static boolean canGeneratePersonalAccessToken(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
            context.getOperationContext(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE)
        || AuthUtil.isAuthorized(context.getOperationContext(), MANAGE_ACCESS_TOKENS);
  }

  public static boolean canManageTokens(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(ACCESS_TOKEN_ENTITY_NAME));
  }

  /**
   * Returns true if the current used is able to create Domains. This is true if the user has the
   * 'Manage Domains' or 'Create Domains' platform privilege.
   */
  public static boolean canCreateDomains(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.CREATE_DOMAINS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE.getType()))));

    return AuthUtil.isAuthorized(context.getOperationContext(), orPrivilegeGroups, null);
  }

  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  /**
   * Returns true if the current used is able to create Tags. This is true if the user has the
   * 'Manage Tags' or 'Create Tags' platform privilege.
   */
  public static boolean canCreateTags(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.CREATE_TAGS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_TAGS_PRIVILEGE.getType()))));

    return AuthUtil.isAuthorized(context.getOperationContext(), orPrivilegeGroups, null);
  }

  public static boolean canManageTags(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_TAGS_PRIVILEGE);
  }

  public static boolean canDeleteEntity(@Nonnull Urn entityUrn, @Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityUrns(
        context.getOperationContext(), DELETE, List.of(entityUrn));
  }

  public static boolean canManageUserCredentials(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE);
  }

  public static boolean canEditGroupMembers(
      @Nonnull String groupUrnStr, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE.getType()))));

    return isAuthorized(context, CORP_GROUP_ENTITY_NAME, groupUrnStr, orPrivilegeGroups);
  }

  public static boolean canCreateGlobalAnnouncements(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.CREATE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType()))));

    return AuthUtil.isAuthorized(context.getOperationContext(), orPrivilegeGroups, null);
  }

  public static boolean canManageGlobalAnnouncements(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE);
  }

  public static boolean canManageGlobalViews(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_GLOBAL_VIEWS);
  }

  public static boolean canManageOwnershipTypes(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_GLOBAL_OWNERSHIP_TYPES);
  }

  public static boolean canEditProperties(@Nonnull Urn targetUrn, @Nonnull QueryContext context) {
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean canEditEntityQueries(
      @Nonnull List<Urn> entityUrns, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType()))));
    return entityUrns.stream()
        .allMatch(
            entityUrn ->
                isAuthorized(
                    context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups));
  }

  public static boolean canCreateQuery(
      @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to create a query.
    return canEditEntityQueries(subjectUrns, context);
  }

  public static boolean canUpdateQuery(
      @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to update any query.
    return canEditEntityQueries(subjectUrns, context);
  }

  public static boolean canDeleteQuery(
      @Nonnull Urn entityUrn, @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to remove any query.
    return canEditEntityQueries(subjectUrns, context);
  }

  /**
   * Can view relationship logic goes here. Should be considered directionless for now. Or direction
   * added to the interface.
   *
   * @param opContext
   * @param a
   * @param b
   * @return
   */
  public static boolean canViewRelationship(
      @Nonnull OperationContext opContext, @Nonnull Urn a, @Nonnull Urn b) {
    // TODO  relationships filter
    return true;
  }

  /*
   * Optionally check view permissions against a list of urns if the config option is enabled
   */
  public static boolean canView(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    // if search authorization is disabled, skip the view permission check
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()
        && VIEW_RESTRICTED_ENTITY_TYPES.contains(urn.getEntityType())) {

      return canViewEntity(opContext, urn);
    }
    return true;
  }

  public static <T> T restrictEntity(@Nonnull Object entity, Class<T> clazz) {
    List<Field> allFields = FieldUtils.getAllFieldsList(entity.getClass());
    try {
      Object[] args =
          allFields.stream()
              // New versions of graphql.codegen generate serialVersionUID
              // We need to filter serialVersionUID out because serialVersionUID is
              // never part of the entity type constructor
              .filter(field -> !field.getName().contains("serialVersionUID"))
              .map(
                  field -> {
                    // properties are often not required but only because
                    // they are a `one of` non-null.
                    // i.e. ChartProperties or ChartEditableProperties are required.
                    if (field.getAnnotation(javax.annotation.Nonnull.class) != null
                        || field.getName().toLowerCase().contains("properties")
                        || field.getType().isPrimitive()) {
                      try {
                        switch (field.getName()) {
                            // pass through to the restricted entity
                          case "name":
                          case "type":
                          case "urn":
                          case "chartId":
                          case "id":
                          case "jobId":
                          case "flowId":
                            Method fieldGetter =
                                MethodUtils.getMatchingMethod(
                                    entity.getClass(),
                                    "get" + StringUtils.capitalise(field.getName()));
                            return fieldGetter.invoke(entity, (Object[]) null);
                          default:
                            switch (field.getType().getSimpleName()) {
                              case "boolean":
                              case "Boolean":
                                Method boolGetter =
                                    MethodUtils.getMatchingMethod(
                                        entity.getClass(),
                                        "get" + StringUtils.capitalise(field.getName()));
                                return Boolean.TRUE.equals(
                                    boolGetter.invoke(entity, (Object[]) null));
                                // mask these fields in the restricted entity
                              case "char":
                              case "String":
                                return "";
                              case "short":
                              case "Short":
                              case "int":
                              case "Integer":
                                return 0;
                              case "long":
                              case "Long":
                                return 0L;
                              case "float":
                              case "Float":
                                return 0F;
                              case "double":
                              case "Double":
                                return 0D;
                              case "List":
                                return List.of();
                              default:
                                if (Enum.class.isAssignableFrom(field.getType())) {
                                  // pass through enum
                                  Method enumGetter =
                                      MethodUtils.getMatchingMethod(
                                          entity.getClass(),
                                          "get" + StringUtils.capitalise(field.getName()));
                                  return enumGetter.invoke(entity, (Object[]) null);
                                } else if (entity
                                    .getClass()
                                    .getPackage()
                                    .getName()
                                    .contains(GRAPHQL_GENERATED_PACKAGE)) {
                                  // handle nested fields recursively
                                  Method getter =
                                      MethodUtils.getMatchingMethod(
                                          entity.getClass(),
                                          "get" + StringUtils.capitalise(field.getName()));
                                  Object nestedEntity = getter.invoke(entity, (Object[]) null);
                                  if (nestedEntity == null) {
                                    return null;
                                  } else {
                                    return restrictEntity(nestedEntity, getter.getReturnType());
                                  }
                                }
                                log.error(
                                    String.format(
                                        "Failed to resolve non-null field: Object:%s Field:%s FieldType: %s",
                                        entity.getClass().getName(),
                                        field.getName(),
                                        field.getType().getName()));
                            }
                        }
                      } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                      }
                    }
                    return (Object) null;
                  })
              .toArray();
      return ConstructorUtils.invokeConstructor(clazz, args);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean canManageStructuredProperties(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_STRUCTURED_PROPERTIES_PRIVILEGE);
  }

  public static boolean canViewStructuredPropertiesPage(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.VIEW_STRUCTURED_PROPERTIES_PAGE_PRIVILEGE);
  }

  public static boolean canManageForms(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_DOCUMENTATION_FORMS_PRIVILEGE);
  }

  public static boolean canManageFeatures(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_FEATURES_PRIVILEGE);
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    final EntitySpec resourceSpec = new EntitySpec(resourceType, resource);
    return AuthUtil.isAuthorized(context.getOperationContext(), privilegeGroup, resourceSpec);
  }

  public static boolean isViewDatasetUsageAuthorized(
      final QueryContext context, final Urn resourceUrn) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE,
        new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()));
  }

  public static boolean isViewDatasetProfileAuthorized(
      final QueryContext context, final Urn resourceUrn) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(),
        PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE,
        new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()));
  }

  public static boolean isViewDatasetOperationsAuthorized(
      final QueryContext context, final Urn resourceUrn) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(),
        PoliciesConfig.VIEW_DATASET_OPERATIONS_PRIVILEGE,
        new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()));
  }

  private AuthorizationUtils() {}
}
