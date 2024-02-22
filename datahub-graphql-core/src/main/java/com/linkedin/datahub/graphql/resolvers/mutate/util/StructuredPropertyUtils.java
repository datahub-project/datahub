package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.structured.PrimitivePropertyValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StructuredPropertyUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private StructuredPropertyUtils() {}

  @Nullable
  public static PrimitivePropertyValue mapPropertyValueInput(
      @Nonnull final PropertyValueInput valueInput) {
    if (valueInput.getStringValue() != null) {
      return PrimitivePropertyValue.create(valueInput.getStringValue());
    } else if (valueInput.getNumberValue() != null) {
      return PrimitivePropertyValue.create(valueInput.getNumberValue().doubleValue());
    }
    return null;
  }

  public static boolean isAuthorizedToUpdateProperties(
      @Nonnull QueryContext context, @Nonnull Urn targetUrn) {
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }
}
