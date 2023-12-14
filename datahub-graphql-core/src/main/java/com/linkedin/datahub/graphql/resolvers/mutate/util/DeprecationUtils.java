package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.aspect.utils.DeprecationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeprecationUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private DeprecationUtils() {}

  public static boolean isAuthorizedToUpdateDeprecationForEntity(
      @Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static void updateDeprecationForResources(
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(
          buildUpdateDeprecationProposal(
              deprecated, note, decommissionTime, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildUpdateDeprecationProposal(
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService) {
    String resourceUrn = resource.getResourceUrn();
    Deprecation deprecation =
        getDeprecation(entityService, resourceUrn, actor, note, deprecated, decommissionTime);
    return MutationUtils.buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resourceUrn), Constants.DEPRECATION_ASPECT_NAME, deprecation);
  }
}
