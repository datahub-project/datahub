package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@Slf4j
public class DeprecationUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private DeprecationUtils() { }

  public static boolean isAuthorizedToUpdateDeprecationForEntity(@Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType()))
    ));

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
      EntityService entityService
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildUpdateDeprecationProposal(deprecated, note, decommissionTime, resource, actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  private static MetadataChangeProposal buildUpdateDeprecationProposal(
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) {
    Deprecation deprecation = (Deprecation) getAspectFromEntity(
        resource.getResourceUrn(),
        Constants.DEPRECATION_ASPECT_NAME,
        entityService,
        new Deprecation());
    deprecation.setActor(actor);
    deprecation.setDeprecated(deprecated);
    deprecation.setDecommissionTime(decommissionTime, SetMode.REMOVE_IF_NULL);
    if (note != null) {
      deprecation.setNote(note);
    } else {
      // Note is required field in GMS. Set to empty string if not provided.
      deprecation.setNote("");
    }
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), Constants.DEPRECATION_ASPECT_NAME, deprecation, actor, entityService);
  }

  private static void ingestChangeProposals(List<MetadataChangeProposal> changes, EntityService entityService, Urn actor) {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      entityService.ingestProposal(change, getAuditStamp(actor), false);
    }
  }
}