package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


// TODO: Move to consuming from OwnerService
@Slf4j
public class OwnerUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private OwnerUtils() { }

  public static void addOwnersToResources(
      List<OwnerInput> owners,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildAddOwnersProposal(owners, UrnUtils.getUrn(resource.getResourceUrn()), actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  public static void removeOwnersFromResources(
      List<Urn> ownerUrns,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildRemoveOwnersProposal(ownerUrns, UrnUtils.getUrn(resource.getResourceUrn()), actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }


  private static MetadataChangeProposal buildAddOwnersProposal(List<OwnerInput> owners, Urn resourceUrn, Urn actor, EntityService entityService) {
    Ownership ownershipAspect = (Ownership) getAspectFromEntity(
        resourceUrn.toString(),
        Constants.OWNERSHIP_ASPECT_NAME,
        entityService,
        new Ownership());
    for (OwnerInput input : owners) {
      addOwner(ownershipAspect, UrnUtils.getUrn(input.getOwnerUrn()), OwnershipType.valueOf(input.getType().toString()));
    }
    return buildMetadataChangeProposal(resourceUrn, Constants.OWNERSHIP_ASPECT_NAME, ownershipAspect, actor, entityService);
  }

  public static MetadataChangeProposal buildRemoveOwnersProposal(
      List<Urn> ownerUrns,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    Ownership ownershipAspect = (Ownership) MutationUtils.getAspectFromEntity(
        resourceUrn.toString(),
        Constants.OWNERSHIP_ASPECT_NAME,
        entityService,
        new Ownership());
    ownershipAspect.setLastModified(getAuditStamp(actor));
    removeOwnersIfExists(ownershipAspect, ownerUrns);
    return buildMetadataChangeProposal(resourceUrn, Constants.OWNERSHIP_ASPECT_NAME, ownershipAspect, actor, entityService);
  }

  private static void addOwner(Ownership ownershipAspect, Urn ownerUrn, OwnershipType type) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    final OwnerArray ownerArray = new OwnerArray(ownershipAspect.getOwners()
        .stream()
        .filter(owner ->  !owner.getOwner().equals(ownerUrn))
        .collect(Collectors.toList()));

    Owner newOwner = new Owner();
    newOwner.setType(type);
    newOwner.setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL));
    newOwner.setOwner(ownerUrn);
    ownerArray.add(newOwner);
    ownershipAspect.setOwners(ownerArray);
  }

  private static void removeOwnersIfExists(Ownership ownership, List<Urn> ownerUrns) {
    if (!ownership.hasOwners()) {
      ownership.setOwners(new OwnerArray());
    }

    OwnerArray ownerArray = ownership.getOwners();
    for (Urn ownerUrn : ownerUrns) {
      ownerArray.removeIf(owner -> owner.getOwner().equals(ownerUrn));
    }
  }

  public static boolean isAuthorizedToUpdateOwners(@Nonnull QueryContext context, Urn resourceUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        resourceUrn.getEntityType(),
        resourceUrn.toString(),
        orPrivilegeGroups);
  }

  public static Boolean validateAddInput(
      List<OwnerInput> owners,
      Urn resourceUrn,
      EntityService entityService
  ) {
    for (OwnerInput owner : owners) {
      boolean result = validateAddInput(
          UrnUtils.getUrn(owner.getOwnerUrn()),
          owner.getOwnerEntityType(),
          resourceUrn,
          entityService);
      if (!result) {
        return false;
      }
    }
    return true;
  }

  public static Boolean validateAddInput(
      Urn ownerUrn,
      OwnerEntityType ownerEntityType,
      Urn resourceUrn,
      EntityService entityService
  ) {

    if (OwnerEntityType.CORP_GROUP.equals(ownerEntityType) && !Constants.CORP_GROUP_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Expected a corp group urn.", resourceUrn));
    }

    if (OwnerEntityType.CORP_USER.equals(ownerEntityType) && !Constants.CORP_USER_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Expected a corp user urn.", resourceUrn));
    }

    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Resource does not exist.", resourceUrn));
    }

    if (!entityService.exists(ownerUrn)) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Owner does not exist.", resourceUrn));
    }

    return true;
  }

  public static void validateOwner(
      Urn ownerUrn,
      OwnerEntityType ownerEntityType,
      EntityService entityService
  ) {
    if (OwnerEntityType.CORP_GROUP.equals(ownerEntityType) && !Constants.CORP_GROUP_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Failed to change ownership for resource(s). Expected a corp group urn, found %s", ownerUrn));
    }

    if (OwnerEntityType.CORP_USER.equals(ownerEntityType) && !Constants.CORP_USER_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Failed to change ownership for resource(s). Expected a corp user urn, found %s.", ownerUrn));
    }

    if (!entityService.exists(ownerUrn)) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource(s). Owner with urn %s does not exist.", ownerUrn));
    }
  }

  public static Boolean validateRemoveInput(
      Urn resourceUrn,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Resource does not exist.", resourceUrn));
    }
    return true;
  }

  private static void ingestChangeProposals(List<MetadataChangeProposal> changes, EntityService entityService, Urn actor) {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      entityService.ingestProposal(change, getAuditStamp(actor), false);
    }
  }
}
