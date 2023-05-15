package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
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
  public static final String SYSTEM_ID = "__system__";

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
      addOwner(ownershipAspect, UrnUtils.getUrn(input.getOwnerUrn()), input.getType(), UrnUtils.getUrn(input.getOwnershipTypeUrn()));
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

  private static void addOwner(Ownership ownershipAspect, Urn ownerUrn, OwnershipType type, Urn ownershipUrn) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    final OwnerArray ownerArray = new OwnerArray(ownershipAspect.getOwners()
        .stream()
        // Fix ownership in place
        .filter(owner -> !(owner.getOwner().equals(ownerUrn) && (
            (owner.getTypeUrn() != null && owner.getTypeUrn().equals(ownershipUrn)) || (owner.getType() != null
                && owner.getType().equals(com.linkedin.common.OwnershipType.valueOf(type.toString())))
        )))
        .collect(Collectors.toList()));

    Owner newOwner = new Owner();

    // For backwards compatibility we have to always set the deprecated type.
    // If the type exists we assume it's an old ownership type that we can map to.
    // Else if it's a net new custom ownership type set old type to CUSTOM.
    com.linkedin.common.OwnershipType gmsType = type != null ? com.linkedin.common.OwnershipType.valueOf(type.toString())
        : com.linkedin.common.OwnershipType.CUSTOM;

    newOwner.setType(gmsType);
    newOwner.setTypeUrn(ownershipUrn);
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
          owner.getOwnershipTypeUrn(),
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
      String ownershipEntityUrn,
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
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource %s. Owner %s does not exist.", resourceUrn, ownerUrn));
    }

    if (ownershipEntityUrn != null && !entityService.exists(UrnUtils.getUrn(ownershipEntityUrn))) {
      throw new IllegalArgumentException(String.format("Failed to change ownership type for resource %s. Ownership Type "
          + "%s does not exist.", resourceUrn, ownershipEntityUrn));
    }

    return true;
  }

  public static void validateOwner(
      Urn ownerUrn,
      OwnerEntityType ownerEntityType,
      Urn ownershipEntityUrn,
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

    if (!entityService.exists(ownershipEntityUrn)) {
      throw new IllegalArgumentException(String.format("Failed to change ownership for resource(s). Ownership type with "
          + "urn %s does not exist.", ownershipEntityUrn));
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

  public static void addCreatorAsOwner(
    QueryContext context,
    String urn,
    OwnerEntityType ownerEntityType,
    OwnershipType ownershipType,
    EntityService entityService) {
    try {
      Urn actorUrn = CorpuserUrn.createFromString(context.getActorUrn());
      String ownershipTypeUrn = mapOwnershipTypeToEntity(ownershipType);

      if (!entityService.exists(UrnUtils.getUrn(ownershipTypeUrn))) {
        throw new RuntimeException(String.format("Unknown ownership type urn %s", ownershipTypeUrn));
      }

      addOwnersToResources(
          ImmutableList.of(new OwnerInput(actorUrn.toString(), ownerEntityType, ownershipType, ownershipTypeUrn)),
          ImmutableList.of(new ResourceRefInput(urn, null, null)),
          actorUrn,
          entityService
      );
    } catch (Exception e) {
      log.error(String.format("Failed to add creator as owner of tag %s", urn), e);
    }
  }

  public static String mapOwnershipTypeToEntity(OwnershipType type) {
    final String typeName = SYSTEM_ID + type.name().toLowerCase();
    return Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, typeName).toString();
  }
}
