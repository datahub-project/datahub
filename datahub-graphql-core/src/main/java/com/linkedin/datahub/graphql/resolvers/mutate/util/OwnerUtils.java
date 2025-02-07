package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

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
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

// TODO: Move to consuming from OwnerService
@Slf4j
public class OwnerUtils {

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));
  public static final String SYSTEM_ID = "__system__";

  private OwnerUtils() {}

  public static void addOwnersToResources(
      @Nonnull OperationContext opContext,
      List<OwnerInput> ownerInputs,
      List<ResourceRefInput> resourceRefs,
      Urn actorUrn,
      EntityService entityService) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resourceRefs) {
      changes.add(
          buildAddOwnersProposal(
              opContext,
              ownerInputs,
              UrnUtils.getUrn(resource.getResourceUrn()),
              actorUrn,
              entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actorUrn, false);
  }

  public static void removeOwnersFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> ownerUrns,
      @Nullable Urn ownershipTypeUrn,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(
          buildRemoveOwnersProposal(
              opContext,
              ownerUrns,
              ownershipTypeUrn,
              UrnUtils.getUrn(resource.getResourceUrn()),
              actor,
              entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  static MetadataChangeProposal buildAddOwnersProposal(
      @Nonnull OperationContext opContext,
      List<OwnerInput> owners,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    Ownership ownershipAspect =
        (Ownership)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.OWNERSHIP_ASPECT_NAME,
                entityService,
                new Ownership());
    ownershipAspect.setLastModified(EntityUtils.getAuditStamp(actor));
    for (OwnerInput input : owners) {
      addOwnerToAspect(
          ownershipAspect,
          UrnUtils.getUrn(input.getOwnerUrn()),
          input.getType(),
          UrnUtils.getUrn(input.getOwnershipTypeUrn()));
    }
    return buildMetadataChangeProposalWithUrn(
        resourceUrn, Constants.OWNERSHIP_ASPECT_NAME, ownershipAspect);
  }

  public static MetadataChangeProposal buildRemoveOwnersProposal(
      @Nonnull OperationContext opContext,
      List<Urn> ownerUrns,
      @Nullable Urn ownershipTypeUrn,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    Ownership ownershipAspect =
        (Ownership)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.OWNERSHIP_ASPECT_NAME,
                entityService,
                new Ownership());
    ownershipAspect.setLastModified(EntityUtils.getAuditStamp(actor));
    removeOwnersIfExists(ownershipAspect, ownerUrns, ownershipTypeUrn);
    return buildMetadataChangeProposalWithUrn(
        resourceUrn, Constants.OWNERSHIP_ASPECT_NAME, ownershipAspect);
  }

  private static void addOwnerToAspect(
      Ownership ownershipAspect, Urn ownerUrn, OwnershipType type, Urn ownershipTypeUrn) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    OwnerArray ownerArray = new OwnerArray(ownershipAspect.getOwners());
    removeExistingOwnerIfExists(ownerArray, ownerUrn, ownershipTypeUrn);

    Owner newOwner = new Owner();

    // For backwards compatibility we have to always set the deprecated type.
    // If the type exists we assume it's an old ownership type that we can map to.
    // Else if it's a net new custom ownership type set old type to CUSTOM.
    com.linkedin.common.OwnershipType gmsType =
        type != null
            ? com.linkedin.common.OwnershipType.valueOf(type.toString())
            : com.linkedin.common.OwnershipType.CUSTOM;

    newOwner.setType(gmsType);
    newOwner.setTypeUrn(ownershipTypeUrn);
    newOwner.setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL));
    newOwner.setOwner(ownerUrn);
    ownerArray.add(newOwner);
    ownershipAspect.setOwners(ownerArray);
  }

  private static void removeExistingOwnerIfExists(
      OwnerArray ownerArray, Urn ownerUrn, Urn ownershipTypeUrn) {
    ownerArray.removeIf(
        owner -> {
          // Remove old ownership if it exists (check ownerUrn + type (entity & deprecated type))
          return isOwnerEqual(owner, ownerUrn, ownershipTypeUrn);
        });
  }

  public static boolean isOwnerEqual(
      @Nonnull Owner owner, @Nonnull Urn ownerUrn, @Nullable Urn ownershipTypeUrn) {
    if (!owner.getOwner().equals(ownerUrn)) {
      return false;
    }
    if (owner.getTypeUrn() != null && ownershipTypeUrn != null) {
      return owner.getTypeUrn().equals(ownershipTypeUrn);
    }
    if (ownershipTypeUrn == null) {
      return true;
    }
    // Fall back to mapping deprecated type to the new ownership entity
    return mapOwnershipTypeToEntity(OwnershipType.valueOf(owner.getType().toString()).name())
        .equals(ownershipTypeUrn.toString());
  }

  private static void removeOwnersIfExists(
      Ownership ownershipAspect, List<Urn> ownerUrns, Urn ownershipTypeUrn) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    OwnerArray ownerArray = ownershipAspect.getOwners();
    for (Urn ownerUrn : ownerUrns) {
      removeExistingOwnerIfExists(ownerArray, ownerUrn, ownershipTypeUrn);
    }
  }

  public static void validateAuthorizedToUpdateOwners(
      @Nonnull QueryContext context, Urn resourceUrn, EntityClient entityClient) {

    if (GlossaryUtils.canUpdateGlossaryEntity(resourceUrn, context, entityClient)) {
      return;
    }

    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType()))));

    boolean authorized =
        AuthorizationUtils.isAuthorized(
            context, resourceUrn.getEntityType(), resourceUrn.toString(), orPrivilegeGroups);
    if (!authorized) {
      throw new AuthorizationException(
          "Unauthorized to update owners. Please contact your DataHub administrator.");
    }
  }

  public static void validateAddOwnerInput(
      @Nonnull OperationContext opContext,
      List<OwnerInput> owners,
      Urn resourceUrn,
      EntityService<?> entityService) {
    for (OwnerInput owner : owners) {
      validateAddOwnerInput(opContext, owner, resourceUrn, entityService);
    }
  }

  public static void validateAddOwnerInput(
      @Nonnull OperationContext opContext,
      OwnerInput owner,
      Urn resourceUrn,
      EntityService<?> entityService) {

    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource %s. Resource does not exist.", resourceUrn));
    }

    validateOwner(opContext, owner, entityService);
  }

  public static void validateOwner(
      @Nonnull OperationContext opContext, OwnerInput owner, EntityService<?> entityService) {

    OwnerEntityType ownerEntityType = owner.getOwnerEntityType();
    Urn ownerUrn = UrnUtils.getUrn(owner.getOwnerUrn());

    if (OwnerEntityType.CORP_GROUP.equals(ownerEntityType)
        && !Constants.CORP_GROUP_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource(s). Expected a corp group urn, found %s",
              ownerUrn));
    }

    if (OwnerEntityType.CORP_USER.equals(ownerEntityType)
        && !Constants.CORP_USER_ENTITY_NAME.equals(ownerUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource(s). Expected a corp user urn, found %s.",
              ownerUrn));
    }

    if (!entityService.exists(opContext, ownerUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource(s). Owner with urn %s does not exist.",
              ownerUrn));
    }

    if (owner.getOwnershipTypeUrn() != null
        && !entityService.exists(opContext, UrnUtils.getUrn(owner.getOwnershipTypeUrn()), true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource(s). Custom Ownership type with "
                  + "urn %s does not exist.",
              owner.getOwnershipTypeUrn()));
    }

    if (owner.getType() == null && owner.getOwnershipTypeUrn() == null) {
      throw new IllegalArgumentException(
          "Failed to change ownership for resource(s). Expected either "
              + "type or ownershipTypeUrn to be specified.");
    }
  }

  public static void validateRemoveInput(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to change ownership for resource %s. Resource does not exist.", resourceUrn));
    }
  }

  public static void addCreatorAsOwner(
      QueryContext context,
      String urn,
      OwnerEntityType ownerEntityType,
      EntityService<?> entityService) {
    try {
      Urn actorUrn = CorpuserUrn.createFromString(context.getActorUrn());
      OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;
      if (!entityService.exists(
          context.getOperationContext(),
          UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name())),
          true)) {
        log.warn("Technical owner does not exist, defaulting to None ownership.");
        ownershipType = OwnershipType.NONE;
      }
      String ownershipTypeUrn = mapOwnershipTypeToEntity(ownershipType.name());

      if (!entityService.exists(
          context.getOperationContext(), UrnUtils.getUrn(ownershipTypeUrn), true)) {
        throw new RuntimeException(
            String.format("Unknown ownership type urn %s", ownershipTypeUrn));
      }

      addOwnersToResources(
          context.getOperationContext(),
          ImmutableList.of(
              new OwnerInput(
                  actorUrn.toString(), ownerEntityType, ownershipType, ownershipTypeUrn)),
          ImmutableList.of(new ResourceRefInput(urn, null, null)),
          actorUrn,
          entityService);
    } catch (Exception e) {
      log.error(String.format("Failed to add creator as owner of tag %s", urn), e);
    }
  }

  public static String mapOwnershipTypeToEntity(String type) {
    final String typeName = SYSTEM_ID + type.toLowerCase();
    return Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, typeName).toString();
  }

  public static boolean isAuthorizedToUpdateOwners(@Nonnull QueryContext context, Urn resourceUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(
        context, resourceUrn.getEntityType(), resourceUrn.toString(), orPrivilegeGroups);
  }
}
