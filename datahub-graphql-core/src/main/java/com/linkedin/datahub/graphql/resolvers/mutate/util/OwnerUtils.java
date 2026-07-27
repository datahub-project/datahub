package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
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
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.service.util.OwnerServiceUtils;
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
      final OwnershipType ownershipType =
          input.getType() != null
              ? OwnershipType.valueOf(input.getType().toString())
              : OwnershipType.NONE;
      final Urn ownershipTypeUrn =
          input.getOwnershipTypeUrn() != null
              ? UrnUtils.getUrn(input.getOwnershipTypeUrn())
              : UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name()));
      OwnerServiceUtils.addOwnerToAspect(
          ownershipAspect,
          UrnUtils.getUrn(input.getOwnerUrn()),
          com.linkedin.common.OwnershipType.valueOf(ownershipType.toString()),
          ownershipTypeUrn,
          OwnershipSourceType.MANUAL);
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

  private static void removeOwnersIfExists(
      Ownership ownershipAspect, List<Urn> ownerUrns, Urn ownershipTypeUrn) {
    if (!ownershipAspect.hasOwners()) {
      ownershipAspect.setOwners(new OwnerArray());
    }

    OwnerArray ownerArray = ownershipAspect.getOwners();
    for (Urn ownerUrn : ownerUrns) {
      OwnerServiceUtils.removeExistingOwnerIfExists(ownerArray, ownerUrn, ownershipTypeUrn);
    }
  }

  public static void validateAuthorizedToUpdateOwners(
      @Nonnull QueryContext context,
      Urn resourceUrn,
      EntityClient entityClient,
      EntityService<?> entityService) {

    if (GlossaryUtils.canUpdateGlossaryEntity(resourceUrn, context, entityClient)) {
      return;
    }

    if (isAuthorizedToUpdateOwners(context, resourceUrn)) {
      return;
    }

    // Assertions are scoped to their assertee: actors who can edit assertions or owners on the
    // assertee can also manage assertion ownership.
    if (Constants.ASSERTION_ENTITY_NAME.equals(resourceUrn.getEntityType())
        && isAuthorizedToUpdateOwnersFromAssertee(context, resourceUrn, entityService)) {
      return;
    }

    throw new AuthorizationException(
        "Unauthorized to update owners. Please contact your DataHub administrator.");
  }

  /**
   * For an assertion URN, fetch the assertee and check whether the actor can edit owners or
   * assertions on it. Mirrors the precedent set by {@link
   * AssertionUtils#isAuthorizedToEditAssertionFromAssertee} for assertion CRUD: assertions are
   * managed via their assertee's permissions, not their own URN.
   */
  private static boolean isAuthorizedToUpdateOwnersFromAssertee(
      @Nonnull QueryContext context,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityService<?> entityService) {
    final AssertionInfo info =
        (AssertionInfo)
            EntityUtils.getAspectFromEntity(
                context.getOperationContext(),
                assertionUrn.toString(),
                Constants.ASSERTION_INFO_ASPECT_NAME,
                entityService,
                null);
    if (info == null) {
      return false;
    }
    final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    return isAuthorizedToUpdateOwners(context, asserteeUrn)
        || AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn);
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

  /**
   * Whether the actor can edit owners on assertions belonging to {@code entityUrn}. Used to gate UI
   * affordances at the parent-entity level before any specific assertion URN exists or is known.
   *
   * <p>Mirrors the assertee-delegation logic in {@link #validateAuthorizedToUpdateOwners}: anyone
   * who can edit owners or edit assertions on the parent entity can manage assertion ownership.
   */
  public static boolean isAuthorizedToUpdateAssertionOwnersOnEntity(
      @Nonnull QueryContext context, @Nonnull Urn entityUrn) {
    return isAuthorizedToUpdateOwners(context, entityUrn)
        || AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, entityUrn);
  }
}
