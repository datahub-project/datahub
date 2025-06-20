package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OwnerService extends BaseService {

  public static final String SYSTEM_ID = "__system__";

  public OwnerService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Adds owners for a particular entity.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity
   * @param owners the owners to add
   */
  public void addOwners(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners)
      throws Exception {

    // First, group by ownership type + ownership type urn
    final Map<String, List<Owner>> ownersByTypeAndUrn =
        owners.stream()
            .collect(
                Collectors.groupingBy(
                    owner ->
                        owner.getType().toString()
                            + (owner.hasTypeUrn() ? owner.getTypeUrn().toString() : ""),
                    Collectors.toList()));

    // Then, for each group, add owners to resources
    for (Map.Entry<String, List<Owner>> entry : ownersByTypeAndUrn.entrySet()) {
      final List<Owner> ownerGroup = entry.getValue();
      final OwnershipType ownershipType = ownerGroup.get(0).getType();
      final Urn ownershipTypeUrn =
          ownerGroup.get(0).hasTypeUrn() ? ownerGroup.get(0).getTypeUrn() : null;
      final List<Urn> ownerUrns =
          ownerGroup.stream().map(Owner::getOwner).collect(Collectors.toList());

      final List<ResourceReference> resources =
          Collections.singletonList(new ResourceReference(entityUrn, null, null));
      addOwnersToResources(opContext, ownerUrns, resources, ownershipType);
    }
  }

  /**
   * Batch adds a specific set of owners to a set of resources.
   *
   * @param ownerUrns the urns of the owners to add
   * @param resources references to the resources to change
   * @param ownershipType the ownership type to add
   */
  public void batchAddOwners(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull OwnershipType ownershipType) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      addOwnersToResources(opContext, ownerUrns, resources, ownershipType);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Owners %s to resources with urns %s!",
              ownerUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch removes a specific set of owners from a set of resources.
   *
   * @param ownerUrns the urns of the owners to remove
   * @param resources references to the resources to change
   */
  public void batchRemoveOwners(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      removeOwnersFromResources(opContext, ownerUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Owners %s to resources with urns %s!",
              ownerUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addOwnersToResources(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddOwnersProposals(opContext, ownerUrns, resources, ownershipType);
    ingestChangeProposals(opContext, changes);
  }

  private void removeOwnersFromResources(
      @Nonnull OperationContext opContext, List<Urn> owners, List<ResourceReference> resources)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildRemoveOwnersProposals(opContext, owners, resources);
    ingestChangeProposals(opContext, changes);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddOwnersProposals(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType) {

    final Map<Urn, Ownership> ownershipAspects =
        getOwnershipAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Ownership());

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (ResourceReference resource : resources) {
      com.linkedin.common.Ownership owners = ownershipAspects.get(resource.getUrn());

      if (owners == null) {
        return null;
      }

      if (!owners.hasOwners()) {
        owners.setOwners(new OwnerArray());
        owners.setLastModified(
            new AuditStamp()
                .setTime(System.currentTimeMillis())
                .setActor(
                    UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));
      }
      addOwnersIfNotExists(owners, ownerUrns, ownershipType);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners));
    }
    return proposals;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveOwnersProposals(
      @Nonnull OperationContext opContext, List<Urn> ownerUrns, List<ResourceReference> resources) {
    final Map<Urn, Ownership> ownershipAspects =
        getOwnershipAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Ownership());

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (ResourceReference resource : resources) {
      final Ownership owners = ownershipAspects.get(resource.getUrn());
      if (owners == null) {
        return null;
      }
      if (!owners.hasOwners()) {
        owners.setOwners(new OwnerArray());
      }
      removeOwnersIfExists(owners, ownerUrns);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners));
    }

    return proposals;
  }

  private void addOwnersIfNotExists(
      Ownership owners, List<Urn> ownerUrns, OwnershipType ownershipType) {
    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
    }

    OwnerArray ownerAssociationArray = owners.getOwners();

    List<Urn> ownersToAdd = new ArrayList<>();
    for (Urn ownerUrn : ownerUrns) {
      if (ownerAssociationArray.stream()
          .anyMatch(association -> association.getOwner().equals(ownerUrn))) {
        continue;
      }
      ownersToAdd.add(ownerUrn);
    }

    // Check for no owners to add
    if (ownersToAdd.size() == 0) {
      return;
    }

    for (Urn ownerUrn : ownersToAdd) {
      Owner newOwner = new Owner();
      newOwner.setOwner(ownerUrn);
      newOwner.setTypeUrn(mapOwnershipTypeToEntity(OwnershipType.NONE.name()));
      newOwner.setType(ownershipType);
      ownerAssociationArray.add(newOwner);
    }
  }

  @VisibleForTesting
  static Urn mapOwnershipTypeToEntity(String type) {
    final String typeName = SYSTEM_ID + type.toLowerCase();
    return Urn.createFromTuple(Constants.OWNERSHIP_TYPE_ENTITY_NAME, typeName);
  }

  private static OwnerArray removeOwnersIfExists(Ownership owners, List<Urn> ownerUrns) {
    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
    }
    OwnerArray ownerAssociationArray = owners.getOwners();
    for (Urn ownerUrn : ownerUrns) {
      ownerAssociationArray.removeIf(association -> association.getOwner().equals(ownerUrn));
    }
    return ownerAssociationArray;
  }

  /**
   * Fetches the owners for an entity.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity
   */
  public List<Owner> getEntityOwners(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {
    final Ownership maybeOwnership = getOwnership(opContext, entityUrn);
    if (maybeOwnership != null) {
      return maybeOwnership.getOwners();
    }
    return Collections.emptyList();
  }

  @Nullable
  private Ownership getOwnership(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse response = getOwnershipEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(OWNERSHIP_ASPECT_NAME)) {
      return new Ownership(response.getAspects().get(OWNERSHIP_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getOwnershipEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {
    return this.entityClient.getV2(
        opContext, entityUrn.getEntityType(), entityUrn, ImmutableSet.of(OWNERSHIP_ASPECT_NAME));
  }
}
