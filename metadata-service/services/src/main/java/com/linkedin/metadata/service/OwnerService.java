package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.datahub.authentication.Authentication;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OwnerService extends BaseService {

  public static final String SYSTEM_ID = "__system__";

  public OwnerService(
      @Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Batch adds a specific set of owners to a set of resources.
   *
   * @param ownerUrns the urns of the owners to add
   * @param resources references to the resources to change
   * @param ownershipType the ownership type to add
   */
  public void batchAddOwners(
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull OwnershipType ownershipType) {
    batchAddOwners(ownerUrns, resources, ownershipType, this.systemAuthentication);
  }

  /**
   * Batch adds a specific set of owners to a set of resources.
   *
   * @param ownerUrns the urns of the owners to add
   * @param resources references to the resources to change
   * @param ownershipType the ownership type to add
   * @param authentication authentication to use when making the change
   */
  public void batchAddOwners(
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull OwnershipType ownershipType,
      @Nonnull Authentication authentication) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      addOwnersToResources(ownerUrns, resources, ownershipType, authentication);
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
      @Nonnull List<Urn> ownerUrns, @Nonnull List<ResourceReference> resources) {
    batchRemoveOwners(ownerUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch removes a specific set of owners from a set of entities.
   *
   * @param ownerUrns the urns of the owners to remove
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   */
  public void batchRemoveOwners(
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull Authentication authentication) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      removeOwnersFromResources(ownerUrns, resources, authentication);
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
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Authentication authentication)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddOwnersProposals(ownerUrns, resources, ownershipType, authentication);
    ingestChangeProposals(changes, authentication);
  }

  private void removeOwnersFromResources(
      List<Urn> owners, List<ResourceReference> resources, Authentication authentication)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildRemoveOwnersProposals(owners, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddOwnersProposals(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Authentication authentication) {

    final Map<Urn, Ownership> ownershipAspects =
        getOwnershipAspects(
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Ownership(),
            authentication);

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
                .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
      }
      addOwnersIfNotExists(owners, ownerUrns, ownershipType);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners));
    }
    return proposals;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveOwnersProposals(
      List<Urn> ownerUrns, List<ResourceReference> resources, Authentication authentication) {
    final Map<Urn, Ownership> ownershipAspects =
        getOwnershipAspects(
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Ownership(),
            authentication);

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
}
