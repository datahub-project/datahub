package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import  com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
public class OwnerService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public OwnerService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  /**
   * Batch adds a specific set of owners to a set of resources.
   *
   * @param ownerUrns the urns of the owners to add
   * @param resources references to the resources to change
   * @param ownershipType the ownership type to add
   */
  public void batchAddOwners(@Nonnull List<Urn> ownerUrns, @Nonnull List<ResourceReference> resources, @Nonnull OwnershipType ownershipType) {
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
      throw new RuntimeException(String.format("Failed to batch add Owners %s to resources with urns %s!",
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
  public void batchRemoveOwners(@Nonnull List<Urn> ownerUrns, @Nonnull List<ResourceReference> resources) {
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
      throw new RuntimeException(String.format("Failed to batch add Owners %s to resources with urns %s!",
          ownerUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addOwnersToResources(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddOwnersProposal(ownerUrns, entity, ownershipType, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  private void removeOwnersFromResources(
      List<Urn> owners,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveOwnersProposal(owners, resource, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildAddOwnersProposal(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      ResourceReference resource,
      OwnershipType ownershipType,
      Authentication authentication
  ) throws URISyntaxException {
    com.linkedin.common.Ownership owners = getOwnershipAspect(
        resource.getUrn(),
        new Ownership(),
        authentication);

    if (owners == null) {
      return null;
    }

    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
      owners.setLastModified(new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
      );
    }
    addOwnersIfNotExists(owners, ownerUrns, ownershipType);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners);
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildRemoveOwnersProposal(
      List<Urn> ownerUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    com.linkedin.common.Ownership owners = getOwnershipAspect(
        resource.getUrn(),
        new Ownership(),
        authentication);

    if (owners == null) {
      return null;
    }

    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
    }
    removeOwnersIfExists(owners, ownerUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.OWNERSHIP_ASPECT_NAME, owners
    );
  }

  private void addOwnersIfNotExists(Ownership owners, List<Urn> ownerUrns, OwnershipType ownershipType) throws URISyntaxException {
    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
    }

    OwnerArray ownerAssociationArray = owners.getOwners();

    List<Urn> ownersToAdd = new ArrayList<>();
    for (Urn ownerUrn : ownerUrns) {
      if (ownerAssociationArray.stream().anyMatch(association -> association.getOwner().equals(ownerUrn))) {
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
      newOwner.setType(ownershipType);
      ownerAssociationArray.add(newOwner);
    }
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

  @Nullable
  private Ownership getOwnershipAspect(@Nonnull Urn entityUrn, @Nonnull Ownership defaultValue, @Nonnull Authentication authentication) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.OWNERSHIP_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      if (aspect == null) {
        return defaultValue;
      }
      return new Ownership(aspect.data());
    } catch (Exception e) {
      log.error(
          "Error retrieving owners for entity. Entity: {} aspect: {}",
          entityUrn,
          Constants.OWNERSHIP_ASPECT_NAME,
          e);
      return null;
    }
  }

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes, Authentication authentication) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, authentication);
    }
  }
}
