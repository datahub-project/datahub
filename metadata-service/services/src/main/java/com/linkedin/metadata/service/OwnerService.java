package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.util.OwnerServiceUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OwnerService extends BaseService {

  public OwnerService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
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
      @Nonnull OwnershipType ownershipType,
      @Nullable Urn ownershipTypeUrn) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      addOwnersToResources(opContext, ownerUrns, resources, ownershipType, ownershipTypeUrn);
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
      OwnershipType ownershipType,
      Urn ownershipTypeUrn)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddOwnersProposals(opContext, ownerUrns, resources, ownershipType, ownershipTypeUrn);
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
      OwnershipType ownershipType,
      Urn ownershipTypeUrn) {

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
      ownerUrns.forEach(
          ownerUrn -> {
            OwnerServiceUtils.addOwnerToAspect(owners, ownerUrn, ownershipType, ownershipTypeUrn);
          });
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
