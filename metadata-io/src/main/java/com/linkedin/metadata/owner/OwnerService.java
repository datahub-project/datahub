package com.linkedin.metadata.owner;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
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

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.AspectUtils.*;


@Slf4j
public class OwnerService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public OwnerService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  public void batchAddOwners(@Nonnull List<Urn> ownerUrns, @Nonnull List<ResourceReference> resources, @Nonnull OwnershipType ownershipType) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      addOwnersToResources(ownerUrns, resources, ownershipType);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Owners %s to resources with urns %s!",
          ownerUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  public void batchRemoveOwners(@Nonnull List<Urn> ownerUrns, @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      removeOwnersFromResources(ownerUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Owners %s to resources with urns %s!",
          ownerUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  public void addOwnersToResources(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddOwnersProposal(ownerUrns, entity, ownershipType);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes);
  }

  public void removeOwnersFromResources(
      List<Urn> owners,
      List<ResourceReference> resources
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveOwnersProposal(owners, resource);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes);
  }

  @Nullable
  private MetadataChangeProposal buildAddOwnersProposal(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      ResourceReference resource,
      OwnershipType ownershipType
  ) throws URISyntaxException {
    com.linkedin.common.Ownership owners = getOwnershipAspect(
        resource.getUrn(),
        new Ownership());

    if (owners == null) {
      return null;
    }

    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
      owners.setLastModified(new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
      );
    }
    addOwnersIfNotExists(owners, ownerUrns, ownershipType);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners);
  }

  @Nullable
  private MetadataChangeProposal buildRemoveOwnersProposal(
      List<Urn> ownerUrns,
      ResourceReference resource
  ) throws URISyntaxException {
    com.linkedin.common.Ownership owners = getOwnershipAspect(
        resource.getUrn(),
        new Ownership());

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
  private Ownership getOwnershipAspect(@Nonnull Urn entityUrn, @Nonnull Ownership defaultValue) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.OWNERSHIP_ASPECT_NAME,
          this.entityClient,
          this.systemAuthentication
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

  @Nonnull
  public static MetadataChangeProposal buildMetadataChangeProposal(@Nonnull Urn urn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, this.systemAuthentication);
    }
  }
}
