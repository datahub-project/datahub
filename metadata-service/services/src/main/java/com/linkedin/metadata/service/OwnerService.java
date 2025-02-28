package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linkedin.metadata.aspect.patch.builder.OwnershipPatchBuilder;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
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
  private final boolean _isAsync;

  public OwnerService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      final boolean isAsync) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = isAsync;
  }

  public OwnerService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = false;
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
      addOwnersToResources(
          opContext, ownerUrns, resources, ownershipType, ownershipTypeUrn, null, null);
    }
  }

  /**
   * Batch adds a specific set of owners to a set of resources.
   *
   * @param ownerUrns the urns of the owners to add
   * @param resources references to the resources to change
   * @param ownershipType the ownership type to add
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchAddOwners(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull OwnershipType ownershipType,
      @Nullable Urn ownershipTypeUrn,
      @Nullable String appSource,
      @Nullable Urn actorUrn) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      addOwnersToResources(
          opContext, ownerUrns, resources, ownershipType, ownershipTypeUrn, appSource, actorUrn);
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
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchRemoveOwners(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> ownerUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug("Batch adding Owners to entities. owners: {}, resources: {}", resources, ownerUrns);
    try {
      removeOwnersFromResources(opContext, ownerUrns, resources, appSource);
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

  private void addOwnersToResources(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Urn ownershipTypeUrn,
      @Nullable String appSource,
      @Nullable Urn actorUrn)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddOwnersProposals(
            opContext, ownerUrns, resources, ownershipType, ownershipTypeUrn, appSource, actorUrn);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  private void removeOwnersFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> owners,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildRemoveOwnersProposals(opContext, owners, resources);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddOwnersProposals(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Urn ownershipTypeUrn,
      @Nullable String appSource,
      @Nullable Urn actorUrn) {

    final Urn finalActorUrn =
        actorUrn != null
            ? actorUrn
            : UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr());
    AuditStamp lastModified =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(finalActorUrn);

    if (appSource != null && appSource.equals(METADATA_TESTS_SOURCE)) {
      return patchAddOwners(ownerUrns, resources, ownershipType, ownershipTypeUrn, lastModified);
    }

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
        owners.setLastModified(lastModified);
      }
      addOwnersIfNotExists(owners, ownerUrns, ownershipType, ownershipTypeUrn);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.OWNERSHIP_ASPECT_NAME, owners));
    }
    return proposals;
  }

  List<MetadataChangeProposal> patchAddOwners(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      OwnershipType ownershipType,
      Urn ownershipTypeUrn,
      AuditStamp lastModified) {
    final List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (ResourceReference resource : resources) {
      OwnershipPatchBuilder patchBuilder = new OwnershipPatchBuilder().urn(resource.getUrn());
      for (Urn ownerUrn : ownerUrns) {
        patchBuilder.addOwner(ownerUrn, ownershipType, ownershipTypeUrn);
      }
      patchBuilder.addLastModified(lastModified);
      mcps.add(patchBuilder.build());
    }
    return mcps;
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
      Ownership owners, List<Urn> ownerUrns, OwnershipType ownershipType, Urn ownershipTypeUrn) {
    if (!owners.hasOwners()) {
      owners.setOwners(new OwnerArray());
    }

    OwnerArray ownerAssociationArray = owners.getOwners();

    List<Urn> ownersToAdd = new ArrayList<>();
    for (Urn ownerUrn : ownerUrns) {
      if (ownerAssociationArray.stream()
          .anyMatch(
              association ->
                  isOwnerEqual(association, ownerUrn, ownershipType, ownershipTypeUrn))) {
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
      if (ownershipTypeUrn != null) {
        newOwner.setTypeUrn(ownershipTypeUrn);
      }
      ownerAssociationArray.add(newOwner);
    }
  }

  private boolean isOwnerEqual(
      Owner owner, Urn ownerUrn, OwnershipType ownershipType, Urn ownershipTypeUrn) {
    return owner.getOwner().equals(ownerUrn)
        && owner.getType().equals(ownershipType)
        && (owner.hasTypeUrn()
            ? owner.getTypeUrn().equals(ownershipTypeUrn)
            : ownershipTypeUrn == null);
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
