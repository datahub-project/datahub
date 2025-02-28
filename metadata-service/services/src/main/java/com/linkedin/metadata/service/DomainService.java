package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.DomainsPatchBuilder;
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
public class DomainService extends BaseService {

  private final boolean _isAsync;

  public DomainService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      final boolean isAsync) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = isAsync;
  }

  public DomainService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = false;
  }

  /**
   * Fetches the domains for an entity.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity
   */
  public List<Urn> getEntityDomains(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final Domains maybeDomains = getDomains(opContext, entityUrn);
    if (maybeDomains != null) {
      return maybeDomains.getDomains();
    }
    return Collections.emptyList();
  }

  /**
   * Sets a single domain for a single entity. Assumes that the operation was already authorized.
   * Assumes that the entity & domain already exist.
   *
   * @param domainUrn the urns of the domain to set
   * @param entityUrn the urn of the entity to change
   */
  public void setDomain(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn)
      throws RemoteInvocationException {
    log.debug("Setting Domain for entity. domain: {}, entity: {}", domainUrn, entityUrn);
    setDomainForResources(
        opContext, domainUrn, ImmutableList.of(new ResourceReference(entityUrn, null, null)), null);
  }

  /**
   * Unset / remove a single domain for a single entity. Assumes that the operation was already
   * authorized. Assumes that the entity already exists.
   *
   * @param entityUrn the urn of the entity to change
   */
  public void unsetDomain(@Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException {
    log.debug("Unsettings Domain for entity. entity: {}", entityUrn);
    unsetDomainForResources(
        opContext, ImmutableList.of(new ResourceReference(entityUrn, null, null)), null);
  }

  /**
   * Batch sets a single domain for a set of resources.
   *
   * @param domainUrn the urns of the domain to set
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchSetDomain(
      @Nonnull OperationContext opContext,
      @Nonnull Urn domainUrn,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug("Batch setting Domain to entities. domain: {}, resources: {}", domainUrn, resources);
    try {
      setDomainForResources(opContext, domainUrn, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set Domain %s to resources with urns %s!",
              domainUrn,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch adds multiple domains for a set of resources. (NOT YET USED)
   *
   * @param domainUrns the urns of the domain to set
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchAddDomains(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> domainUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug(
        "Batch adding Domains to entities. domains: {}, resources: {}", resources, domainUrns);
    try {
      addDomainsToResources(opContext, domainUrns, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Domains %s to resources with urns %s!",
              domainUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch unsets a domains for a set of resources. This removes all domains.
   *
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchUnsetDomain(
      @Nonnull OperationContext opContext,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug("Batch unsetting Domains to entities. resources: {}", resources);
    try {
      unsetDomainForResources(opContext, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to unset add Domain for resources with urns %s!",
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch removes a specific set of domains for a set of resources. (NOT YET USED)
   *
   * @param domainUrns the urns of domains to remove
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchRemoveDomains(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> domainUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug(
        "Batch adding Domains to entities. domains: {}, resources: {}", resources, domainUrns);
    try {
      removeDomainsFromResources(opContext, domainUrns, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Domains %s to resources with urns %s!",
              domainUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void setDomainForResources(
      @Nonnull OperationContext opContext,
      com.linkedin.common.urn.Urn domainUrn,
      List<ResourceReference> resources,
      @Nullable String appSource) {
    final List<MetadataChangeProposal> changes =
        buildSetDomainProposals(domainUrn, resources, appSource);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  private void addDomainsToResources(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> domainUrns,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddDomainsProposals(opContext, domainUrns, resources);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  private void unsetDomainForResources(
      @Nonnull OperationContext opContext,
      List<ResourceReference> resources,
      @Nullable String appSource) {
    final List<MetadataChangeProposal> changes = buildUnsetDomainProposals(resources);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  public void removeDomainsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> domains,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildRemoveDomainsProposals(opContext, domains, resources);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildSetDomainProposals(
      com.linkedin.common.urn.Urn domainUrn,
      List<ResourceReference> resources,
      @Nullable String appSource) {
    if (appSource != null && appSource.equals(METADATA_TESTS_SOURCE)) {
      return patchSetDomain(domainUrn, resources);
    }

    List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = new Domains();
      domains.setDomains(new UrnArray(ImmutableList.of(domainUrn)));
      changes.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return changes;
  }

  List<MetadataChangeProposal> patchSetDomain(
      com.linkedin.common.urn.Urn domainUrn, List<ResourceReference> resources) {
    final List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (ResourceReference resource : resources) {
      DomainsPatchBuilder patchBuilder = new DomainsPatchBuilder().urn(resource.getUrn());
      patchBuilder.setDomain(domainUrn);
      mcps.add(patchBuilder.build());
    }
    return mcps;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildAddDomainsProposals(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> domainUrns,
      List<ResourceReference> resources)
      throws URISyntaxException {

    final Map<Urn, Domains> domainAspects =
        getDomainsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Domains());

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = domainAspects.get(resource.getUrn());
      if (domains == null) {
        continue;
      }
      if (!domains.hasDomains()) {
        domains.setDomains(new UrnArray());
      }
      addDomainsIfNotExists(domains, domainUrns);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return proposals;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildUnsetDomainProposals(List<ResourceReference> resources) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = new Domains();
      domains.setDomains(new UrnArray(Collections.emptyList()));
      changes.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return changes;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildRemoveDomainsProposals(
      @Nonnull OperationContext opContext,
      List<Urn> domainUrns,
      List<ResourceReference> resources) {
    final Map<Urn, Domains> domainAspects =
        getDomainsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new Domains());

    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = domainAspects.get(resource.getUrn());
      if (domains == null) {
        continue;
      }
      if (!domains.hasDomains()) {
        domains.setDomains(new UrnArray());
      }
      removeDomainsIfExists(domains, domainUrns);
      proposals.add(
          buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return proposals;
  }

  private void addDomainsIfNotExists(Domains domains, List<Urn> domainUrns) {
    if (!domains.hasDomains()) {
      domains.setDomains(new UrnArray());
    }

    UrnArray domainsArray = domains.getDomains();

    List<Urn> domainsToAdd = new ArrayList<>();
    for (Urn domainUrn : domainUrns) {
      if (domainsArray.stream().anyMatch(urn -> urn.equals(domainUrn))) {
        continue;
      }
      domainsToAdd.add(domainUrn);
    }

    // Check for no domains to add
    if (domainsToAdd.size() == 0) {
      return;
    }

    domainsArray.addAll(domainsToAdd);
  }

  private static UrnArray removeDomainsIfExists(Domains domains, List<Urn> domainUrns) {
    if (!domains.hasDomains()) {
      domains.setDomains(new UrnArray());
    }
    UrnArray domainAssociationArray = domains.getDomains();
    for (Urn domainUrn : domainUrns) {
      domainAssociationArray.removeIf(urn -> urn.equals(domainUrn));
    }
    return domainAssociationArray;
  }

  @Nullable
  private Domains getDomains(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getDomainsEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(DOMAINS_ASPECT_NAME)) {
      return new Domains(response.getAspects().get(DOMAINS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getDomainsEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext, entityUrn.getEntityType(), entityUrn, ImmutableSet.of(DOMAINS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve domains for entity with urn %s", entityUrn), e);
    }
  }
}
