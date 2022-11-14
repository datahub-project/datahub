package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
public class DomainService extends BaseService {

  public DomainService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Batch sets a single domain for a set of resources.
   *
   * @param domainUrn the urns of the domain to set
   * @param resources references to the resources to change
   */
  public void batchSetDomain(@Nonnull Urn domainUrn, @Nonnull List<ResourceReference> resources) {
    batchSetDomain(domainUrn, resources, this.systemAuthentication);
  }

  /**
   * Batch sets a single domain for a set of resources.
   *
   * @param domainUrn the urns of the domain to set
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   */
  public void batchSetDomain(@Nonnull Urn domainUrn, @Nonnull List<ResourceReference> resources, @Nonnull Authentication authentication) {
    log.debug("Batch setting Domain to entities. domain: {}, resources: {}", resources, domainUrn);
    try {
      setDomainForResources(domainUrn, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch set Domain %s to resources with urns %s!",
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
   */
  public void batchAddDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources) {
    batchAddDomains(domainUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch adds multiple domains for a set of resources. (NOT YET USED)
   *
   * @param domainUrns the urns of the domain to set
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   */
  public void batchAddDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources, @Nonnull Authentication authentication) {
    log.debug("Batch adding Domains to entities. domains: {}, resources: {}", resources, domainUrns);
    try {
      addDomainsToResources(domainUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Domains %s to resources with urns %s!",
          domainUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch unsets a domains for a set of resources. This removes all domains.
   *
   * @param resources references to the resources to change
   */
  public void batchUnsetDomain(@Nonnull List<ResourceReference> resources) {
    batchUnsetDomain(resources, this.systemAuthentication);
  }

  /**
   * Batch unsets a domains for a set of resources. This removes all domains.
   *
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   */
  public void batchUnsetDomain(@Nonnull List<ResourceReference> resources, @Nullable Authentication authentication) {
    log.debug("Batch unsetting Domains to entities. resources: {}", resources);
    try {
      unsetDomainForResources(resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to unset add Domain for resources with urns %s!",
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch removes a specific set of domains for a set of resources. (NOT YET USED)
   *
   * @param domainUrns the urns of domains to remove
   * @param resources references to the resources to change
   */
  public void batchRemoveDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources) {
    batchRemoveDomains(domainUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch removes a specific set of domains for a set of resources. (NOT YET USED)
   *
   * @param domainUrns the urns of domains to remove
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   */
  public void batchRemoveDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources, @Nullable Authentication authentication) {
    log.debug("Batch adding Domains to entities. domains: {}, resources: {}", resources, domainUrns);
    try {
      removeDomainsFromResources(domainUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Domains %s to resources with urns %s!",
        domainUrns,
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
      e);
    }
  }

  private void setDomainForResources(
      com.linkedin.common.urn.Urn domainUrn,
      List<ResourceReference> resources,
      @Nullable Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = buildSetDomainProposals(domainUrn, resources);
    ingestChangeProposals(changes, authentication);
  }

  private void addDomainsToResources(
      List<com.linkedin.common.urn.Urn> domainUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = buildAddDomainsProposals(domainUrns, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  private void unsetDomainForResources(
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = buildUnsetDomainProposals(resources);
    ingestChangeProposals(changes, authentication);
  }

  public void removeDomainsFromResources(
      List<Urn> domains,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = buildRemoveDomainsProposals(domains, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildSetDomainProposals(
      com.linkedin.common.urn.Urn domainUrn,
      List<ResourceReference> resources
  ) {
    List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = new Domains();
      domains.setDomains(new UrnArray(ImmutableList.of(domainUrn)));
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return changes;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildAddDomainsProposals(
      List<com.linkedin.common.urn.Urn> domainUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws URISyntaxException {

    final Map<Urn, Domains> domainAspects = getDomainsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new Domains(),
        authentication
    );

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
     proposals.add(buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
   }
   return proposals;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildUnsetDomainProposals(
      List<ResourceReference> resources
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      Domains domains = new Domains();
      domains.setDomains(new UrnArray(Collections.emptyList()));
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
    }
    return changes;
  }

  @VisibleForTesting
  @Nonnull
  List<MetadataChangeProposal> buildRemoveDomainsProposals(
      List<Urn> domainUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) {
    final Map<Urn, Domains> domainAspects = getDomainsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new Domains(),
        authentication
    );

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
      proposals.add(buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains));
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
}
