package com.linkedin.metadata.domain;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
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
public class DomainService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public DomainService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  public void batchSetDomain(@Nonnull Urn domainUrn, @Nonnull List<ResourceReference> resources) {
    batchSetDomain(domainUrn, resources, this.systemAuthentication);
  }

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

  public void batchAddDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources) {
    batchAddDomains(domainUrns, resources, this.systemAuthentication);
  }

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

  public void batchUnsetDomain(@Nonnull List<ResourceReference> resources) {
    batchUnsetDomain(resources, this.systemAuthentication);
  }

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

  public void batchRemoveDomains(@Nonnull List<Urn> domainUrns, @Nonnull List<ResourceReference> resources) {
    batchRemoveDomains(domainUrns, resources, this.systemAuthentication);
  }

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

  public void setDomainForResources(
      com.linkedin.common.urn.Urn domainUrn,
      List<ResourceReference> resources,
      @Nullable Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildSetDomainProposal(domainUrn, entity);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  public void addDomainsToResources(
      List<com.linkedin.common.urn.Urn> domainUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddDomainsProposal(domainUrns, entity, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  public void unsetDomainForResources(
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildUnsetDomainProposal(resource);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  public void removeDomainsFromResources(
      List<Urn> domains,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveDomainsProposal(domains, resource, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  @Nullable
  private MetadataChangeProposal buildSetDomainProposal(
      com.linkedin.common.urn.Urn domainUrn,
      ResourceReference resource
  ) throws URISyntaxException {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(ImmutableList.of(domainUrn)));
    return buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains);
  }

  @Nullable
  private MetadataChangeProposal buildAddDomainsProposal(
      List<com.linkedin.common.urn.Urn> domainUrns,
      ResourceReference resource,
      @Nonnull Authentication authentication
  ) throws URISyntaxException {
    Domains domains = getDomainsAspect(
        resource.getUrn(),
        new Domains(),
        authentication);

    if (domains == null) {
      return null;
    }

    if (!domains.hasDomains()) {
      domains.setDomains(new UrnArray());
    }
    addDomainsIfNotExists(domains, domainUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains);
  }

  @Nullable
  private MetadataChangeProposal buildUnsetDomainProposal(
      ResourceReference resource
  ) throws URISyntaxException {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.emptyList()));
    return buildMetadataChangeProposal(resource.getUrn(), Constants.DOMAINS_ASPECT_NAME, domains);
  }

  @Nullable
  private MetadataChangeProposal buildRemoveDomainsProposal(
      List<Urn> domainUrns,
      ResourceReference resource,
      @Nonnull Authentication authentication
  ) throws URISyntaxException {
    Domains domains = getDomainsAspect(
        resource.getUrn(),
        new Domains(),
        authentication);

    if (domains == null) {
      return null;
    }

    if (!domains.hasDomains()) {
      domains.setDomains(new UrnArray());
    }
    removeDomainsIfExists(domains, domainUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.DOMAINS_ASPECT_NAME,
        domains
    );
  }

  private void addDomainsIfNotExists(Domains domains, List<Urn> domainUrns) throws URISyntaxException {
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
  private Domains getDomainsAspect(@Nonnull Urn entityUrn, @Nonnull Domains defaultValue, @Nonnull Authentication authentication) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.DOMAINS_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      if (aspect == null) {
        return defaultValue;
      }
      return new Domains(aspect.data());
    } catch (Exception e) {
      log.error(
          "Error retrieving domains for entity. Entity: {} aspect: {}",
          entityUrn,
          Constants.DOMAIN_ENTITY_NAME,
          e);
      return null;
    }
  }

  @Nonnull
  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes, @Nonnull Authentication authentication) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, authentication);
    }
  }
}
