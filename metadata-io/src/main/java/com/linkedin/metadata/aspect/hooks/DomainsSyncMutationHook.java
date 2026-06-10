package com.linkedin.metadata.aspect.hooks;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps the {@code domains} and {@code domainAssociations} fields of the Domains aspect in sync.
 *
 * <p>Write path:
 *
 * <ul>
 *   <li>If both fields are present and inconsistent (non-propagated URNs from associations ≠
 *       domains) AND domains changed from previous → reject with 400.
 *   <li>If {@code domainAssociations} is provided → derive {@code domains} from non-propagated
 *       entries.
 *   <li>If only {@code domains} is provided (legacy path) → diff against previous and update {@code
 *       domainAssociations}.
 * </ul>
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class DomainsSyncMutationHook extends MutationHook {

  private static final String PROPAGATED_KEY = "propagated";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    List<Pair<ChangeMCP, Boolean>> results = new ArrayList<>();
    for (ChangeMCP item : changeMCPS) {
      if (!Constants.DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
        results.add(Pair.of(item, false));
        continue;
      }
      results.add(Pair.of(item, syncDomains(item)));
    }
    return results.stream();
  }

  private boolean syncDomains(ChangeMCP item) {
    Domains proposed = item.getAspect(Domains.class);
    if (proposed == null) {
      return false;
    }

    Domains previous = item.getPreviousAspect(Domains.class);

    boolean associationsProvided = proposed.hasDomainAssociations();

    if (associationsProvided) {
      // Both fields present — only allowed if they are already in sync.
      // "In sync" means the non-propagated URNs from associations equal the domains set.
      Set<Urn> nonPropagatedUrns = getNonPropagatedUrns(proposed.getDomainAssociations());
      Set<Urn> domainUrns = new LinkedHashSet<>(proposed.getDomains());

      if (!nonPropagatedUrns.equals(domainUrns)) {
        boolean domainsChanged = didDomainsChange(proposed, previous);
        if (domainsChanged) {
          throw new ValidationException(
              "Cannot update both 'domains' and 'domainAssociations' in the same write. "
                  + "Use 'domainAssociations' for rich metadata or 'domains' for legacy "
                  + "compatibility, but not both. Entity: "
                  + item.getUrn());
        }
      }

      // domainAssociations is the source of truth; derive domains from non-propagated entries
      deriveDomainsFromAssociations(proposed);
      return true;
    }

    // Legacy path: caller only set domains. Sync domainAssociations.
    deriveAssociationsFromDomains(proposed, previous);
    return true;
  }

  /**
   * Derives the {@code domains} array from {@code domainAssociations}, excluding propagated
   * entries. An association is considered propagated if {@code
   * attribution.sourceDetail["propagated"]} is {@code "true"} (case-insensitive).
   */
  private void deriveDomainsFromAssociations(Domains proposed) {
    DomainAssociationArray associations = proposed.getDomainAssociations();
    Set<Urn> domainUrns = new LinkedHashSet<>();
    if (associations != null) {
      for (DomainAssociation assoc : associations) {
        if (!isPropagated(assoc)) {
          domainUrns.add(assoc.getDomain());
        }
      }
    }
    proposed.setDomains(new UrnArray(new ArrayList<>(domainUrns)));
  }

  /**
   * Derives {@code domainAssociations} from a diff between previous and proposed {@code domains}.
   * New URNs get empty-attribution entries; removed URNs have all their associations deleted.
   */
  private void deriveAssociationsFromDomains(Domains proposed, @Nullable Domains previous) {
    Set<Urn> proposedUrns = new LinkedHashSet<>(proposed.getDomains());

    // Start from previous associations if available
    DomainAssociationArray newAssociations = new DomainAssociationArray();

    if (previous != null
        && previous.hasDomainAssociations()
        && previous.getDomainAssociations() != null) {
      // Keep associations whose domain is still in the proposed set
      for (DomainAssociation assoc : previous.getDomainAssociations()) {
        if (proposedUrns.contains(assoc.getDomain())) {
          newAssociations.add(assoc);
        }
      }
    }

    // Add new entries for URNs not in previous
    Set<Urn> existingUrns = new LinkedHashSet<>();
    for (DomainAssociation assoc : newAssociations) {
      existingUrns.add(assoc.getDomain());
    }
    for (Urn urn : proposedUrns) {
      if (!existingUrns.contains(urn)) {
        DomainAssociation assoc = new DomainAssociation();
        assoc.setDomain(urn);
        newAssociations.add(assoc);
      }
    }

    proposed.setDomainAssociations(newAssociations);
  }

  private static Set<Urn> getNonPropagatedUrns(DomainAssociationArray associations) {
    Set<Urn> urns = new LinkedHashSet<>();
    if (associations != null) {
      for (DomainAssociation assoc : associations) {
        if (!isPropagated(assoc)) {
          urns.add(assoc.getDomain());
        }
      }
    }
    return urns;
  }

  private static boolean isPropagated(DomainAssociation assoc) {
    if (!assoc.hasAttribution()) {
      return false;
    }
    MetadataAttribution attribution = assoc.getAttribution();
    if (!attribution.hasSourceDetail()) {
      return false;
    }
    String propagated = attribution.getSourceDetail().get(PROPAGATED_KEY);
    return propagated != null && propagated.equalsIgnoreCase("true");
  }

  private static boolean didDomainsChange(Domains proposed, @Nullable Domains previous) {
    UrnArray proposedDomains = proposed.getDomains();
    UrnArray previousDomains = previous != null ? previous.getDomains() : null;
    return !Objects.equals(proposedDomains, previousDomains);
  }
}
