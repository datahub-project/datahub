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
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
 * <p>Write path — based on which field(s) actually <em>changed</em> relative to the previous value:
 *
 * <ul>
 *   <li>If both fields changed and the URN sets are inconsistent → reject with 400.
 *   <li>If only {@code domainAssociations} changed → derive {@code domains} from associations.
 *   <li>If only {@code domains} changed → derive {@code domainAssociations} from domains.
 *   <li>If {@code domainAssociations} is not present → derive associations from {@code domains}.
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

    @Nullable DomainAssociationArray associations = proposed.getDomainAssociations();

    if (associations == null) {
      // Caller only set domains. Sync domainAssociations.
      deriveAssociationsFromDomains(proposed, previous);
    } else {
      // Both fields present — determine which actually changed to decide source of truth.
      boolean domainsChanged = didDomainsChange(proposed, previous);
      boolean associationsChanged = didAssociationsChange(proposed, previous);

      if (domainsChanged && associationsChanged) {
        // Both changed — only allowed if they are consistent.
        Set<Urn> allAssociationUrns =
            associations.stream()
                .map(DomainAssociation::getDomain)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Urn> domainUrns = new LinkedHashSet<>(proposed.getDomains());
        if (!domainUrns.isEmpty() && !allAssociationUrns.equals(domainUrns)) {
          throw new ValidationException(
              "Cannot update both 'domains' and 'domainAssociations' in the same write. "
                  + "Use 'domainAssociations' for rich metadata or 'domains' for legacy "
                  + "compatibility, but not both. Entity: "
                  + item.getUrn());
        }
      } else if (domainsChanged) {
        // Only domains changed (e.g. read-modify-write that updated only domains).
        deriveAssociationsFromDomains(proposed, previous);
      }
      // else: only associations changed or neither — associations are already correct.
    }

    // Sort associations (manual before propagated), then derive domains to match.
    sortAssociations(proposed);
    deriveDomainsFromAssociations(proposed);
    return true;
  }

  /**
   * Derives the {@code domains} array from {@code domainAssociations}, preserving the association
   * order and deduplicating. Call after {@link #sortAssociations} so manual URNs come first.
   */
  private static void deriveDomainsFromAssociations(Domains proposed) {
    DomainAssociationArray associations = proposed.getDomainAssociations();
    Set<Urn> urns = new LinkedHashSet<>();
    if (associations != null) {
      for (DomainAssociation assoc : associations) {
        urns.add(assoc.getDomain());
      }
    }
    proposed.setDomains(new UrnArray(new ArrayList<>(urns)));
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

  /** Sorts associations so that manual (non-propagated) entries come before propagated ones. */
  private static void sortAssociations(Domains domains) {
    DomainAssociationArray associations = domains.getDomainAssociations();
    if (associations == null || associations.size() <= 1) {
      return;
    }
    List<DomainAssociation> sorted = new ArrayList<>(associations);
    sorted.sort(Comparator.comparing(DomainsSyncMutationHook::isPropagated));
    domains.setDomainAssociations(new DomainAssociationArray(sorted));
  }

  static boolean isPropagated(DomainAssociation assoc) {
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
    UrnArray previousDomains = previous != null ? previous.getDomains() : new UrnArray();
    return !proposedDomains.equals(previousDomains);
  }

  private static boolean didAssociationsChange(Domains proposed, @Nullable Domains previous) {
    DomainAssociationArray proposedAssociations =
        proposed.getDomainAssociations() != null
            ? proposed.getDomainAssociations()
            : new DomainAssociationArray();
    DomainAssociationArray previousAssociations =
        previous != null && previous.getDomainAssociations() != null
            ? previous.getDomainAssociations()
            : new DomainAssociationArray();
    if (proposedAssociations == null && previousAssociations == null) {
      return false;
    }
    if (proposedAssociations == null || previousAssociations == null) {
      return true;
    }
    return !proposedAssociations.equals(previousAssociations);
  }
}
