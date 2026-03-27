package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.DomainChangeEvent;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This is a simple differ that compares to Domains aspects and assumes that each domain will have a
 * single domain (currently the semantic contract).
 */
public class SingleDomainChangeEventGenerator extends EntityChangeEventGenerator<Domains> {
  private static final String DOMAIN_ADDED_FORMAT = "Domain '%s' added to entity '%s'.";
  private static final String DOMAIN_REMOVED_FORMAT = "Domain '%s' removed from entity '%s'.";

  private static Domains getDomainsFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Domains.class, entityAspect.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(DOMAINS_ASPECT_NAME)
        || !currentValue.getAspect().equals(DOMAINS_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DOMAINS_ASPECT_NAME);
    }

    Domains baseDomains = getDomainsFromAspect(previousValue);
    Domains targetDomains = getDomainsFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOMAIN) {
      changeEvents.addAll(computeDiffs(baseDomains, targetDomains, currentValue.getUrn(), null));
    }

    SemanticChangeType highestSemanticChange = SemanticChangeType.NONE;
    ChangeEvent highestChangeEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestChangeEvent != null) {
      highestSemanticChange = highestChangeEvent.getSemVerChange();
    }

    return ChangeTransaction.builder()
        .semVerChange(highestSemanticChange)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Domains> from,
      @Nonnull Aspect<Domains> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      Domains baseDomains, Domains targetDomains, String entityUrn, AuditStamp auditStamp) {

    // Simply fetch the first element from each domains list and compare. If they are different,
    // emit
    // a domain ADD / REMOVE event.
    if (isDomainSet(baseDomains, targetDomains)) {
      Urn domainUrn = targetDomains.getDomains().get(0);
      return Collections.singletonList(
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .semVerChange(SemanticChangeType.MINOR)
              .operation(ChangeOperation.ADD)
              .entityUrn(entityUrn)
              .modifier(domainUrn.toString())
              .domainUrn(domainUrn)
              .description(String.format(DOMAIN_ADDED_FORMAT, domainUrn.getId(), entityUrn))
              .auditStamp(auditStamp)
              .build());
    }

    if (isDomainUnset(baseDomains, targetDomains)) {
      Urn domainUrn = baseDomains.getDomains().get(0);
      return Collections.singletonList(
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .semVerChange(SemanticChangeType.MINOR)
              .operation(ChangeOperation.REMOVE)
              .entityUrn(entityUrn)
              .modifier(domainUrn.toString())
              .domainUrn(domainUrn)
              .description(String.format(DOMAIN_REMOVED_FORMAT, domainUrn.getId(), entityUrn))
              .auditStamp(auditStamp)
              .build());
    }

    if (isDomainChanged(baseDomains, targetDomains)) {
      Urn removedDomainUrn = baseDomains.getDomains().get(0);
      Urn addedDomainUrn = targetDomains.getDomains().get(0);
      return ImmutableList.of(
          // 2 events: Previous domain removed, new one added.
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .semVerChange(SemanticChangeType.MINOR)
              .operation(ChangeOperation.REMOVE)
              .entityUrn(entityUrn)
              .modifier(removedDomainUrn.toString())
              .domainUrn(removedDomainUrn)
              .description(
                  String.format(DOMAIN_REMOVED_FORMAT, removedDomainUrn.getId(), entityUrn))
              .auditStamp(auditStamp)
              .build(),
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .semVerChange(SemanticChangeType.MINOR)
              .operation(ChangeOperation.ADD)
              .entityUrn(entityUrn)
              .modifier(addedDomainUrn.toString())
              .domainUrn(addedDomainUrn)
              .description(String.format(DOMAIN_ADDED_FORMAT, addedDomainUrn.getId(), entityUrn))
              .auditStamp(auditStamp)
              .build());
    }

    return Collections.emptyList();
  }

  private boolean isDomainSet(@Nullable final Domains from, @Nullable final Domains to) {
    return isDomainEmpty(from) && !isDomainEmpty(to);
  }

  private boolean isDomainUnset(@Nullable final Domains from, @Nullable final Domains to) {
    return !isDomainEmpty(from) && isDomainEmpty(to);
  }

  private boolean isDomainChanged(@Nullable final Domains from, @Nullable final Domains to) {
    return !isDomainEmpty(from)
        && !isDomainEmpty(to)
        && !from.getDomains().get(0).equals(to.getDomains().get(0));
  }

  private boolean isDomainEmpty(@Nullable final Domains domains) {
    return domains == null || domains.getDomains().size() == 0;
  }
}
