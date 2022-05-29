package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.entity.DomainChangeEvent;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * This is a simple differ that compares to Domains aspects and assumes that each domain
 * will have a single domain (currently the semantic contract).
 */
public class SingleDomainDiffer implements AspectDiffer<Domains> {
  @Override
  public ChangeTransaction getSemanticDiff(EntityAspect previousValue, EntityAspect currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {

    // TODO: Migrate away from using getSemanticDiff.
    throw new UnsupportedOperationException();
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
      Domains baseDomains,
      Domains targetDomains,
      String entityUrn,
      AuditStamp auditStamp) {

    // Simply fetch the first element from each domains list and compare. If they are different, emit
    // a domain ADD / REMOVE event.
    if (isDomainSet(baseDomains, targetDomains)) {
      return Collections.singletonList(
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .operation(ChangeOperation.ADD)
              .entityUrn(entityUrn)
              .modifier(targetDomains.getDomains().get(0).toString())
              .domainUrn(targetDomains.getDomains().get(0))
              .auditStamp(auditStamp)
              .build());
    }

    if (isDomainUnset(baseDomains, targetDomains)) {
      return Collections.singletonList(
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .operation(ChangeOperation.REMOVE)
              .entityUrn(entityUrn)
              .modifier(baseDomains.getDomains().get(0).toString())
              .domainUrn(baseDomains.getDomains().get(0))
              .auditStamp(auditStamp)
              .build());
    }

    if (isDomainChanged(baseDomains, targetDomains)) {
      return ImmutableList.of(
          // 2 events: Previous domain removed, new one added.
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .operation(ChangeOperation.REMOVE)
              .entityUrn(entityUrn)
              .modifier(baseDomains.getDomains().get(0).toString())
              .domainUrn(baseDomains.getDomains().get(0))
              .auditStamp(auditStamp)
              .build(),
          DomainChangeEvent.entityDomainChangeEventBuilder()
              .category(ChangeCategory.DOMAIN)
              .operation(ChangeOperation.ADD)
              .entityUrn(entityUrn)
              .modifier(targetDomains.getDomains().get(0).toString())
              .domainUrn(targetDomains.getDomains().get(0))
              .auditStamp(auditStamp)
              .build()
          );
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
    return !isDomainEmpty(from) && !isDomainEmpty(to) && !from.getDomains().get(0).equals(to.getDomains().get(0));
  }

  private boolean isDomainEmpty(@Nullable final Domains domains) {
    return domains == null || domains.getDomains().size() == 0;
  }
}
