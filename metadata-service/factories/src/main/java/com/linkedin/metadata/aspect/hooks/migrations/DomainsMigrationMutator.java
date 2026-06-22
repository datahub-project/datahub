package com.linkedin.metadata.aspect.hooks.migrations;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * v1 → v2 migration: backfills {@code domainAssociations} from the {@code domains} array when the
 * field is absent. Always returns a copy for v1 aspects so the schema version gets bumped to v2,
 * even if associations are already present (e.g. set by a new-path client writing to a v1 entity).
 */
@Slf4j
@Component
public class DomainsMigrationMutator extends AspectMigrationMutator {

  @Nonnull
  @Override
  public String getAspectName() {
    return DOMAINS_ASPECT_NAME;
  }

  @Override
  public long getSourceVersion() {
    return 1L;
  }

  @Override
  public long getTargetVersion() {
    return 2L;
  }

  @Nullable
  @Override
  protected RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    Domains domains = new Domains(sourceAspect.data());

    try {
      Domains copy = domains.copy();

      // Only backfill when associations are missing; if a new-path client already set them, keep
      // as-is
      if (!copy.hasDomainAssociations() || copy.getDomainAssociations().isEmpty()) {
        UrnArray domainUrns = copy.getDomains();
        if (domainUrns != null && !domainUrns.isEmpty()) {
          DomainAssociationArray associations = new DomainAssociationArray();
          for (var urn : domainUrns) {
            DomainAssociation assoc = new DomainAssociation();
            assoc.setDomain(urn);
            associations.add(assoc);
          }
          copy.setDomainAssociations(associations);
        }
      }

      // Always return copy so schema version gets bumped from v1 → v2
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy Domains aspect for migration", e);
    }
  }
}
