package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainUpstream;
import com.linkedin.domain.DomainUpstreams;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Guards the {@link DomainUpstreams} aspect against payloads that would corrupt the lineage graph:
 * self-edges (a Domain declared as its own upstream) and edges pointing at non-Domain urns.
 *
 * <p>This validator is the canonical enforcement point for declared Domain -> Domain lineage and is
 * wired in SpringStandardPluginConfiguration so it covers GraphQL, OpenAPI and RestLI uniformly.
 * The GraphQL resolver and any future SDK writers therefore do not need to re-implement the same
 * checks.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class DomainUpstreamsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (BatchItem item : mcpItems) {
      final DomainUpstreams lineage = item.getAspect(DomainUpstreams.class);
      if (lineage == null || !lineage.hasUpstreams()) {
        continue;
      }
      final Urn ownerUrn = item.getUrn();
      for (DomainUpstream upstream : lineage.getUpstreams()) {
        final Urn target = upstream.getDomain();
        if (target == null) {
          continue;
        }
        if (target.equals(ownerUrn)) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "Domain %s cannot declare itself as an upstream Domain", ownerUrn)));
          continue;
        }
        if (!Constants.DOMAIN_ENTITY_NAME.equals(target.getEntityType())) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "DomainUpstreams on %s points at non-domain urn %s", ownerUrn, target)));
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
