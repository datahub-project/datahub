package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductUpstream;
import com.linkedin.dataproduct.DataProductUpstreams;
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
 * Guards the {@link DataProductUpstreams} aspect against payloads that would corrupt the lineage
 * graph: self-edges (a DataProduct declared as its own upstream) and edges pointing at
 * non-DataProduct urns.
 *
 * <p>This validator is the canonical enforcement point for declared DP -> DP lineage and is wired
 * in SpringStandardPluginConfiguration so it covers GraphQL, OpenAPI and RestLI uniformly. The
 * GraphQL resolver and any future SDK writers therefore do not need to re-implement the same
 * checks.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class DataProductUpstreamsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (BatchItem item : mcpItems) {
      final DataProductUpstreams lineage = item.getAspect(DataProductUpstreams.class);
      if (lineage == null || !lineage.hasUpstreams()) {
        continue;
      }
      final Urn ownerUrn = item.getUrn();
      for (DataProductUpstream upstream : lineage.getUpstreams()) {
        final Urn target = upstream.getDataProduct();
        if (target == null) {
          continue;
        }
        if (target.equals(ownerUrn)) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "DataProduct %s cannot declare itself as an upstream DataProduct",
                      ownerUrn)));
          continue;
        }
        if (!Constants.DATA_PRODUCT_ENTITY_NAME.equals(target.getEntityType())) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "DataProductUpstreams on %s points at non-dataProduct urn %s",
                      ownerUrn, target)));
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
