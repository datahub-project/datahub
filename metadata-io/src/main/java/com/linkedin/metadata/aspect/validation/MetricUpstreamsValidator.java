package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metric.MetricUpstreams;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Rejects {@code metricUpstreams} writes when a column's parent dataset is missing from {@code
 * datasetUpstreams}. Absent or empty {@code fieldUpstreams} is valid. An empty array clears
 * previous column edges.
 */
@Setter
@Getter
@Accessors(chain = true)
public class MetricUpstreamsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    mcpItems.forEach(
        item -> {
          if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
            validatePatchItem((MCPItem) item, exceptions);
            return;
          }
          validateMetricUpstreams(item, item.getAspect(MetricUpstreams.class), exceptions);
        });
    return exceptions.streamAllExceptions();
  }

  private void validatePatchItem(MCPItem item, ValidationExceptionCollection exceptions) {
    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op ->
                PatchOperationUtils.nestValueAtObjectPath(op.getFirst(), op.getSecond())
                    .ifPresent(
                        nested -> {
                          try {
                            validateMetricUpstreams(
                                item,
                                RecordUtils.toRecordTemplate(
                                    MetricUpstreams.class, nested.toString()),
                                exceptions);
                          } catch (RuntimeException e) {
                            // unparseable delta. Schema validation rejects it at merge time.
                          }
                        }));
  }

  private void validateMetricUpstreams(
      BatchItem item, MetricUpstreams upstreams, ValidationExceptionCollection exceptions) {
    if (upstreams == null
        || !upstreams.hasFieldUpstreams()
        || upstreams.getFieldUpstreams() == null) {
      return;
    }
    if (upstreams.getFieldUpstreams().isEmpty()) {
      return;
    }

    Set<Urn> datasetUrns = new HashSet<>();
    if (upstreams.hasDatasetUpstreams() && upstreams.getDatasetUpstreams() != null) {
      for (Edge datasetEdge : upstreams.getDatasetUpstreams()) {
        if (datasetEdge.getDestinationUrn() != null) {
          datasetUrns.add(datasetEdge.getDestinationUrn());
        }
      }
    }

    for (Edge fieldEdge : upstreams.getFieldUpstreams()) {
      Urn fieldUrn = fieldEdge.getDestinationUrn();
      if (fieldUrn == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                item, "metricUpstreams.fieldUpstreams edge is missing destinationUrn"));
        continue;
      }
      Optional<Pair<Urn, String>> parsed = SchemaFieldUtils.parseSchemaFieldUrn(fieldUrn);
      if (parsed.isEmpty()) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "metricUpstreams.fieldUpstreams destination must be a schemaField URN: %s",
                    fieldUrn)));
        continue;
      }
      Urn parentDataset = parsed.get().getFirst();
      if (!datasetUrns.contains(parentDataset)) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "schemaField %s parent dataset %s is not in metricUpstreams.datasetUpstreams",
                    fieldUrn, parentDataset)));
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
