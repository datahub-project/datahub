package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
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
import com.linkedin.metadata.utils.LargeStrings;
import com.linkedin.service.ServiceDefinition;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates that a serviceDefinition's {@code rawSpec} LargeString blob actually decodes under its
 * declared {@code compression} — i.e. a GZIP blob is valid base64+gzip. Rejects a
 * corrupt/mislabeled blob at write time so readers (which decode server-side) never fail on a bad
 * aspect.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class ServiceDefinitionLargeStringValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        item -> {
          if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
            validatePatchItem((MCPItem) item, exceptions);
            return;
          }
          validateDefinition(item, item.getAspect(ServiceDefinition.class), exceptions);
        });

    return exceptions.streamAllExceptions();
  }

  /**
   * A patch item carries only its delta; rebuild a partial definition from each add/replace
   * operation (a value at {@code /rawSpec} becomes {@code {"rawSpec":<value>}}) and run the same
   * decode check. Unparseable values are left to schema validation at merge time.
   */
  private void validatePatchItem(MCPItem item, ValidationExceptionCollection exceptions) {
    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op ->
                PatchOperationUtils.nestValueAtObjectPath(op.getFirst(), op.getSecond())
                    .ifPresent(
                        nested -> {
                          try {
                            validateDefinition(
                                item,
                                RecordUtils.toRecordTemplate(
                                    ServiceDefinition.class, nested.toString()),
                                exceptions);
                          } catch (RuntimeException e) {
                            // unparseable delta — schema validation rejects it at merge time
                          }
                        }));
  }

  private void validateDefinition(
      BatchItem item, ServiceDefinition definition, ValidationExceptionCollection exceptions) {
    if (definition != null && definition.hasRawSpec()) {
      try {
        LargeStrings.decode(definition.getRawSpec());
      } catch (IllegalArgumentException e) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "serviceDefinition rawSpec LargeString failed to decode under declared"
                        + " compression %s: %s",
                    definition.getRawSpec().getCompression(), e.getMessage())));
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
