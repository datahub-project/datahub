package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ABORTED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_CANCELLED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_DUPLICATE;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_SUCCESS;

import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/** A Validator for StructuredProperties Aspect that is attached to entities like Datasets, etc. */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class ExecutionRequestResultValidator extends AspectPayloadValidator {
  private static final Set<String> IMMUTABLE_STATUS =
      Set.of(
          EXECUTION_REQUEST_STATUS_ABORTED,
          EXECUTION_REQUEST_STATUS_CANCELLED,
          EXECUTION_REQUEST_STATUS_SUCCESS,
          EXECUTION_REQUEST_STATUS_DUPLICATE);

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPs.stream()
        .filter(item -> item.getPreviousRecordTemplate() != null)
        .map(
            item -> {
              ExecutionRequestResult existingResult =
                  item.getPreviousAspect(ExecutionRequestResult.class);

              if (IMMUTABLE_STATUS.contains(existingResult.getStatus())) {
                ExecutionRequestResult currentResult = item.getAspect(ExecutionRequestResult.class);
                return AspectValidationException.forItem(
                    item,
                    String.format(
                        "Invalid update to immutable state for aspect dataHubExecutionRequestResult. Execution urn: %s previous status: %s. Denied status update: %s",
                        item.getUrn(), existingResult.getStatus(), currentResult.getStatus()));
              }

              return null;
            })
        .filter(Objects::nonNull);
  }
}
