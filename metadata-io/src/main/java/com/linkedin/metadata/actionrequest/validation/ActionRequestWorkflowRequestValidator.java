package com.linkedin.metadata.actionrequest.validation;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.service.ActionWorkflowService;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates ActionWorkflowRequest submissions.
 *
 * <p>This validator ensures that ActionWorkflowRequest objects are properly structured and contain
 * valid field values according to the defined workflow schema.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class ActionRequestWorkflowRequestValidator extends AspectPayloadValidator {
  private AspectPluginConfig config;

  public ActionRequestWorkflowRequestValidator() {}

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return validateWorkflowRequestUpserts(
        changeMCPs.stream()
            .filter(i -> ACTION_REQUEST_INFO_ASPECT_NAME.equals(i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  public Stream<AspectValidationException> validateWorkflowRequestUpserts(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (ChangeMCP item : changeMCPs) {
      final com.linkedin.actionrequest.ActionRequestInfo actionRequestInfo =
          item.getAspect(com.linkedin.actionrequest.ActionRequestInfo.class);

      try {
        // Only validate if this is a workflow request
        if (actionRequestInfo.hasParams()
            && actionRequestInfo.getParams().hasWorkflowFormRequest()) {
          ActionWorkflowFormRequest workflowRequest =
              actionRequestInfo.getParams().getWorkflowFormRequest();

          // Fetch the workflow definition directly from the retriever context
          Urn workflowUrn = workflowRequest.getWorkflow();
          com.linkedin.entity.Aspect aspectObject =
              retrieverContext
                  .getAspectRetriever()
                  .getLatestAspectObject(workflowUrn, ACTION_WORKFLOW_INFO_ASPECT_NAME);

          if (aspectObject == null) {
            exceptions.addException(item, "Workflow definition not found for URN: " + workflowUrn);
            continue;
          }

          // Convert the Aspect to ActionWorkflowInfo
          ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo(aspectObject.data());

          // Validate the request against the workflow definition
          validateWorkflowRequest(workflowRequest, workflowInfo, item, exceptions);
        }
      } catch (Exception e) {
        exceptions.addException(item, "Failed to validate workflow request: " + e.getMessage());
      }
    }

    return exceptions.streamAllExceptions();
  }

  /**
   * Validates a workflow request against its workflow definition.
   *
   * @param workflowRequest the workflow request to validate
   * @param workflowInfo the workflow definition
   * @param item the change MCP item
   * @param exceptions the validation exception collection
   */
  private void validateWorkflowRequest(
      @Nonnull ActionWorkflowFormRequest workflowRequest,
      @Nonnull ActionWorkflowInfo workflowInfo,
      @Nonnull ChangeMCP item,
      @Nonnull ValidationExceptionCollection exceptions) {

    try {
      // Use the static validation method from ActionWorkflowService
      ActionWorkflowService.validateWorkflowFormRequestAgainstDefinition(
          workflowRequest, workflowInfo);
    } catch (IllegalArgumentException e) {
      exceptions.addException(item, e.getMessage());
    }
  }
}
