package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import org.json.JSONObject;
import org.testng.annotations.Test;

public class FormUtilsTest {

  private static final String TEST_FORM_URN = "urn:li:form:test-form";

  @Test
  public void testCreateFormNotificationsExecutionRequest() {
    // When
    MetadataChangeProposal proposal =
        FormUtils.createFormNotificationsExecutionRequest(TEST_FORM_URN);

    // Then
    assertNotNull(proposal);
    assertEquals(proposal.getEntityType(), EXECUTION_REQUEST_ENTITY_NAME);
    assertEquals(proposal.getAspectName(), EXECUTION_REQUEST_INPUT_ASPECT_NAME);

    // Verify the input
    ExecutionRequestInput input =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            ExecutionRequestInput.class);
    assertNotNull(input);
    assertEquals(input.getTask(), "RUN_INGEST");

    // Verify the source
    ExecutionRequestSource source = input.getSource();
    assertNotNull(source);
    assertEquals(source.getType(), "MANUAL_INGESTION_SOURCE");
    assertEquals(
        source.getIngestionSource(),
        UrnUtils.getUrn("urn:li:dataHubIngestionSource:datahub-forms-notifications"));

    // Verify the actor and executor
    assertEquals(input.getActorUrn(), UrnUtils.getUrn(SYSTEM_ACTOR));
    assertEquals(input.getExecutorId(), DEFAULT_EXECUTOR_ID);

    // Verify the arguments
    StringMap args = input.getArgs();
    assertNotNull(args);
    assertTrue(args.containsKey("recipe"));

    // Parse and verify the recipe JSON
    JSONObject recipe = new JSONObject(args.get("recipe"));
    assertTrue(recipe.has("source"));
    JSONObject sourceConfig = recipe.getJSONObject("source");
    assertEquals(sourceConfig.getString("type"), "datahub-forms-notifications");

    assertTrue(args.containsKey("extra_pip_requirements"));
    assertEquals(
        args.get("extra_pip_requirements"),
        "[\"acryl-datahub-cloud[datahub-forms-notifications]@/acryl-cloud\"]");

    JSONObject config = sourceConfig.getJSONObject("config");
    assertTrue(config.has("form_urns"));
    assertEquals(config.getJSONArray("form_urns").getString(0), TEST_FORM_URN);
  }

  @Test
  public void testCreateFormNotificationsExecutionRequestWithDifferentFormUrn() {
    // Given
    String differentFormUrn = "urn:li:form:different-form";

    // When
    MetadataChangeProposal proposal =
        FormUtils.createFormNotificationsExecutionRequest(differentFormUrn);

    // Then
    ExecutionRequestInput input =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            ExecutionRequestInput.class);
    StringMap args = input.getArgs();
    JSONObject recipe = new JSONObject(args.get("recipe"));
    JSONObject config = recipe.getJSONObject("source").getJSONObject("config");

    assertEquals(config.getJSONArray("form_urns").getString(0), differentFormUrn);
  }
}
