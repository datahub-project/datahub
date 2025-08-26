package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.json.JSONObject;

public class FormUtils {
  private static final String RUN_INGEST_TASK_NAME = "RUN_INGEST";
  private static final String MANUAL_EXECUTION_SOURCE_NAME = "MANUAL_INGESTION_SOURCE";

  public static MetadataChangeProposal createFormNotificationsExecutionRequest(
      @Nonnull String formUrn) {
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    execInput.setTask(RUN_INGEST_TASK_NAME); // Set the RUN_INGEST task
    execInput.setSource(
        new ExecutionRequestSource()
            .setType(MANUAL_EXECUTION_SOURCE_NAME)
            .setIngestionSource(
                UrnUtils.getUrn("urn:li:dataHubIngestionSource:datahub-forms-notifications")));
    execInput.setRequestedAt(System.currentTimeMillis());
    execInput.setActorUrn(UrnUtils.getUrn(SYSTEM_ACTOR));
    execInput.setExecutorId(DEFAULT_EXECUTOR_ID);
    Map<String, String> arguments = new HashMap<>();
    final JSONObject jsonRecipe = new JSONObject();
    final JSONObject config = new JSONObject();
    config.put("form_urns", ImmutableList.of(formUrn));
    final JSONObject source = new JSONObject();
    source.put("type", "datahub-forms-notifications");
    source.put("config", config);
    jsonRecipe.put("source", source);
    arguments.put("recipe", jsonRecipe.toString());
    arguments.put(
        "extra_pip_requirements",
        "[\"acryl-datahub-cloud[datahub-forms-notifications]@/acryl-cloud\"]");
    execInput.setArgs(new StringMap(arguments));

    final ExecutionRequestKey key = new ExecutionRequestKey();
    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    key.setId(uuidStr);

    return MutationUtils.buildMetadataChangeProposalWithKey(
        key, EXECUTION_REQUEST_ENTITY_NAME, EXECUTION_REQUEST_INPUT_ASPECT_NAME, execInput);
  }
}
