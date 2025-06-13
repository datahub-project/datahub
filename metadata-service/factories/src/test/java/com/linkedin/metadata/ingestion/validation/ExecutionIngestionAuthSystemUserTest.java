package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.DEFAULT_EXECUTOR_ID;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = SpringStandardPluginConfiguration.class)
@TestPropertySource(properties = {"authentication.enabled=true", "entityClient.impl=java"})
@Import(ExecutionIngestionAuthTestConfiguration.class)
public class ExecutionIngestionAuthSystemUserTest extends AbstractTestNGSpringContextTests {

  @Autowired private ExecuteIngestionAuthValidator validator;

  @Qualifier("systemOperationContext")
  @Autowired
  private OperationContext systemOperationContext;

  @Autowired private AuthorizerChain authorizerChain;

  @Autowired private EntityRegistry entityRegistry;

  @Test
  public void testAuthInit() {
    assertNotNull(validator);
    assertNotNull(systemOperationContext);
    assertNotNull(authorizerChain);

    AuthorizationRequest request =
        new AuthorizationRequest(
            SYSTEM_ACTOR,
            "EXECUTE_ENTITY",
            Optional.of(
                new EntitySpec(
                    INGESTION_SOURCE_ENTITY_NAME,
                    "urn:li:dataHubIngestionSource:datahub-forms-notifications")),
            Collections.emptyList());

    assertEquals(
        authorizerChain.authorize(request).getType(),
        AuthorizationResult.Type.ALLOW,
        "System user is expected to be authorized.");
  }

  @Test
  public void testAuthSystemUserAllowed() {
    final MetadataChangeProposal mcp = createFormNotificationsExecutionRequest("");
    final ChangeMCP item =
        ChangeItemImpl.builder()
            .build(
                mcp,
                AuditStampUtils.createAuditStamp(SYSTEM_ACTOR),
                systemOperationContext.getAspectRetriever());

    assertEquals(
        validator
            .validateProposed(
                Set.of(item),
                systemOperationContext.getRetrieverContext(),
                systemOperationContext.asSession(
                    RequestContext.TEST,
                    authorizerChain,
                    new Authentication(
                        new Actor(ActorType.USER, UrnUtils.getUrn(SYSTEM_ACTOR).getId()), "creds")))
            .count(),
        0,
        "Expected Execution Request to be allowed when the system user has execute permission on the Ingestion source");

    assertEquals(
        validator
            .validateProposed(
                Set.of(item), systemOperationContext.getRetrieverContext(), systemOperationContext)
            .count(),
        0,
        "Expected Execution Request to be allowed when the system user has execute permission on the Ingestion source");
  }

  private static MetadataChangeProposal createFormNotificationsExecutionRequest(
      @Nonnull String formUrn) {
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    execInput.setTask("RUN_INGEST"); // Set the RUN_INGEST task
    execInput.setSource(
        new ExecutionRequestSource()
            .setType("MANUAL_INGESTION_SOURCE")
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
