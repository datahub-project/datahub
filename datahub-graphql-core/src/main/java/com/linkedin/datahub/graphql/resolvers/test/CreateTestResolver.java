package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTestInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;


/**
 * Creates or updates a Test. Requires the MANAGE_TESTS privilege.
 */
public class CreateTestResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public CreateTestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateTestInput input = bindArgument(environment.getArgument("input"), CreateTestInput.class);

    return CompletableFuture.supplyAsync(() -> {

      if (canManageTests(context)) {

        try {

          final MetadataChangeProposal proposal = new MetadataChangeProposal();

          // Create new test
          // Since we are creating a new Test, we need to generate a unique UUID.
          final UUID uuid = UUID.randomUUID();
          final String uuidStr = input.getId() == null ? uuid.toString() : input.getId();

          // Create the Ingestion source key
          final TestKey key = new TestKey();
          key.setId(uuidStr);
          proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

          if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.TEST_ENTITY_NAME), context.getAuthentication())) {
            throw new IllegalArgumentException("This Test already exists!");
          }

          // Create the Test info.
          final TestInfo info = mapCreateTestInput(input);
          proposal.setEntityType(Constants.TEST_ENTITY_NAME);
          proposal.setAspectName(Constants.TEST_INFO_ASPECT_NAME);
          proposal.setAspect(GenericRecordUtils.serializeAspect(info));
          proposal.setChangeType(ChangeType.UPSERT);

          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform update against Test with urn %s", input.toString()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private static TestInfo mapCreateTestInput(final CreateTestInput input) {
    final TestInfo result = new TestInfo();
    result.setName(input.getName());
    result.setCategory(input.getCategory());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    result.setDefinition(mapDefinition(input.getDefinition()));
    return result;
  }

}
