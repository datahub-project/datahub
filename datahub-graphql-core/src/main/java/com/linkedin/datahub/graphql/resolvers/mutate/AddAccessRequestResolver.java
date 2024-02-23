package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddAccessRequestInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.AccessRequestUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddAccessRequestResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    log.info("Running mutation addAccessRequest");
    final AddAccessRequestInput input =
        bindArgument(environment.getArgument("input"), AddAccessRequestInput.class);
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());
    java.util.List<StringMapEntryInput> additionalMetadataInput = input.getAdditionalMetadata();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            log.info("Adding AccessRequest. input: {}", input);
            QueryContext context = environment.getContext();
            Urn actor = Urn.createFromString(context.getAuthentication().getActor().toUrnStr());
            AccessRequestUtils.addAccessRequestToEntity(actor, additionalMetadataInput, targetUrn, _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        });
  }

}
