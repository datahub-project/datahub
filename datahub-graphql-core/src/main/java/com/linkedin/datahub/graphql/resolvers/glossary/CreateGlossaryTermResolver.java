package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
public class CreateGlossaryTermResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateGlossaryEntityInput input = bindArgument(environment.getArgument("input"), CreateGlossaryEntityInput.class);

    return CompletableFuture.supplyAsync(() -> {
      if (AuthorizationUtils.canManageGlossaries((context))) {
        try {
          final GlossaryTermKey key = new GlossaryTermKey();

          final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
          key.setName(id);

          if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_TERM_ENTITY_NAME), context.getAuthentication())) {
            throw new IllegalArgumentException("This Glossary Term already exists!");
          }

          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
          proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
          proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
          proposal.setAspect(GenericRecordUtils.serializeAspect(mapGlossaryTermInfo(input)));
          proposal.setChangeType(ChangeType.UPSERT);

          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          log.error("Failed to create GlossaryTerm with id: {}, name: {}: {}", input.getId(), input.getName(), e.getMessage());
          throw new RuntimeException(String.format("Failed to create GlossaryTerm with id: %s, name: %s", input.getId(), input.getName()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private GlossaryTermInfo mapGlossaryTermInfo(final CreateGlossaryEntityInput input) {
    final GlossaryTermInfo result = new GlossaryTermInfo();
    result.setName(input.getName());
    result.setTermSource("INTERNAL");
    final String description = input.getDescription() != null ? input.getDescription() : "";
    result.setDefinition(description);
    if (input.getParentNode() != null) {
      try {
        final GlossaryNodeUrn parentNode = GlossaryNodeUrn.createFromString(input.getParentNode());
        result.setParentNode(parentNode, SetMode.IGNORE_NULL);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to create GlossaryNodeUrn from string: %s", input.getParentNode()), e);
      }
    }
    return result;
  }
}
