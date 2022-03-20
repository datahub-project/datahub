package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDomainInput;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
public class CreateTermResolver implements DataFetcher<CompletableFuture<String>> {
    private final EntityClient _entityClient;

    @Override
    public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
        final QueryContext context = environment.getContext();
        final CreateDomainInput input = bindArgument(environment.getArgument("input"), CreateDomainInput.class);

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create the Term Key
                final GlossaryTermKey key = new GlossaryTermKey();
                key.setName(String.format("Classification.%s", input.getName()));

                // Create the MCP
                final MetadataChangeProposal proposal = new MetadataChangeProposal();
                proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));
                proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
                proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
                proposal.setAspect(GenericAspectUtils.serializeAspect(mapTermProperties(input)));
                proposal.setChangeType(ChangeType.UPSERT);
                return _entityClient.ingestProposal(proposal, context.getAuthentication());
            } catch (Exception e) {
                log.error("Failed to create GlossaryTerm with name: {}: {}", input.getName(), e.getMessage());
                throw new RuntimeException(String.format("Failed to create GlossaryTerm with name: %s", input.getName()), e);
            }
        });
    }

    private GlossaryTermInfo mapTermProperties(final CreateDomainInput input) throws URISyntaxException {
        final GlossaryTermInfo result = new GlossaryTermInfo();
        result.setName(input.getName());
        result.setDefinition(input.getDescription(),  SetMode.IGNORE_NULL);
        result.setParentNode(GlossaryNodeUrn.createFromString("urn:li:glossaryNode:Classification"));
        result.setTermSource("INTERNAL");
        return result;
    }
}
