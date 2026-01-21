package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateDocumentInput;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for creating a new Document on DataHub. Requires the CREATE_ENTITY privilege for
 * Documents or MANAGE_DOCUMENTS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateDocumentResolver implements DataFetcher<CompletableFuture<String>> {

  private final DocumentService _documentService;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateDocumentInput input =
        bindArgument(environment.getArgument("input"), CreateDocumentInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateDocument(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Extract content text
            final String content = input.getContents().getText();

            // Extract related URNs
            final Urn parentDocumentUrn =
                input.getParentDocument() != null
                    ? UrnUtils.getUrn(input.getParentDocument())
                    : null;
            final List<Urn> relatedAssetUrns =
                input.getRelatedAssets() != null
                    ? input.getRelatedAssets().stream()
                        .map(UrnUtils::getUrn)
                        .collect(Collectors.toList())
                    : null;
            final List<Urn> relatedDocumentUrns =
                input.getRelatedDocuments() != null
                    ? input.getRelatedDocuments().stream()
                        .map(UrnUtils::getUrn)
                        .collect(Collectors.toList())
                    : null;

            // Map GraphQL state enum to PDL enum if provided
            com.linkedin.knowledge.DocumentState pdlState =
                input.getState() != null
                    ? com.linkedin.knowledge.DocumentState.valueOf(input.getState().name())
                    : null;

            // Automatically create source with NATIVE type - users cannot set this via API
            // (reserved for ingestion from external systems)
            final com.linkedin.knowledge.DocumentSource source =
                new com.linkedin.knowledge.DocumentSource();
            source.setSourceType(com.linkedin.knowledge.DocumentSourceType.NATIVE);

            // Convert single subType to list for subTypes aspect
            final List<String> subTypes =
                input.getSubType() != null
                    ? java.util.Collections.singletonList(input.getSubType())
                    : null;

            // Extract settings (defaults to true if not provided)
            com.linkedin.knowledge.DocumentSettings settings = null;
            if (input.getSettings() != null) {
              settings = new com.linkedin.knowledge.DocumentSettings();
              if (input.getSettings().getShowInGlobalContext() != null) {
                settings.setShowInGlobalContext(input.getSettings().getShowInGlobalContext());
              } else {
                settings.setShowInGlobalContext(true);
              }
            }

            // Create document using service
            final Urn documentUrn =
                _documentService.createDocument(
                    context.getOperationContext(),
                    input.getId(),
                    subTypes,
                    input.getTitle(),
                    source,
                    pdlState,
                    content,
                    parentDocumentUrn,
                    relatedAssetUrns,
                    relatedDocumentUrns,
                    settings,
                    UrnUtils.getUrn(context.getActorUrn()));

            // Set ownership
            final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
            if (input.getOwners() != null && !input.getOwners().isEmpty()) {
              // Use provided owners
              final List<Owner> owners = mapOwnerInputsToOwners(input.getOwners());
              _documentService.setDocumentOwnership(
                  context.getOperationContext(), documentUrn, owners, actorUrn);
            } else {
              // Default to adding the creator as owner
              final Owner creatorOwner = new Owner();
              creatorOwner.setOwner(actorUrn);
              creatorOwner.setType(OwnershipType.TECHNICAL_OWNER);
              _documentService.setDocumentOwnership(
                  context.getOperationContext(),
                  documentUrn,
                  java.util.Collections.singletonList(creatorOwner),
                  actorUrn);
            }

            return documentUrn.toString();
          } catch (Exception e) {
            log.error(
                "Failed to create Document with id: {}, subType: {}: {}",
                input.getId(),
                input.getSubType(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to create Document: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Maps GraphQL OwnerInputs to PDL Owner objects. */
  private List<Owner> mapOwnerInputsToOwners(List<OwnerInput> ownerInputs) {
    List<Owner> owners = new ArrayList<>();
    for (OwnerInput ownerInput : ownerInputs) {
      Owner owner = new Owner();
      owner.setOwner(UrnUtils.getUrn(ownerInput.getOwnerUrn()));

      // Map ownership type
      if (ownerInput.getOwnershipTypeUrn() != null) {
        // Custom ownership type URN
        owner.setTypeUrn(UrnUtils.getUrn(ownerInput.getOwnershipTypeUrn()));
      } else if (ownerInput.getType() != null) {
        // Standard ownership type enum
        owner.setType(OwnershipType.valueOf(ownerInput.getType().name()));
      } else {
        // Default to NONE
        owner.setType(OwnershipType.NONE);
      }

      owners.add(owner);
    }
    return owners;
  }
}
