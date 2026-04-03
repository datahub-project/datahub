package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromGitHubInput;
import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.ImportResult;
import com.linkedin.metadata.service.docimport.ImportUseCase;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for importing documents from a GitHub repository. Requires MANAGE_DOCUMENTS privilege.
 */
@Slf4j
public class ImportDocumentsFromGitHubResolver
    implements DataFetcher<CompletableFuture<ImportDocumentsResult>> {

  private final DocumentImportService _importService;

  public ImportDocumentsFromGitHubResolver(@Nonnull final DocumentImportService importService) {
    this._importService = Objects.requireNonNull(importService, "importService must not be null");
  }

  @Override
  public CompletableFuture<ImportDocumentsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ImportDocumentsFromGitHubInput input =
        bindArgument(environment.getArgument("input"), ImportDocumentsFromGitHubInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageDocuments(context)) {
            throw new AuthorizationException(
                "Unauthorized to import documents. Requires MANAGE_DOCUMENTS privilege.");
          }

          try {
            String branch = input.getBranch() != null ? input.getBranch() : "main";
            String path = input.getPath() != null ? input.getPath() : "";
            List<String> extensions =
                input.getFileExtensions() != null
                    ? input.getFileExtensions()
                    : List.of(".md", ".txt");
            boolean showInGlobal =
                input.getShowInGlobalContext() != null ? input.getShowInGlobalContext() : true;
            ImportUseCase useCase =
                input.getUseCase() != null
                    ? ImportUseCase.fromString(input.getUseCase().toString())
                    : ImportUseCase.CONTEXT_DOCUMENT;

            Urn parentUrn =
                input.getParentDocumentUrn() != null
                    ? Urn.createFromString(input.getParentDocumentUrn())
                    : null;

            ImportResult result =
                _importService.importFromGitHub(
                    context.getOperationContext(),
                    input.getRepoUrl(),
                    branch,
                    path,
                    extensions,
                    input.getGithubToken(),
                    useCase,
                    showInGlobal,
                    parentUrn,
                    Urn.createFromString(context.getActorUrn()));

            return toGraphQL(result);
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to import documents from GitHub: {}", e.getMessage());
            throw new RuntimeException(
                String.format("Failed to import documents from GitHub: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  static ImportDocumentsResult toGraphQL(ImportResult result) {
    ImportDocumentsResult graphqlResult = new ImportDocumentsResult();
    graphqlResult.setCreatedCount(result.getCreatedCount());
    graphqlResult.setUpdatedCount(result.getUpdatedCount());
    graphqlResult.setFailedCount(result.getFailedCount());
    graphqlResult.setErrors(new ArrayList<>(result.getErrors()));
    graphqlResult.setDocumentUrns(new ArrayList<>(result.getDocumentUrns()));
    return graphqlResult;
  }
}
