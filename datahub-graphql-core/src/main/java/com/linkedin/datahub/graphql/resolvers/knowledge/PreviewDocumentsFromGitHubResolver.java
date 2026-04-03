package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GitHubFilePreview;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromGitHubInput;
import com.linkedin.datahub.graphql.generated.PreviewDocumentsFromGitHubResult;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.GitHubFileInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for previewing which files would be imported from a GitHub repository. Requires
 * MANAGE_DOCUMENTS privilege.
 */
@Slf4j
public class PreviewDocumentsFromGitHubResolver
    implements DataFetcher<CompletableFuture<PreviewDocumentsFromGitHubResult>> {

  private final DocumentImportService _importService;

  public PreviewDocumentsFromGitHubResolver(@Nonnull final DocumentImportService importService) {
    this._importService = Objects.requireNonNull(importService, "importService must not be null");
  }

  @Override
  public CompletableFuture<PreviewDocumentsFromGitHubResult> get(
      DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final ImportDocumentsFromGitHubInput input =
        bindArgument(environment.getArgument("input"), ImportDocumentsFromGitHubInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageDocuments(context)) {
            throw new AuthorizationException(
                "Unauthorized to preview document imports. Requires MANAGE_DOCUMENTS privilege.");
          }

          try {
            String branch = input.getBranch() != null ? input.getBranch() : "main";
            String path = input.getPath() != null ? input.getPath() : "";
            List<String> extensions =
                input.getFileExtensions() != null
                    ? input.getFileExtensions()
                    : List.of(".md", ".txt");

            List<GitHubFileInfo> files =
                _importService.previewGitHubImport(
                    input.getRepoUrl(), branch, path, extensions, input.getGithubToken());

            return toGraphQL(files);
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to preview documents from GitHub: {}", e.getMessage());
            throw new RuntimeException(
                String.format("Failed to preview documents from GitHub: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  static PreviewDocumentsFromGitHubResult toGraphQL(List<GitHubFileInfo> files) {
    PreviewDocumentsFromGitHubResult result = new PreviewDocumentsFromGitHubResult();
    result.setTotalCount(files.size());
    result.setFiles(
        files.stream()
            .map(
                f -> {
                  GitHubFilePreview preview = new GitHubFilePreview();
                  preview.setPath(f.getPath());
                  preview.setSize(f.getSize());
                  return preview;
                })
            .collect(Collectors.toList()));
    return result;
  }
}
