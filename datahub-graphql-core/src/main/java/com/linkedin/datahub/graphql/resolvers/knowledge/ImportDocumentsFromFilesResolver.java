package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DocumentFileInput;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromFilesInput;
import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.DocumentCandidate;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.ImportResult;
import com.linkedin.metadata.service.docimport.ImportUseCase;
import com.linkedin.metadata.service.docimport.TextExtractors;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for importing documents from pre-parsed file contents. The frontend is responsible for
 * text extraction (e.g. FileReader for text files, mammoth for DOCX). Requires MANAGE_DOCUMENTS
 * privilege.
 */
@Slf4j
public class ImportDocumentsFromFilesResolver
    implements DataFetcher<CompletableFuture<ImportDocumentsResult>> {

  private final DocumentImportService _importService;

  public ImportDocumentsFromFilesResolver(@Nonnull final DocumentImportService importService) {
    this._importService = Objects.requireNonNull(importService, "importService must not be null");
  }

  @Override
  public CompletableFuture<ImportDocumentsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ImportDocumentsFromFilesInput input =
        bindArgument(environment.getArgument("input"), ImportDocumentsFromFilesInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageDocuments(context)) {
            throw new AuthorizationException(
                "Unauthorized to import documents. Requires MANAGE_DOCUMENTS privilege.");
          }

          try {
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

            List<DocumentCandidate> candidates = new ArrayList<>();
            for (DocumentFileInput doc : input.getDocuments()) {
              String filename = doc.getFileName();
              String text = doc.getContent();
              if (text == null || text.isBlank()) {
                log.warn("Skipping empty document: {}", filename);
                continue;
              }

              String ext = TextExtractors.getExtension(filename);
              Map<String, String> props = new HashMap<>();
              props.put("import_source", "file_upload");
              props.put("original_filename", filename);
              props.put("file_extension", ext);

              candidates.add(
                  DocumentCandidate.builder()
                      .title(TextExtractors.titleFromFilename(filename))
                      .text(text)
                      .sourceId(DocumentImportService.makeFileSourceId(filename))
                      .customProperties(props)
                      .build());
            }

            ImportResult result =
                _importService.importDocuments(
                    context.getOperationContext(),
                    candidates,
                    useCase,
                    showInGlobal,
                    parentUrn,
                    Urn.createFromString(context.getActorUrn()));

            return ImportDocumentsFromGitHubResolver.toGraphQL(result);
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to import documents from files: {}", e.getMessage());
            throw new RuntimeException(
                String.format("Failed to import documents from files: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
