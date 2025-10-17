package com.linkedin.datahub.graphql.resolvers.file;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CreateDataHubFileInput;
import com.linkedin.datahub.graphql.generated.CreateDataHubFileResponse;
import com.linkedin.datahub.graphql.generated.DataHubFile;
import com.linkedin.datahub.graphql.types.file.DataHubFileMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.DataHubFileService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateDataHubFileResolver
    implements DataFetcher<CompletableFuture<CreateDataHubFileResponse>> {

  private final DataHubFileService _dataHubFileService;

  @Override
  public CompletableFuture<CreateDataHubFileResponse> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateDataHubFileInput input =
        bindArgument(environment.getArgument("input"), CreateDataHubFileInput.class);

    String id = input.getId();
    String storageBucket = input.getStorageBucket();
    String storageKey = input.getStorageKey();
    String originalFileName = input.getOriginalFileName();
    String mimeType = input.getMimeType();
    Long sizeInBytes = input.getSizeInBytes();
    com.linkedin.file.FileUploadScenario scenario =
        com.linkedin.file.FileUploadScenario.valueOf(input.getScenario().toString());
    String contentHash = input.getContentHash();

    Urn referencedByAsset = null;
    if (input.getReferencedByAsset() != null) {
      referencedByAsset = UrnUtils.getUrn(input.getReferencedByAsset());
    }

    Urn schemaField = null;
    if (input.getSchemaField() != null) {
      schemaField = UrnUtils.getUrn(input.getSchemaField());
    }

    final Urn finalReferencedByAsset = referencedByAsset;
    final Urn finalSchemaField = schemaField;
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn fileUrn =
                _dataHubFileService.createDataHubFile(
                    context.getOperationContext(),
                    id,
                    storageBucket,
                    storageKey,
                    originalFileName,
                    mimeType,
                    sizeInBytes,
                    scenario,
                    finalReferencedByAsset,
                    finalSchemaField,
                    contentHash);

            EntityResponse response =
                _dataHubFileService.getDataHubFileEntityResponse(
                    context.getOperationContext(), fileUrn);

            DataHubFile file = DataHubFileMapper.map(context, response);

            CreateDataHubFileResponse result = new CreateDataHubFileResponse();
            result.setFile(file);
            return result;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create DataHub file with id %s", id), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
