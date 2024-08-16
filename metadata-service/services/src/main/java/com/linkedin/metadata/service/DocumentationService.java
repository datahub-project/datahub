package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.applyAppSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableSchemaMetadataPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentationService extends BaseService {

  private final String _appSource;
  private final boolean _isAsync;

  public DocumentationService(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      @Nonnull final String appSource,
      final boolean isAsync) {
    super(systemEntityClient, openApiClient, objectMapper);
    _appSource = appSource;
    _isAsync = isAsync;
  }

  public DocumentationService(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(systemEntityClient, openApiClient, objectMapper);
    _appSource = null;
    _isAsync = false;
  }

  public void updateDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn)
      throws Exception {
    List<MetadataChangeProposal> mcps = new ArrayList<>();
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        mcps.add(buildPatchDatasetDocumentation(opContext, entityUrn, documentation, actorUrn));
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Updating documentation for entity type %s not supported",
                entityUrn.getEntityType()));
    }

    if (_appSource != null) {
      applyAppSource(mcps, _appSource);
    }
    ingestChangeProposals(opContext, mcps, _isAsync);
  }

  private MetadataChangeProposal buildPatchDatasetDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    TimeStamp timeStamp = new TimeStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());
    return new DatasetPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .setLastModified(timeStamp)
        .build();
  }

  public void updateSchemaFieldsDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<String> fieldPaths,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn)
      throws Exception {
    List<MetadataChangeProposal> mcps = new ArrayList<>();
    mcps.add(
        buildPatchSchemaFieldDocumentation(
            opContext, entityUrn, fieldPaths, documentation, actorUrn));

    if (_appSource != null) {
      applyAppSource(mcps, _appSource);
    }
    ingestChangeProposals(opContext, mcps, _isAsync);
  }

  public MetadataChangeProposal buildPatchSchemaFieldDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<String> fieldPaths,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    TimeStamp timeStamp = new TimeStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());

    EditableSchemaMetadataPatchBuilder patchBuilder =
        new EditableSchemaMetadataPatchBuilder().urn(entityUrn);
    fieldPaths.forEach(
        fieldPath -> {
          patchBuilder.setDescription(documentation, fieldPath);
        });
    return patchBuilder.build();
  }
}
