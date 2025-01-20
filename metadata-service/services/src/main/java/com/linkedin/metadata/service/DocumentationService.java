package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.applyAppSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.DataProductPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DomainPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableChartPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableContainerPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableDashboardPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableDataFlowPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableDataJobPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableDatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableMLFeaturePropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableMLFeatureTablePropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableMLModelGroupPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableMLPrimaryKeyPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableMlModelPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableSchemaMetadataPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlossaryNodeInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlossaryTermInfoPatchBuilder;
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
        mcps.add(buildPatchDatasetDocumentation(entityUrn, documentation));
        break;
      case Constants.DASHBOARD_ENTITY_NAME:
        mcps.add(buildPatchDashboardDocumentation(opContext, entityUrn, documentation, actorUrn));
        break;
      case Constants.CHART_ENTITY_NAME:
        mcps.add(buildPatchChartDocumentation(opContext, entityUrn, documentation, actorUrn));
        break;
      case Constants.DATA_FLOW_ENTITY_NAME:
        mcps.add(buildPatchDataFlowDocumentation(opContext, entityUrn, documentation, actorUrn));
        break;
      case Constants.DATA_JOB_ENTITY_NAME:
        mcps.add(buildPatchDataJobDocumentation(opContext, entityUrn, documentation, actorUrn));
        break;
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        mcps.add(buildPatchGlossaryTermDocumentation(entityUrn, documentation));
        break;
      case Constants.GLOSSARY_NODE_ENTITY_NAME:
        mcps.add(buildPatchGlossaryNodeDocumentation(entityUrn, documentation));
        break;
      case Constants.CONTAINER_ENTITY_NAME:
        mcps.add(buildPatchContainerDocumentation(entityUrn, documentation));
        break;
      case Constants.DOMAIN_ENTITY_NAME:
        mcps.add(buildPatchDomainDocumentation(entityUrn, documentation));
        break;
      case Constants.ML_MODEL_ENTITY_NAME:
        mcps.add(buildPatchMLModelDocumentation(entityUrn, documentation));
        break;
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        mcps.add(buildPatchMLModelGroupDocumentation(entityUrn, documentation));
        break;
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        mcps.add(buildPatchMLFeatureTableDocumentation(entityUrn, documentation));
        break;
      case Constants.ML_FEATURE_ENTITY_NAME:
        mcps.add(buildPatchMLFeatureDocumentation(entityUrn, documentation));
        break;
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        mcps.add(buildPatchMLPrimaryKeyDocumentation(entityUrn, documentation));
        break;
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        mcps.add(buildPatchDataProductDocumentation(entityUrn, documentation));
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
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableDatasetPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchDashboardDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    AuditStamp timeStamp = new AuditStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());
    return new EditableDashboardPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .setLastModified(timeStamp)
        .build();
  }

  private MetadataChangeProposal buildPatchChartDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    AuditStamp timeStamp = new AuditStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());
    return new EditableChartPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .setLastModified(timeStamp)
        .build();
  }

  private MetadataChangeProposal buildPatchDataFlowDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    AuditStamp timeStamp = new AuditStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());
    return new EditableDataFlowPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .setLastModified(timeStamp)
        .build();
  }

  private MetadataChangeProposal buildPatchDataJobDocumentation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nullable Urn actorUrn) {
    AuditStamp timeStamp = new AuditStamp();
    timeStamp.setTime(System.currentTimeMillis());
    timeStamp.setActor(actorUrn != null ? actorUrn : opContext.getAuditStamp().getActor());
    return new EditableDataJobPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .setLastModified(timeStamp)
        .build();
  }

  private MetadataChangeProposal buildPatchGlossaryTermDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new GlossaryTermInfoPatchBuilder().urn(entityUrn).setDescription(documentation).build();
  }

  private MetadataChangeProposal buildPatchGlossaryNodeDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new GlossaryNodeInfoPatchBuilder().urn(entityUrn).setDescription(documentation).build();
  }

  private MetadataChangeProposal buildPatchContainerDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableContainerPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchDomainDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new DomainPropertiesPatchBuilder().urn(entityUrn).setDescription(documentation).build();
  }

  private MetadataChangeProposal buildPatchMLModelDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableMlModelPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchMLModelGroupDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableMLModelGroupPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchMLFeatureTableDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableMLFeatureTablePropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchMLFeatureDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableMLFeaturePropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchMLPrimaryKeyDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new EditableMLPrimaryKeyPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
        .build();
  }

  private MetadataChangeProposal buildPatchDataProductDocumentation(
      @Nonnull final Urn entityUrn, @Nonnull final String documentation) {
    return new DataProductPropertiesPatchBuilder()
        .urn(entityUrn)
        .setDescription(documentation)
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
