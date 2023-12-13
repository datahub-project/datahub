package com.linkedin.datahub.graphql.types.notebook.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ChartCell;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.NotebookCell;
import com.linkedin.datahub.graphql.generated.NotebookCellType;
import com.linkedin.datahub.graphql.generated.NotebookContent;
import com.linkedin.datahub.graphql.generated.NotebookEditableProperties;
import com.linkedin.datahub.graphql.generated.NotebookInfo;
import com.linkedin.datahub.graphql.generated.QueryCell;
import com.linkedin.datahub.graphql.generated.TextCell;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.ChangeAuditStampsMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.NotebookKey;
import com.linkedin.notebook.EditableNotebookProperties;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class NotebookMapper implements ModelMapper<EntityResponse, Notebook> {
  public static final NotebookMapper INSTANCE = new NotebookMapper();

  public static Notebook map(EntityResponse response) {
    return INSTANCE.apply(response);
  }

  @Override
  public Notebook apply(EntityResponse response) {
    final Notebook convertedNotebook = new Notebook();
    Urn entityUrn = response.getUrn();

    convertedNotebook.setUrn(response.getUrn().toString());
    convertedNotebook.setType(EntityType.NOTEBOOK);
    EnvelopedAspectMap aspectMap = response.getAspects();
    MappingHelper<Notebook> mappingHelper = new MappingHelper<>(aspectMap, convertedNotebook);
    mappingHelper.mapToResult(NOTEBOOK_KEY_ASPECT_NAME, this::mapNotebookKey);
    mappingHelper.mapToResult(
        NOTEBOOK_INFO_ASPECT_NAME,
        (entity, dataMap) -> this.mapNotebookInfo(entity, dataMap, entityUrn));
    mappingHelper.mapToResult(NOTEBOOK_CONTENT_ASPECT_NAME, this::mapNotebookContent);
    mappingHelper.mapToResult(
        EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME, this::mapEditableNotebookProperties);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (notebook, dataMap) ->
            notebook.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (notebook, dataMap) -> notebook.setStatus(StatusMapper.map(new Status(dataMap))));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (notebook, dataMap) ->
            notebook.setTags(GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (notebook, dataMap) ->
            notebook.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
    mappingHelper.mapToResult(SUB_TYPES_ASPECT_NAME, this::mapSubTypes);
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (notebook, dataMap) ->
            notebook.setGlossaryTerms(
                GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, this::mapDataPlatformInstance);
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (notebook, dataMap) ->
            notebook.setBrowsePathV2(BrowsePathsV2Mapper.map(new BrowsePathsV2(dataMap))));
    return mappingHelper.getResult();
  }

  private void mapDataPlatformInstance(Notebook notebook, DataMap dataMap) {
    DataPlatformInstance dataPlatformInstance = new DataPlatformInstance(dataMap);
    notebook.setPlatform(
        DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(dataPlatformInstance.getPlatform().toString())
            .build());
    notebook.setDataPlatformInstance(
        DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap)));
  }

  private void mapSubTypes(Notebook notebook, DataMap dataMap) {
    SubTypes pegasusSubTypes = new SubTypes(dataMap);
    if (pegasusSubTypes.hasTypeNames()) {
      com.linkedin.datahub.graphql.generated.SubTypes subTypes =
          new com.linkedin.datahub.graphql.generated.SubTypes();
      subTypes.setTypeNames(pegasusSubTypes.getTypeNames().stream().collect(Collectors.toList()));
      notebook.setSubTypes(subTypes);
    }
  }

  private void mapNotebookKey(@Nonnull Notebook notebook, @Nonnull DataMap dataMap) {
    final NotebookKey notebookKey = new NotebookKey(dataMap);
    notebook.setNotebookId(notebookKey.getNotebookId());
    notebook.setTool(notebookKey.getNotebookTool());
  }

  private void mapNotebookInfo(
      @Nonnull Notebook notebook, @Nonnull DataMap dataMap, Urn entityUrn) {
    final com.linkedin.notebook.NotebookInfo gmsNotebookInfo =
        new com.linkedin.notebook.NotebookInfo(dataMap);
    final NotebookInfo notebookInfo = new NotebookInfo();
    notebookInfo.setTitle(gmsNotebookInfo.getTitle());
    notebookInfo.setChangeAuditStamps(
        ChangeAuditStampsMapper.map(gmsNotebookInfo.getChangeAuditStamps()));
    notebookInfo.setDescription(gmsNotebookInfo.getDescription());

    if (gmsNotebookInfo.hasExternalUrl()) {
      notebookInfo.setExternalUrl(gmsNotebookInfo.getExternalUrl().toString());
    }

    if (gmsNotebookInfo.hasCustomProperties()) {
      notebookInfo.setCustomProperties(
          CustomPropertiesMapper.map(gmsNotebookInfo.getCustomProperties(), entityUrn));
    }
    notebook.setInfo(notebookInfo);
  }

  private void mapNotebookContent(@Nonnull Notebook notebook, @Nonnull DataMap dataMap) {
    com.linkedin.notebook.NotebookContent pegasusNotebookContent =
        new com.linkedin.notebook.NotebookContent(dataMap);
    NotebookContent notebookContent = new NotebookContent();
    notebookContent.setCells(mapNotebookCells(pegasusNotebookContent.getCells()));
    notebook.setContent(notebookContent);
  }

  private List<NotebookCell> mapNotebookCells(
      com.linkedin.notebook.NotebookCellArray pegasusCells) {
    return pegasusCells.stream()
        .map(
            pegasusCell -> {
              NotebookCell notebookCell = new NotebookCell();
              NotebookCellType cellType =
                  NotebookCellType.valueOf(pegasusCell.getType().toString());
              notebookCell.setType(cellType);
              switch (cellType) {
                case CHART_CELL:
                  notebookCell.setChartCell(mapChartCell(pegasusCell.getChartCell()));
                  break;
                case TEXT_CELL:
                  notebookCell.setTextCell(mapTextCell(pegasusCell.getTextCell()));
                  break;
                case QUERY_CELL:
                  notebookCell.setQueryChell(mapQueryCell(pegasusCell.getQueryCell()));
                  break;
                default:
                  throw new DataHubGraphQLException(
                      String.format("Un-supported NotebookCellType: %s", cellType),
                      DataHubGraphQLErrorCode.SERVER_ERROR);
              }
              return notebookCell;
            })
        .collect(Collectors.toList());
  }

  private ChartCell mapChartCell(com.linkedin.notebook.ChartCell pegasusChartCell) {
    ChartCell chartCell = new ChartCell();
    chartCell.setCellId(pegasusChartCell.getCellId());
    chartCell.setCellTitle(pegasusChartCell.getCellTitle());
    chartCell.setChangeAuditStamps(
        ChangeAuditStampsMapper.map(pegasusChartCell.getChangeAuditStamps()));
    return chartCell;
  }

  private TextCell mapTextCell(com.linkedin.notebook.TextCell pegasusTextCell) {
    TextCell textCell = new TextCell();
    textCell.setCellId(pegasusTextCell.getCellId());
    textCell.setCellTitle(pegasusTextCell.getCellTitle());
    textCell.setChangeAuditStamps(
        ChangeAuditStampsMapper.map(pegasusTextCell.getChangeAuditStamps()));
    textCell.setText(pegasusTextCell.getText());
    return textCell;
  }

  private QueryCell mapQueryCell(com.linkedin.notebook.QueryCell pegasusQueryCell) {
    QueryCell queryCell = new QueryCell();
    queryCell.setCellId(pegasusQueryCell.getCellId());
    queryCell.setCellTitle(pegasusQueryCell.getCellTitle());
    queryCell.setChangeAuditStamps(
        ChangeAuditStampsMapper.map(pegasusQueryCell.getChangeAuditStamps()));
    queryCell.setRawQuery(pegasusQueryCell.getRawQuery());
    if (pegasusQueryCell.hasLastExecuted()) {
      queryCell.setLastExecuted(AuditStampMapper.map(pegasusQueryCell.getLastExecuted()));
    }
    return queryCell;
  }

  private void mapEditableNotebookProperties(@Nonnull Notebook notebook, @Nonnull DataMap dataMap) {
    final EditableNotebookProperties editableNotebookProperties =
        new EditableNotebookProperties(dataMap);
    final NotebookEditableProperties notebookEditableProperties = new NotebookEditableProperties();
    notebookEditableProperties.setDescription(editableNotebookProperties.getDescription());
    notebook.setEditableProperties(notebookEditableProperties);
  }

  private void mapDomains(@Nonnull Notebook notebook, @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    notebook.setDomain(DomainAssociationMapper.map(domains, notebook.getUrn()));
  }
}
