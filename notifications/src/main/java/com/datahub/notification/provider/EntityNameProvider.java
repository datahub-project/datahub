package com.datahub.notification.provider;

import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.DataMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MLFeatureKey;
import com.linkedin.metadata.key.MLFeatureTableKey;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.metadata.key.MLModelKey;
import com.linkedin.metadata.key.MLPrimaryKeyKey;
import com.linkedin.notebook.NotebookInfo;
import com.linkedin.tag.TagProperties;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Provider of basic information about entity names. */
@Slf4j
public class EntityNameProvider {

  protected final SystemEntityClient _entityClient;
  protected final IdentityProvider _identityProvider;

  public EntityNameProvider(@Nonnull final SystemEntityClient entityClient) {
    _entityClient = entityClient;
    _identityProvider = new IdentityProvider(entityClient);
  }

  /**
   * Returns the name for use when displaying any entity.
   *
   * <p>Returns the urn of the term if one cannot be resolved.
   */
  public String getName(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return getDatasetName(opContext, entityUrn);
      case Constants.DASHBOARD_ENTITY_NAME:
        return getDashboardName(opContext, entityUrn);
      case Constants.CHART_ENTITY_NAME:
        return getChartName(opContext, entityUrn);
      case Constants.DATA_JOB_ENTITY_NAME:
        return getDataJobName(opContext, entityUrn);
      case Constants.DATA_FLOW_ENTITY_NAME:
        return getDataFlowName(opContext, entityUrn);
      case Constants.CONTAINER_ENTITY_NAME:
        return getContainerName(opContext, entityUrn);
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return getGlossaryTermName(opContext, entityUrn);
      case Constants.TAG_ENTITY_NAME:
        return getTagName(opContext, entityUrn);
      case Constants.DOMAIN_ENTITY_NAME:
        return getDomainName(opContext, entityUrn);
      case Constants.CORP_USER_ENTITY_NAME:
        return getUserName(opContext, entityUrn);
      case Constants.CORP_GROUP_ENTITY_NAME:
        return getGroupName(opContext, entityUrn);
      case Constants.INGESTION_SOURCE_ENTITY_NAME:
        return getIngestionSourceName(opContext, entityUrn);
      case Constants.DATA_PLATFORM_ENTITY_NAME:
        return getDataPlatformName(opContext, entityUrn);
      case Constants.SCHEMA_FIELD_ENTITY_NAME:
        return getSchemaFieldName(entityUrn);
      case Constants.ML_FEATURE_ENTITY_NAME:
        return getMLFeatureName(opContext, entityUrn);
      case Constants.ML_MODEL_ENTITY_NAME:
        return getMLModelName(opContext, entityUrn);
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        return getMLModelGroupName(opContext, entityUrn);
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        return getMLFeatureTableName(opContext, entityUrn);
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return getMLPrimaryKeyName(opContext, entityUrn);
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        return getDataProductName(opContext, entityUrn);
      case Constants.NOTEBOOK_ENTITY_NAME:
        return getNotebookName(opContext, entityUrn);
      default:
        return entityUrn.toString();
    }
  }

  /**
   * Returns the platform name of any entity.
   *
   * <p>Returns null if not found.
   */
  @Nullable
  public String getPlatformName(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
      case Constants.NOTEBOOK_ENTITY_NAME:
      case Constants.DASHBOARD_ENTITY_NAME:
      case Constants.CHART_ENTITY_NAME:
      case Constants.DATA_JOB_ENTITY_NAME:
      case Constants.DATA_FLOW_ENTITY_NAME:
      case Constants.CONTAINER_ENTITY_NAME:
      case Constants.ML_MODEL_ENTITY_NAME:
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return getAssetPlatform(opContext, entityUrn);
      default:
        return null;
    }
  }

  /**
   * Returns the type name of an entity.
   *
   * <p>Returns null if not found.
   */
  @Nonnull
  public String getTypeName(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    String maybeSubType = getEntitySubType(opContext, entityUrn);
    if (maybeSubType != null) {
      return capitalizeFirstLetter(maybeSubType);
    }
    return getEntityTypeNameFromEntity(entityUrn.getEntityType());
  }

  private String getDatasetName(@Nonnull OperationContext opContext, Urn datasetUrn) {
    DataMap data = getAspectData(opContext, datasetUrn, Constants.DATASET_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      DatasetProperties datasetProperties = new DatasetProperties(data);
      return datasetProperties.hasName()
          ? datasetProperties.getName()
          : datasetUrn.getEntityKey().get(1);
    }
    return datasetUrn.getEntityKey().get(1);
  }

  private String getDashboardName(@Nonnull OperationContext opContext, Urn dashboardUrn) {
    DataMap data = getAspectData(opContext, dashboardUrn, Constants.DASHBOARD_INFO_ASPECT_NAME);
    if (data != null) {
      return new DashboardInfo(data).getTitle();
    }
    return dashboardUrn.toString();
  }

  private String getDataJobName(@Nonnull OperationContext opContext, Urn dataJobUrn) {
    DataMap data = getAspectData(opContext, dataJobUrn, Constants.DATA_JOB_INFO_ASPECT_NAME);
    if (data != null) {
      return new DataJobInfo(data).getName();
    }
    return dataJobUrn.toString();
  }

  private String getDataFlowName(@Nonnull OperationContext opContext, Urn dataFlowUrn) {
    DataMap data = getAspectData(opContext, dataFlowUrn, Constants.DATA_FLOW_INFO_ASPECT_NAME);
    if (data != null) {
      return new DataFlowInfo(data).getName();
    }
    return dataFlowUrn.toString();
  }

  private String getChartName(@Nonnull OperationContext opContext, Urn chartUrn) {
    DataMap data = getAspectData(opContext, chartUrn, Constants.CHART_INFO_ASPECT_NAME);
    if (data != null) {
      return new ChartInfo(data).getTitle();
    }
    return chartUrn.toString();
  }

  private String getContainerName(@Nonnull OperationContext opContext, Urn containerUrn) {
    DataMap data =
        getAspectData(opContext, containerUrn, Constants.CONTAINER_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new ContainerProperties(data).getName();
    }
    return containerUrn.toString();
  }

  private String getGlossaryTermName(@Nonnull OperationContext opContext, Urn glossaryTermUrn) {
    DataMap data =
        getAspectData(opContext, glossaryTermUrn, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    if (data != null) {
      GlossaryTermInfo info = new GlossaryTermInfo(data);
      return info.hasName() ? info.getName() : glossaryTermUrn.getId();
    }
    return glossaryTermUrn.toString();
  }

  private String getTagName(@Nonnull OperationContext opContext, Urn tagUrn) {
    DataMap data = getAspectData(opContext, tagUrn, Constants.TAG_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new TagProperties(data).getName();
    }
    return tagUrn.toString();
  }

  private String getDomainName(@Nonnull OperationContext opContext, Urn domainUrn) {
    DataMap data = getAspectData(opContext, domainUrn, Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new DomainProperties(data).getName();
    }
    return domainUrn.toString();
  }

  private String getUserName(@Nonnull OperationContext opContext, Urn userUrn) {
    IdentityProvider.User maybeUser = _identityProvider.getUser(opContext, userUrn);
    return maybeUser != null ? maybeUser.getResolvedDisplayName() : userUrn.getId();
  }

  private String getGroupName(@Nonnull OperationContext opContext, Urn groupUrn) {
    DataMap data = getAspectData(opContext, groupUrn, Constants.CORP_GROUP_INFO_ASPECT_NAME);
    if (data != null) {
      CorpGroupInfo info = new CorpGroupInfo(data);
      return info.hasDisplayName() ? info.getDisplayName() : groupUrn.getId();
    }
    return groupUrn.toString();
  }

  private String getIngestionSourceName(
      @Nonnull OperationContext opContext, Urn ingestionSourceUrn) {
    DataMap data =
        getAspectData(opContext, ingestionSourceUrn, Constants.INGESTION_INFO_ASPECT_NAME);
    if (data != null) {
      DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo(data);
      return info.hasName() ? info.getName() : ingestionSourceUrn.toString();
    }
    return ingestionSourceUrn.toString();
  }

  private String getDataPlatformName(@Nonnull OperationContext opContext, Urn dataPlatformUrn) {
    DataMap data =
        getAspectData(opContext, dataPlatformUrn, Constants.DATA_PLATFORM_INFO_ASPECT_NAME);
    if (data != null) {
      DataPlatformInfo info = new DataPlatformInfo(data);
      return info.hasDisplayName() ? info.getDisplayName() : info.getName();
    }
    return dataPlatformUrn.toString();
  }

  private String getSchemaFieldName(Urn schemaFieldUrn) {
    return schemaFieldUrn.getEntityKey().get(1);
  }

  private String getMLFeatureName(@Nonnull OperationContext opContext, Urn mlFeatureUrn) {
    DataMap data = getAspectData(opContext, mlFeatureUrn, Constants.ML_FEATURE_KEY_ASPECT_NAME);
    if (data != null) {
      MLFeatureKey mlFeatureKey = new MLFeatureKey(data);
      return mlFeatureKey.getName();
    }
    return mlFeatureUrn.getEntityKey().get(1);
  }

  private String getMLModelName(@Nonnull OperationContext opContext, Urn mlModelUrn) {
    DataMap data = getAspectData(opContext, mlModelUrn, Constants.ML_MODEL_KEY_ASPECT_NAME);
    if (data != null) {
      MLModelKey mlModelKey = new MLModelKey(data);
      return mlModelKey.getName();
    }
    return mlModelUrn.getEntityKey().get(1);
  }

  private String getMLModelGroupName(@Nonnull OperationContext opContext, Urn mlModelGroupUrn) {
    DataMap data =
        getAspectData(opContext, mlModelGroupUrn, Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME);
    if (data != null) {
      MLModelGroupKey mlModelGroupKey = new MLModelGroupKey(data);
      return mlModelGroupKey.getName();
    }
    return mlModelGroupUrn.getEntityKey().get(1);
  }

  private String getMLFeatureTableName(@Nonnull OperationContext opContext, Urn mlFeatureTableUrn) {
    DataMap data =
        getAspectData(opContext, mlFeatureTableUrn, Constants.ML_FEATURE_TABLE_KEY_ASPECT_NAME);
    if (data != null) {
      MLFeatureTableKey mlFeatureTableKey = new MLFeatureTableKey(data);
      return mlFeatureTableKey.getName();
    }
    return mlFeatureTableUrn.getEntityKey().get(1);
  }

  private String getMLPrimaryKeyName(@Nonnull OperationContext opContext, Urn mlPrimaryKeyUrn) {
    DataMap data =
        getAspectData(opContext, mlPrimaryKeyUrn, Constants.ML_PRIMARY_KEY_KEY_ASPECT_NAME);
    if (data != null) {
      MLPrimaryKeyKey mlPrimaryKeyKey = new MLPrimaryKeyKey(data);
      return mlPrimaryKeyKey.getName();
    }
    return mlPrimaryKeyUrn.getEntityKey().get(1);
  }

  private String getDataProductName(@Nonnull OperationContext opContext, Urn dataProductUrn) {
    DataMap data =
        getAspectData(opContext, dataProductUrn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      DataProductProperties dataProductProperties = new DataProductProperties(data);
      return dataProductProperties.getName();
    }
    return dataProductUrn.toString();
  }

  private String getNotebookName(@Nonnull OperationContext opContext, Urn notebookUrn) {
    DataMap data = getAspectData(opContext, notebookUrn, Constants.NOTEBOOK_INFO_ASPECT_NAME);
    if (data != null) {
      NotebookInfo notebookInfo = new NotebookInfo(data);
      return notebookInfo.getTitle();
    }
    return notebookUrn.toString();
  }

  @Nullable
  private String getAssetPlatform(@Nonnull OperationContext opContext, Urn assetUrn) {
    DataMap data = getAspectData(opContext, assetUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (data != null) {
      DataPlatformInstance dataPlatformInstance = new DataPlatformInstance(data);
      if (dataPlatformInstance.hasPlatform()) {
        DataMap platformData =
            getAspectData(
                opContext,
                dataPlatformInstance.getPlatform(),
                Constants.DATA_PLATFORM_INFO_ASPECT_NAME);
        if (platformData != null) {
          DataPlatformInfo info = new DataPlatformInfo(platformData);
          return info.hasDisplayName() ? info.getDisplayName() : info.getName();
        }
        return dataPlatformInstance.getPlatform().getId();
      }
    }
    return null;
  }

  @Nullable
  private String getEntitySubType(@Nonnull OperationContext opContext, Urn assetUrn) {
    DataMap data = getAspectData(opContext, assetUrn, Constants.SUB_TYPES_ASPECT_NAME);
    if (data != null) {
      SubTypes subTypes = new SubTypes(data);
      if (subTypes.hasTypeNames() && subTypes.getTypeNames().size() > 0) {
        return subTypes.getTypeNames().get(0);
      }
    }
    return null;
  }

  @Nullable
  private DataMap getAspectData(@Nonnull OperationContext opContext, Urn urn, String aspectName) {
    try {
      EntityResponse response =
          _entityClient.getV2(opContext, urn.getEntityType(), urn, ImmutableSet.of(aspectName));
      if (response != null && response.getAspects().containsKey(aspectName)) {
        return response.getAspects().get(aspectName).getValue().data();
      } else {
        log.warn(
            String.format(
                "Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
        return null;
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
      return null;
    }
  }

  private String getEntityTypeNameFromEntity(@Nonnull final String entityType) {
    switch (entityType) {
      case Constants.DATASET_ENTITY_NAME:
        return "Dataset";
      case Constants.DASHBOARD_ENTITY_NAME:
        return "Dashboard";
      case Constants.CHART_ENTITY_NAME:
        return "Chart";
      case Constants.DATA_JOB_ENTITY_NAME:
        return "Data Job";
      case Constants.DATA_FLOW_ENTITY_NAME:
        return "Data Flow";
      case Constants.CONTAINER_ENTITY_NAME:
        return "Container";
      case Constants.ML_MODEL_ENTITY_NAME:
        return "ML Model";
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        return "ML Model Group";
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        return "ML Feature Table";
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return "ML Primary Key";
      case Constants.NOTEBOOK_ENTITY_NAME:
        return "Notebook";
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return "Glossary Term";
      case Constants.TAG_ENTITY_NAME:
        return "Tag";
      case Constants.DOMAIN_ENTITY_NAME:
        return "Domain";
      case Constants.CORP_USER_ENTITY_NAME:
        return "User";
      case Constants.CORP_GROUP_ENTITY_NAME:
        return "Group";
      case Constants.INGESTION_SOURCE_ENTITY_NAME:
        return "Ingestion Source";
      case Constants.DATA_PLATFORM_ENTITY_NAME:
        return "Data Platform";
      case Constants.SCHEMA_FIELD_ENTITY_NAME:
        return "Column";
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        return "Data Product";
      default:
        // JUST RETURN THE ENTITY TYPE ITSELF OTHERWISE.
        return entityType;
    }
  }

  public static String capitalizeFirstLetter(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }
}
