package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.DataMap;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Provider of basic information about a Tag entity.
 */
@Slf4j
public class EntityNameProvider {

  protected final EntityClient _entityClient;
  protected final Authentication _systemAuthentication;
  protected final IdentityProvider _identityProvider;

  public EntityNameProvider(
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication systemAuthentication) {
    _entityClient = entityClient;
    _systemAuthentication = systemAuthentication;
    _identityProvider = new IdentityProvider(entityClient, systemAuthentication);
  }

  /**
   * Returns the name for use when displaying a glossary term.
   *
   * Returns the urn of the term if one cannot be resolved.
   */
  public String getName(@Nonnull final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return getDatasetName(entityUrn);
      case Constants.DASHBOARD_ENTITY_NAME:
        return getDashboardName(entityUrn);
      case Constants.CHART_ENTITY_NAME:
        return getChartName(entityUrn);
      case Constants.CONTAINER_ENTITY_NAME:
        return getContainerName(entityUrn);
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return getGlossaryTermName(entityUrn);
      case Constants.TAG_ENTITY_NAME:
        return getTagName(entityUrn);
      case Constants.DOMAIN_ENTITY_NAME:
        return getDomainName(entityUrn);
      case Constants.CORP_USER_ENTITY_NAME:
        return getUserName(entityUrn);
      case Constants.CORP_GROUP_ENTITY_NAME:
        return getGroupName(entityUrn);
      case Constants.INGESTION_SOURCE_ENTITY_NAME:
        return getIngestionSourceName(entityUrn);
      case Constants.DATA_PLATFORM_ENTITY_NAME:
        return getDataPlatformName(entityUrn);
      case Constants.SCHEMA_FIELD_ENTITY_NAME:
        return getSchemaFieldName(entityUrn);
      case Constants.ML_FEATURE_ENTITY_NAME:
        return getMLFeatureName(entityUrn);
      case Constants.ML_MODEL_ENTITY_NAME:
        return getMLModelName(entityUrn);
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        return getMLModelGroupName(entityUrn);
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        return getMLFeatureTableName(entityUrn);
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return getMLPrimaryKeyName(entityUrn);
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        return getDataProductName(entityUrn);
      case Constants.NOTEBOOK_ENTITY_NAME:
        return getNotebookName(entityUrn);
      default:
        return entityUrn.toString();
    }
  }

  private String getDatasetName(Urn datasetUrn) {
    DataMap data = getAspectData(datasetUrn, Constants.DATASET_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      DatasetProperties datasetProperties = new DatasetProperties(data);
      return datasetProperties.hasName() ? datasetProperties.getName() : datasetUrn.getEntityKey().get(1);
    }
    return datasetUrn.getEntityKey().get(1);
  }

  private String getDashboardName(Urn dashboardUrn) {
    DataMap data = getAspectData(dashboardUrn, Constants.DASHBOARD_INFO_ASPECT_NAME);
    if (data != null) {
      return new DashboardInfo(data).getTitle();
    }
    return dashboardUrn.toString();
  }

  private String getChartName(Urn chartUrn) {
    DataMap data = getAspectData(chartUrn, Constants.CHART_INFO_ASPECT_NAME);
    if (data != null) {
      return new ChartInfo(data).getTitle();
    }
    return chartUrn.toString();
  }

  private String getContainerName(Urn containerUrn) {
    DataMap data = getAspectData(containerUrn, Constants.CONTAINER_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new ContainerProperties(data).getName();
    }
    return containerUrn.toString();
  }

  private String getGlossaryTermName(Urn glossaryTermUrn) {
    DataMap data = getAspectData(glossaryTermUrn, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    if (data != null) {
      GlossaryTermInfo info = new GlossaryTermInfo(data);
      return info.hasName() ? info.getName() : glossaryTermUrn.getId();
    }
    return glossaryTermUrn.toString();
  }

  private String getTagName(Urn tagUrn) {
    DataMap data = getAspectData(tagUrn, Constants.TAG_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new TagProperties(data).getName();
    }
    return tagUrn.toString();
  }

  private String getDomainName(Urn domainUrn) {
    DataMap data = getAspectData(domainUrn, Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      return new DomainProperties(data).getName();
    }
    return domainUrn.toString();
  }

  private String getUserName(Urn userUrn) {
    IdentityProvider.User maybeUser = _identityProvider.getUser(userUrn);
    return maybeUser != null ? maybeUser.getResolvedDisplayName() : userUrn.getId();
  }

  private String getGroupName(Urn groupUrn) {
    DataMap data = getAspectData(groupUrn, Constants.CORP_GROUP_INFO_ASPECT_NAME);
    if (data != null) {
      CorpGroupInfo info = new CorpGroupInfo(data);
      return info.hasDisplayName() ? info.getDisplayName() : groupUrn.getId();
    }
    return groupUrn.toString();
  }

  private String getIngestionSourceName(Urn ingestionSourceUrn) {
    DataMap data = getAspectData(ingestionSourceUrn, Constants.INGESTION_INFO_ASPECT_NAME);
    if (data != null) {
      DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo(data);
      return info.hasName() ? info.getName() : ingestionSourceUrn.toString();
    }
    return ingestionSourceUrn.toString();
  }

  private String getDataPlatformName(Urn dataPlatformUrn) {
    DataMap data = getAspectData(dataPlatformUrn, Constants.DATA_PLATFORM_INFO_ASPECT_NAME);
    if (data != null) {
      DataPlatformInfo info = new DataPlatformInfo(data);
      return info.hasDisplayName() ? info.getDisplayName() : info.getName();
    }
    return dataPlatformUrn.toString();
  }


  private String getSchemaFieldName(Urn schemaFieldUrn) {
    return schemaFieldUrn.getEntityKey().get(1);
  }

  private String getMLFeatureName(Urn mlFeatureUrn) {
    DataMap data = getAspectData(mlFeatureUrn, Constants.ML_FEATURE_KEY_ASPECT_NAME);
    if (data != null) {
      MLFeatureKey mlFeatureKey = new MLFeatureKey(data);
      return mlFeatureKey.getName();
    }
    return mlFeatureUrn.getEntityKey().get(1);
  }

  private String getMLModelName(Urn mlModelUrn) {
    DataMap data = getAspectData(mlModelUrn, Constants.ML_MODEL_KEY_ASPECT_NAME);
    if (data != null) {
      MLModelKey mlModelKey = new MLModelKey(data);
      return mlModelKey.getName();
    }
    return mlModelUrn.getEntityKey().get(1);
  }

  private String getMLModelGroupName(Urn mlModelGroupUrn) {
    DataMap data = getAspectData(mlModelGroupUrn, Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME);
    if (data != null) {
      MLModelGroupKey mlModelGroupKey = new MLModelGroupKey(data);
      return mlModelGroupKey.getName();
    }
    return mlModelGroupUrn.getEntityKey().get(1);
  }

  private String getMLFeatureTableName(Urn mlFeatureTableUrn) {
    DataMap data = getAspectData(mlFeatureTableUrn, Constants.ML_FEATURE_TABLE_KEY_ASPECT_NAME);
    if (data != null) {
      MLFeatureTableKey mlFeatureTableKey = new MLFeatureTableKey(data);
      return mlFeatureTableKey.getName();
    }
    return mlFeatureTableUrn.getEntityKey().get(1);
  }

  private String getMLPrimaryKeyName(Urn mlPrimaryKeyUrn) {
    DataMap data = getAspectData(mlPrimaryKeyUrn, Constants.ML_PRIMARY_KEY_KEY_ASPECT_NAME);
    if (data != null) {
      MLPrimaryKeyKey mlPrimaryKeyKey = new MLPrimaryKeyKey(data);
      return mlPrimaryKeyKey.getName();
    }
    return mlPrimaryKeyUrn.getEntityKey().get(1);
  }

  private String getDataProductName(Urn dataProductUrn) {
    DataMap data = getAspectData(dataProductUrn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    if (data != null) {
      DataProductProperties dataProductProperties = new DataProductProperties(data);
      return dataProductProperties.getName();
    }
    return dataProductUrn.toString();
  }

  private String getNotebookName(Urn notebookUrn) {
    DataMap data = getAspectData(notebookUrn, Constants.NOTEBOOK_INFO_ASPECT_NAME);
    if (data != null) {
      NotebookInfo notebookInfo = new NotebookInfo(data);
      return notebookInfo.getTitle();
    }
    return notebookUrn.toString();
  }

  @Nullable
  private DataMap getAspectData(Urn urn, String aspectName) {
    try {
      EntityResponse response = _entityClient.getV2(
          urn.getEntityType(),
          urn,
          ImmutableSet.of(aspectName),
          _systemAuthentication
      );
      if (response != null && response.getAspects().containsKey(aspectName)) {
        return response.getAspects().get(aspectName).getValue().data();
      } else {
        log.warn(String.format("Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
        return null;
      }
    } catch (Exception e) {
      log.error(String.format("Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
      return null;
    }
  }
}