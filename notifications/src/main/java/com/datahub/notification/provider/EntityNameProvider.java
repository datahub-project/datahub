package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.DataMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
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
      case "schemaField":
        return getSchemaFieldName(entityUrn);
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

  private String getSchemaFieldName(Urn schemaFieldUrn) {
    return schemaFieldUrn.getEntityKey().get(1);
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