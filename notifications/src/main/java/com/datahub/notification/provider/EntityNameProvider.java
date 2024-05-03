package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Provider of basic information about entity names. */
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
   * Returns the names for use when displaying any entity.
   *
   * <p>Returns the urns of the terms if one cannot be resolved.
   */
  public Map<Urn, String> batchGetName(@Nonnull final Set<Urn> entityUrns, String entityType) {
    switch (entityType) {
      case Constants.DATASET_ENTITY_NAME:
        return batchGetDatasetName(entityUrns);
      case Constants.DASHBOARD_ENTITY_NAME:
        return batchGetDashboardName(entityUrns);
      case Constants.CHART_ENTITY_NAME:
        return batchGetChartName(entityUrns);
      case Constants.DATA_JOB_ENTITY_NAME:
        return batchGetDataJobName(entityUrns);
      case Constants.DATA_FLOW_ENTITY_NAME:
        return batchGetDataFlowName(entityUrns);
      case Constants.CONTAINER_ENTITY_NAME:
        return batchGetContainerName(entityUrns);
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return batchGetGlossaryTermName(entityUrns);
      case Constants.TAG_ENTITY_NAME:
        return batchGetTagName(entityUrns);
      case Constants.DOMAIN_ENTITY_NAME:
        return batchGetDomainName(entityUrns);
      case Constants.CORP_USER_ENTITY_NAME:
        return batchGetUserName(entityUrns);
      case Constants.CORP_GROUP_ENTITY_NAME:
        return batchGetGroupName(entityUrns);
      case Constants.INGESTION_SOURCE_ENTITY_NAME:
        return batchGetIngestionSourceName(entityUrns);
      case Constants.DATA_PLATFORM_ENTITY_NAME:
        return batchGetDataPlatformName(entityUrns);
      case Constants.SCHEMA_FIELD_ENTITY_NAME:
        return batchGetSchemaFieldName(entityUrns);
      case Constants.ML_FEATURE_ENTITY_NAME:
        return batchGetMLFeatureName(entityUrns);
      case Constants.ML_MODEL_ENTITY_NAME:
        return batchGetMLModelName(entityUrns);
      case Constants.ML_MODEL_GROUP_ENTITY_NAME:
        return batchGetMLModelGroupName(entityUrns);
      case Constants.ML_FEATURE_TABLE_ENTITY_NAME:
        return batchGetMLFeatureTableName(entityUrns);
      case Constants.ML_PRIMARY_KEY_ENTITY_NAME:
        return batchGetMLPrimaryKeyName(entityUrns);
      case Constants.DATA_PRODUCT_ENTITY_NAME:
        return batchGetDataProductName(entityUrns);
      case Constants.NOTEBOOK_ENTITY_NAME:
        return batchGetNotebookName(entityUrns);
      default:
        return entityUrns.stream().collect(Collectors.toMap(k -> k, Urn::toString));
    }
  }

  /**
   * Returns the name for use when displaying any entity.
   *
   * <p>Returns the urn of the term if one cannot be resolved.
   */
  public String getName(@Nonnull final Urn entityUrn) {
    return batchGetName(Set.of(entityUrn), entityUrn.getEntityType()).get(entityUrn);
  }

  /**
   * Returns the platform name of any entity.
   *
   * <p>Returns null if not found.
   */
  @Nullable
  public String getPlatformName(@Nonnull final Urn entityUrn) {
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
        return getAssetPlatform(entityUrn);
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
  public String getTypeName(@Nonnull final Urn entityUrn) {
    String maybeSubType =
        batchGetEntitySubTypes(Set.of(entityUrn), entityUrn.getEntityType()).get(entityUrn);
    if (maybeSubType != null) {
      return capitalizeFirstLetter(maybeSubType);
    }
    return getEntityTypeNameFromEntity(entityUrn.getEntityType());
  }

  @Nonnull
  public Map<Urn, String> batchGetTypeNames(
      @Nonnull final Set<Urn> entityUrns, @Nonnull final String entityType) {
    Map<Urn, String> maybeSubTypes = batchGetEntitySubTypes(entityUrns, entityType);
    return entityUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                k -> {
                  final String maybeSubType = maybeSubTypes.get(k);
                  if (maybeSubType != null) {
                    return capitalizeFirstLetter(maybeSubType);
                  }
                  return getEntityTypeNameFromEntity(k.getEntityType());
                }));
  }

  private Map<Urn, String> batchGetDatasetName(Set<Urn> datasetUrns) {
    Map<Urn, DataMap> urnToData =
        batchGetAspectData(
            datasetUrns, Constants.DATASET_ENTITY_NAME, Constants.DATASET_PROPERTIES_ASPECT_NAME);
    return datasetUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                key -> {
                  final DataMap data = urnToData.get(key);
                  if (data == null) {
                    return key.getEntityKey().get(1);
                  }
                  DatasetProperties datasetProperties = new DatasetProperties(data);
                  return datasetProperties.hasName()
                      ? datasetProperties.getName()
                      : key.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetDashboardName(Set<Urn> dashboardUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            dashboardUrns, Constants.DASHBOARD_ENTITY_NAME, Constants.DASHBOARD_INFO_ASPECT_NAME);
    return dashboardUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  final DataMap data = dataMap.get(urn);
                  return data != null ? new DashboardInfo(data).getTitle() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetDataJobName(Set<Urn> dataJobUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            dataJobUrns, Constants.DATA_JOB_ENTITY_NAME, Constants.DATA_JOB_INFO_ASPECT_NAME);
    return dataJobUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  final DataMap data = dataMap.get(urn);
                  return data != null ? new DataJobInfo(data).getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetDataFlowName(Set<Urn> dataFlowUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            dataFlowUrns, Constants.DATA_FLOW_ENTITY_NAME, Constants.DATA_FLOW_INFO_ASPECT_NAME);
    return dataFlowUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new DataFlowInfo(data).getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetChartName(Set<Urn> chartUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            chartUrns, Constants.CHART_ENTITY_NAME, Constants.CHART_INFO_ASPECT_NAME);
    return chartUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new ChartInfo(data).getTitle() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetContainerName(Set<Urn> containerUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            containerUrns,
            Constants.CONTAINER_ENTITY_NAME,
            Constants.CONTAINER_PROPERTIES_ASPECT_NAME);
    return containerUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new ContainerProperties(data).getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetGlossaryTermName(Set<Urn> glossaryTermUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            glossaryTermUrns,
            Constants.GLOSSARY_TERM_ENTITY_NAME,
            Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    return glossaryTermUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  if (data == null) return urn.getId();
                  GlossaryTermInfo info = new GlossaryTermInfo(data);
                  return info.hasName() ? info.getName() : urn.getId();
                }));
  }

  private Map<Urn, String> batchGetTagName(Set<Urn> tagUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            tagUrns, Constants.TAG_ENTITY_NAME, Constants.TAG_PROPERTIES_ASPECT_NAME);
    return tagUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new TagProperties(data).getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetDomainName(Set<Urn> domainUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            domainUrns, Constants.DOMAIN_ENTITY_NAME, Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    return domainUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new DomainProperties(data).getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetUserName(Set<Urn> userUrns) {
    final Map<Urn, IdentityProvider.User> maybeUserMap = _identityProvider.batchGetUsers(userUrns);
    return userUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  if (maybeUserMap == null) return urn.getId();
                  IdentityProvider.User maybeUser = maybeUserMap.get(urn);
                  return maybeUser != null ? maybeUser.getResolvedDisplayName() : urn.getId();
                }));
  }

  private Map<Urn, String> batchGetGroupName(Set<Urn> groupUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            groupUrns, Constants.CORP_GROUP_ENTITY_NAME, Constants.CORP_GROUP_INFO_ASPECT_NAME);
    return groupUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  if (data == null) return urn.toString();
                  CorpGroupInfo info = new CorpGroupInfo(data);
                  return info.hasDisplayName() ? info.getDisplayName() : urn.getId();
                }));
  }

  private Map<Urn, String> batchGetIngestionSourceName(Set<Urn> ingestionSourceUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            ingestionSourceUrns,
            Constants.INGESTION_SOURCE_ENTITY_NAME,
            Constants.INGESTION_INFO_ASPECT_NAME);
    return ingestionSourceUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  if (data == null) return urn.toString();
                  DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo(data);
                  return info.hasName() ? info.getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetDataPlatformName(Set<Urn> dataPlatformUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            dataPlatformUrns,
            Constants.DATA_PLATFORM_ENTITY_NAME,
            Constants.DATA_PLATFORM_INFO_ASPECT_NAME);
    return dataPlatformUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  if (data == null) return urn.toString();
                  DataPlatformInfo info = new DataPlatformInfo(data);
                  return info.hasDisplayName() ? info.getDisplayName() : info.getName();
                }));
  }

  private Map<Urn, String> batchGetSchemaFieldName(Set<Urn> schemaFieldUrns) {
    return schemaFieldUrns.stream()
        .collect(Collectors.toMap(k -> k, urn -> urn.getEntityKey().get(1)));
  }

  private Map<Urn, String> batchGetMLFeatureName(Set<Urn> mlFeatureUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            mlFeatureUrns, Constants.ML_FEATURE_ENTITY_NAME, Constants.ML_FEATURE_KEY_ASPECT_NAME);
    return mlFeatureUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null
                      ? new MLFeatureKey(data).getName()
                      : urn.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetMLModelName(Set<Urn> mlModelUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            mlModelUrns, Constants.ML_MODEL_ENTITY_NAME, Constants.ML_MODEL_KEY_ASPECT_NAME);
    return mlModelUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new MLModelKey(data).getName() : urn.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetMLModelGroupName(Set<Urn> mlModelGroupUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            mlModelGroupUrns,
            Constants.ML_MODEL_GROUP_ENTITY_NAME,
            Constants.ML_MODEL_GROUP_KEY_ASPECT_NAME);
    return mlModelGroupUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null
                      ? new MLModelGroupKey(data).getName()
                      : urn.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetMLFeatureTableName(Set<Urn> mlFeatureTableUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            mlFeatureTableUrns,
            Constants.ML_FEATURE_TABLE_ENTITY_NAME,
            Constants.ML_FEATURE_TABLE_KEY_ASPECT_NAME);
    return mlFeatureTableUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null
                      ? new MLFeatureTableKey(data).getName()
                      : urn.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetMLPrimaryKeyName(Set<Urn> mlPrimaryKeyUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            mlPrimaryKeyUrns,
            Constants.ML_PRIMARY_KEY_ENTITY_NAME,
            Constants.ML_PRIMARY_KEY_KEY_ASPECT_NAME);
    return mlPrimaryKeyUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null
                      ? new MLPrimaryKeyKey(data).getName()
                      : urn.getEntityKey().get(1);
                }));
  }

  private Map<Urn, String> batchGetDataProductName(Set<Urn> dataProductUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            dataProductUrns,
            Constants.DATA_PRODUCT_ENTITY_NAME,
            Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    return dataProductUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  if (data == null) return urn.toString();
                  DataProductProperties productProperties = new DataProductProperties(data);
                  return productProperties.hasName() ? productProperties.getName() : urn.toString();
                }));
  }

  private Map<Urn, String> batchGetNotebookName(Set<Urn> notebookUrns) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(
            notebookUrns, Constants.NOTEBOOK_ENTITY_NAME, Constants.NOTEBOOK_INFO_ASPECT_NAME);
    return notebookUrns.stream()
        .collect(
            Collectors.toMap(
                k -> k,
                urn -> {
                  DataMap data = dataMap.get(urn);
                  return data != null ? new NotebookInfo(data).getTitle() : urn.toString();
                }));
  }

  @Nullable
  private String getAssetPlatform(Urn assetUrn) {
    DataMap data =
        batchGetAspectData(
                Set.of(assetUrn),
                assetUrn.getEntityType(),
                Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)
            .get(assetUrn);
    if (data == null) {
      return null;
    }
    DataPlatformInstance dataPlatformInstance = new DataPlatformInstance(data);
    if (!dataPlatformInstance.hasPlatform()) {
      return null;
    }
    Urn platformUrn = dataPlatformInstance.getPlatform();
    DataMap platformData =
        batchGetAspectData(
                Set.of(platformUrn),
                platformUrn.getEntityType(),
                Constants.DATA_PLATFORM_INFO_ASPECT_NAME)
            .get(platformUrn);
    if (platformData != null) {
      DataPlatformInfo info = new DataPlatformInfo(platformData);
      return info.hasDisplayName() ? info.getDisplayName() : info.getName();
    }
    return dataPlatformInstance.getPlatform().getId();
  }

  @Nonnull
  private Map<Urn, String> batchGetEntitySubTypes(Set<Urn> assetUrns, String entityType) {
    Map<Urn, DataMap> dataMap =
        batchGetAspectData(assetUrns, entityType, Constants.SUB_TYPES_ASPECT_NAME);
    return dataMap.entrySet().stream()
        .map(
            entry -> {
              SubTypes subTypes = new SubTypes(entry.getValue());
              if (subTypes.hasTypeNames() && !subTypes.getTypeNames().isEmpty()) {
                return new AbstractMap.SimpleEntry<>(
                    entry.getKey(), subTypes.getTypeNames().get(0));
              }
              return null;
            })
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
  }

  @Nonnull
  private Map<Urn, DataMap> batchGetAspectData(
      Set<Urn> urns, String entityType, String aspectName) {
    try {
      Map<Urn, EntityResponse> response =
          _entityClient.batchGetV2(
              entityType, urns, ImmutableSet.of(aspectName), _systemAuthentication);
      if (!response.isEmpty()) {
        Map<Urn, DataMap> toReturn = new HashMap<>();
        response.forEach(
            (key, value) -> {
              if (value.getAspects().containsKey(aspectName)) {
                toReturn.put(key, value.getAspects().get(aspectName).getValue().data());
              }
            });
        return toReturn;
      } else {
        log.warn(
            String.format(
                "Failed to get aspect data for  urns %s aspect %s",
                urns.stream().limit(100).map(Object::toString).collect(Collectors.joining(", ")),
                aspectName));
        return Collections.emptyMap();
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to get aspect data for  urn %s aspect %s",
              urns.stream().limit(100).map(Object::toString).collect(Collectors.joining(", ")),
              aspectName));
      return Collections.emptyMap();
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
