package io.datahubproject.openlineage.converter;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataprocess.DataProcessInstanceRunResult;
import com.linkedin.dataprocess.DataProcessRunStatus;
import com.linkedin.dataprocess.RunResultType;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.domain.Domains;
import com.linkedin.schema.MapType;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.datahubproject.openlineage.dataset.HdfsPlatform;
import io.datahubproject.openlineage.dataset.PathSpec;
import io.datahubproject.openlineage.utils.DatahubUtils;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
public class OpenLineageToDataHub {

  // Constants
  public static final String FILE_NAMESPACE = "file";
  public static final String SCHEME_SEPARATOR = "://";
  public static final String URN_LI_CORPUSER = "urn:li:corpuser:";
  public static final String URN_LI_CORPUSER_DATAHUB = URN_LI_CORPUSER + "datahub";
  public static final String URN_LI_DATA_PROCESS_INSTANCE = "urn:li:dataProcessInstance:";

  // Custom property keys
  public static final String PROCESSING_ENGINE_KEY = "processingEngine";
  public static final String PROCESSING_ENGINE_VERSION_KEY = "processingEngineVersion";
  public static final String OPENLINEAGE_ADAPTER_VERSION_KEY = "openlineageAdapterVersion";
  public static final String JOB_ID_KEY = "jobId";
  public static final String JOB_DESCRIPTION_KEY = "jobDescription";
  public static final String JOB_GROUP_KEY = "jobGroup";
  public static final String JOB_CALL_SITE_KEY = "jobCallSite";
  public static final String SPARK_VERSION_KEY = "spark-version";
  public static final String OPENLINEAGE_SPARK_VERSION_KEY = "openlineage-spark-version";
  public static final String SPARK_LOGICAL_PLAN_KEY = "spark.logicalPlan";

  // SQL patterns
  public static final String MERGE_INTO_COMMAND_PATTERN = "execute_merge_into_command_edge";
  public static final String MERGE_INTO_SQL_PATTERN = "MERGE INTO";
  public static final String TABLE_PREFIX = "table/";
  public static final String WAREHOUSE_PATH_PATTERN = "/warehouse/";
  public static final String DB_SUFFIX = ".db/";

  public static final Map<String, String> PLATFORM_MAP =
      Stream.of(
              new String[][] {
                {"awsathena", "athena"}, {"sqlserver", "mssql"},
              })
          .collect(Collectors.toMap(data -> data[0], data -> data[1]));

  private OpenLineageToDataHub() {}

  public static Optional<DatasetUrn> convertOpenlineageDatasetToDatasetUrn(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {

    String namespace = dataset.getNamespace();
    String datasetName = dataset.getName();
    Optional<DatasetUrn> datahubUrn;

    if (dataset.getFacets() != null
        && dataset.getFacets().getSymlinks() != null
        && !mappingConfig.isDisableSymlinkResolution()) {
      Optional<DatasetUrn> originalUrn =
          getDatasetUrnFromOlDataset(namespace, datasetName, mappingConfig);
      for (OpenLineage.SymlinksDatasetFacetIdentifiers symlink :
          dataset.getFacets().getSymlinks().getIdentifiers()) {
        if ("TABLE".equals(symlink.getType())) {
          // Before OpenLineage 0.17.1 the namespace started with "aws:glue:" and after that it was
          // changed to :arn:aws:glue:"
          if (symlink.getNamespace().startsWith("aws:glue:")
              || symlink.getNamespace().startsWith("arn:aws:glue:")) {
            namespace = "glue";
          } else {
            namespace = mappingConfig.getHivePlatformAlias();
          }
          if (symlink.getName().startsWith(TABLE_PREFIX)) {
            datasetName = symlink.getName().replaceFirst(TABLE_PREFIX, "").replace("/", ".");
          } else {
            datasetName = symlink.getName();
          }
        }
      }
      Optional<DatasetUrn> symlinkedUrn =
          getDatasetUrnFromOlDataset(namespace, datasetName, mappingConfig);
      if (symlinkedUrn.isPresent() && originalUrn.isPresent()) {
        mappingConfig
            .getUrnAliases()
            .put(originalUrn.get().toString(), symlinkedUrn.get().toString());
      }
      datahubUrn = symlinkedUrn;
    } else {
      datahubUrn = getDatasetUrnFromOlDataset(namespace, datasetName, mappingConfig);
    }

    log.debug("Dataset URN: {}, alias_list: {}", datahubUrn, mappingConfig.getUrnAliases());
    // If we have the urn in urn aliases then we should use the alias instead of the original urn
    if (datahubUrn.isPresent()
        && mappingConfig.getUrnAliases().containsKey(datahubUrn.get().toString())) {
      try {
        datahubUrn =
            Optional.of(
                DatasetUrn.createFromString(
                    mappingConfig.getUrnAliases().get(datahubUrn.get().toString())));
        return datahubUrn;
      } catch (URISyntaxException e) {
        log.warn("Failed to create URN from alias: {}", e.getMessage());
        return Optional.empty();
      }
    }

    return datahubUrn;
  }

  private static Optional<DatasetUrn> getDatasetUrnFromOlDataset(
      String namespace, String datasetName, DatahubOpenlineageConfig mappingConfig) {
    String platform;
    if (mappingConfig.isLowerCaseDatasetUrns()) {
      namespace = namespace.toLowerCase();
      datasetName = datasetName.toLowerCase();
    }

    if (namespace.contains(SCHEME_SEPARATOR)) {
      try {
        URI datasetUri;
        if (!namespace.endsWith("/") && !datasetName.startsWith("/")) {
          datasetUri = new URI(namespace + "/" + datasetName);
        } else {
          datasetUri = new URI(namespace + datasetName);
        }
        if (PLATFORM_MAP.containsKey(datasetUri.getScheme())) {
          platform = PLATFORM_MAP.get(datasetUri.getScheme());
        } else {
          platform = datasetUri.getScheme();
        }
        if (HdfsPlatform.isFsPlatformPrefix(platform)) {
          datasetName = datasetUri.getPath();
          try {
            HdfsPathDataset hdfsPathDataset = HdfsPathDataset.create(datasetUri, mappingConfig);
            DatasetUrn urn = hdfsPathDataset.urn();
            return Optional.of(urn);
          } catch (InstantiationException e) {
            log.warn(
                "Unable to create urn from namespace: {} and dataset {}.", namespace, datasetName);
            return Optional.empty();
          }
        }
      } catch (URISyntaxException e) {
        log.warn("Unable to create URI from namespace: {} and dataset {}.", namespace, datasetName);
        return Optional.empty();
      }
    } else {
      platform = namespace;
    }

    String platformInstance = getPlatformInstance(mappingConfig, platform, namespace);
    FabricType env = getEnv(mappingConfig, platform, namespace);
    DatasetUrn urn = DatahubUtils.createDatasetUrn(platform, platformInstance, datasetName, env);
    return Optional.of(urn);
  }

  private static FabricType getEnv(
      DatahubOpenlineageConfig mappingConfig, String platform, String namespace) {
    // First check if namespace contains URI format and can be mapped to an environment
    if (namespace != null && namespace.contains(SCHEME_SEPARATOR)) {
      try {
        URI namespaceUri = new URI(namespace);
        String namespaceAuthority = namespaceUri.getAuthority();

        if (namespaceAuthority != null && !mappingConfig.getEnvironmentNamespaceMap().isEmpty()) {
          // Check if we have a direct match for this namespace URI
          String environmentName = mappingConfig.getEnvironmentNamespaceMap().get(namespace);
          if (environmentName != null) {
            try {
              FabricType fabricType = FabricType.valueOf(environmentName.toUpperCase());
              log.debug("Found environment {} for namespace {}", fabricType, namespace);
              return fabricType;
            } catch (IllegalArgumentException e) {
              log.warn("Invalid environment value in namespace mapping: {}", environmentName);
            }
          }
        }
      } catch (URISyntaxException e) {
        log.warn("Unable to parse namespace URI for environment mapping: {}", namespace);
      }
    }

    FabricType fabricType = mappingConfig.getFabricType();
    if (mappingConfig.getPathSpecs() != null
        && mappingConfig.getPathSpecs().containsKey(platform)) {
      List<PathSpec> pathSpecs = mappingConfig.getPathSpecs().get(platform);
      for (PathSpec pathSpec : pathSpecs) {
        if (pathSpec.getEnv().isPresent()) {
          try {
            fabricType = FabricType.valueOf(pathSpec.getEnv().get());
            return fabricType;
          } catch (IllegalArgumentException e) {
            log.warn("Invalid environment value: {}", pathSpec.getEnv());
          }
        }
      }
    }
    return fabricType;
  }

  private static String getPlatformInstance(
      DatahubOpenlineageConfig mappingConfig, String platform, String namespace) {
    // First check if namespace contains URI format and can be mapped to a platform instance
    if (namespace != null && namespace.contains(SCHEME_SEPARATOR)) {
      try {
        URI namespaceUri = new URI(namespace);
        String namespaceAuthority = namespaceUri.getAuthority();

        if (namespaceAuthority != null
            && !mappingConfig.getPlatformInstanceNamespaceMap().isEmpty()) {
          // Check if we have a direct match for this namespace URI
          String platformInstanceName =
              mappingConfig.getPlatformInstanceNamespaceMap().get(namespace);
          if (platformInstanceName != null) {
            log.debug(
                "Found platform instance {} for namespace {}", platformInstanceName, namespace);
            return platformInstanceName;
          }
        }
      } catch (URISyntaxException e) {
        log.warn("Unable to parse namespace URI: {}", namespace);
      }
    }

    // Use the platform instance from the path spec if it is present otherwise use the one from the
    // commonDatasetPlatformInstance
    String platformInstance = mappingConfig.getCommonDatasetPlatformInstance();
    if (mappingConfig.getPathSpecs() != null
        && mappingConfig.getPathSpecs().containsKey(platform)) {
      List<PathSpec> pathSpecs = mappingConfig.getPathSpecs().get(platform);
      for (PathSpec pathSpec : pathSpecs) {
        if (pathSpec.getPlatformInstance().isPresent()) {
          return pathSpec.getPlatformInstance().get();
        }
      }
    }
    return platformInstance;
  }

  public static GlobalTags generateTags(List<String> tags) {
    tags.sort(String::compareToIgnoreCase);
    GlobalTags globalTags = new GlobalTags();
    TagAssociationArray tagAssociationArray = new TagAssociationArray();
    for (String tag : tags) {
      TagAssociation tagAssociation = new TagAssociation();
      tagAssociation.setTag(new TagUrn(tag));
      tagAssociationArray.add(tagAssociation);
    }
    globalTags.setTags(tagAssociationArray);
    return globalTags;
  }

  public static Domains generateDomains(List<String> domains) {
    domains.sort(String::compareToIgnoreCase);
    Domains datahubDomains = new Domains();
    UrnArray domainArray = new UrnArray();
    for (String domain : domains) {
      try {
        domainArray.add(Urn.createFromString(domain));
      } catch (URISyntaxException e) {
        log.warn("Unable to create domain urn for domain urn: {}", domain);
      }
    }
    datahubDomains.setDomains(domainArray);
    return datahubDomains;
  }

  public static Urn dataPlatformInstanceUrn(String platform, String instance) {
    return new Urn(
        "dataPlatformInstance",
        new TupleKey(Arrays.asList(new DataPlatformUrn(platform).toString(), instance)));
  }

  public static DatahubJob convertRunEventToJob(
      OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws IOException, URISyntaxException {
    DatahubJob.DatahubJobBuilder jobBuilder = DatahubJob.builder();

    if (event.getEventTime() != null) {
      jobBuilder.eventTime(event.getEventTime().toInstant().toEpochMilli());
    }

    log.info("Emitting lineage: {}", OpenLineageClientUtils.toJson(event));
    DataFlowInfo dfi = convertRunEventToDataFlowInfo(event, datahubConf.getPipelineName());

    String processingEngine = null;

    if (event.getRun().getFacets() != null
        && event.getRun().getFacets().getProcessing_engine() != null) {
      processingEngine = event.getRun().getFacets().getProcessing_engine().getName();
    }

    DataFlowUrn dataFlowUrn =
        getFlowUrn(
            event.getJob().getNamespace(),
            event.getJob().getName(),
            processingEngine,
            event.getProducer(),
            datahubConf);
    jobBuilder.flowUrn(dataFlowUrn);

    if (datahubConf.getPlatformInstance() != null) {
      DataPlatformInstance dpi =
          new DataPlatformInstance()
              .setPlatform(new DataPlatformUrn(dataFlowUrn.getOrchestratorEntity()))
              .setInstance(
                  dataPlatformInstanceUrn(
                      dataFlowUrn.getOrchestratorEntity(), datahubConf.getPlatformInstance()));
      jobBuilder.flowPlatformInstance(dpi);
    }

    StringMap customProperties = generateCustomProperties(event, true);
    dfi.setCustomProperties(customProperties);

    String description = getDescription(event);
    if (description != null) {
      dfi.setDescription(description);
    }
    jobBuilder.dataFlowInfo(dfi);

    Ownership ownership = generateOwnership(event);
    jobBuilder.flowOwnership(ownership);

    GlobalTags tags = generateTags(event);
    jobBuilder.flowGlobalTags(tags);

    DatahubJob datahubJob = jobBuilder.build();
    convertJobToDataJob(datahubJob, event, datahubConf);
    return datahubJob;
  }

  static void forEachValue(Map<String, Object> source, StringMap customProperties) {
    for (final Map.Entry<String, Object> entry : source.entrySet()) {
      if (entry.getValue() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedMap = (Map<String, Object>) entry.getValue();
        forEachValue(nestedMap, customProperties);
      } else {
        customProperties.put(entry.getKey(), entry.getValue().toString());
      }
    }
  }

  private static Ownership generateOwnership(OpenLineage.RunEvent event) {
    Ownership ownership = new Ownership();
    OwnerArray owners = new OwnerArray();
    if ((event.getJob().getFacets() != null)
        && (event.getJob().getFacets().getOwnership() != null)) {
      for (OpenLineage.OwnershipJobFacetOwners ownerFacet :
          event.getJob().getFacets().getOwnership().getOwners()) {
        Owner owner = new Owner();
        try {
          owner.setOwner(Urn.createFromString(URN_LI_CORPUSER + ownerFacet.getName()));
          owner.setType(OwnershipType.DEVELOPER);
          OwnershipSource source = new OwnershipSource();
          source.setType(OwnershipSourceType.SERVICE);
          owner.setSource(source);
          owners.add(owner);
        } catch (URISyntaxException e) {
          log.warn("Unable to create owner urn for owner: {}", ownerFacet.getName());
        }
      }
    }
    ownership.setOwners(owners);
    try {
      AuditStamp auditStamp = new AuditStamp();
      auditStamp.setActor(Urn.createFromString(URN_LI_CORPUSER_DATAHUB));
      auditStamp.setTime(System.currentTimeMillis());
      ownership.setLastModified(auditStamp);
    } catch (URISyntaxException e) {
      log.warn("Unable to create actor urn for actor: {}", URN_LI_CORPUSER_DATAHUB);
    }
    return ownership;
  }

  private static String getDescription(OpenLineage.RunEvent event) {
    if (event.getJob().getFacets() != null
        && event.getJob().getFacets().getDocumentation() != null) {
      return event.getJob().getFacets().getDocumentation().getDescription();
    }
    return null;
  }

  private static UpstreamLineage getFineGrainedLineage(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    FineGrainedLineageArray fgla = new FineGrainedLineageArray();
    UpstreamArray upstreams = new UpstreamArray();

    if ((dataset.getFacets() == null) || (dataset.getFacets().getColumnLineage() == null)) {
      return null;
    }

    OpenLineage.ColumnLineageDatasetFacet columnLineage = dataset.getFacets().getColumnLineage();
    Set<Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional>> fields =
        columnLineage.getFields().getAdditionalProperties().entrySet();
    for (Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> field : fields) {
      FineGrainedLineage fgl = new FineGrainedLineage();

      UrnArray upstreamFields = new UrnArray();
      UrnArray downstreamsFields = new UrnArray();
      Optional<DatasetUrn> datasetUrn =
          convertOpenlineageDatasetToDatasetUrn(dataset, mappingConfig);
      datasetUrn.ifPresent(
          urn ->
              downstreamsFields.add(
                  UrnUtils.getUrn("urn:li:schemaField:" + "(" + urn + "," + field.getKey() + ")")));

      LinkedHashSet<String> transformationTexts = new LinkedHashSet<>();
      OpenLineage.StaticDatasetBuilder staticDatasetBuilder =
          new OpenLineage.StaticDatasetBuilder();
      field
          .getValue()
          .getInputFields()
          .forEach(
              inputField -> {
                OpenLineage.Dataset staticDataset =
                    staticDatasetBuilder
                        .name(inputField.getName())
                        .namespace(inputField.getNamespace())
                        .build();

                if (inputField.getTransformations() != null) {
                  for (OpenLineage.InputFieldTransformations transformation :
                      inputField.getTransformations()) {
                    transformationTexts.add(
                        String.format(
                            "%s:%s", transformation.getType(), transformation.getSubtype()));
                  }
                }
                Optional<DatasetUrn> urn =
                    convertOpenlineageDatasetToDatasetUrn(staticDataset, mappingConfig);
                if (urn.isPresent()) {
                  Urn datasetFieldUrn =
                      UrnUtils.getUrn(
                          "urn:li:schemaField:"
                              + "("
                              + urn.get()
                              + ","
                              + inputField.getField()
                              + ")");
                  upstreamFields.add(datasetFieldUrn);
                  if (upstreams.stream()
                      .noneMatch(
                          upstream ->
                              upstream.getDataset().toString().equals(urn.get().toString()))) {
                    upstreams.add(
                        new Upstream()
                            .setDataset(urn.get())
                            .setType(DatasetLineageType.TRANSFORMED));
                  }
                }
              });

      upstreamFields.sort(Comparator.comparing(Urn::toString));
      fgl.setUpstreams(upstreamFields);
      fgl.setConfidenceScore(0.5f);
      fgl.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);

      downstreamsFields.sort(Comparator.comparing(Urn::toString));
      fgl.setDownstreams(downstreamsFields);
      fgl.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);

      // Capture transformation information from OpenLineage
      if (!transformationTexts.isEmpty()) {
        List<String> sortedList =
            transformationTexts.stream()
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.toList());
        fgl.setTransformOperation(String.join(",", sortedList));
      }

      fgla.add(fgl);
    }

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    upstreamLineage.setFineGrainedLineages(fgla);
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }

  private static GlobalTags generateTags(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() == null
        || event.getRun().getFacets().getAdditionalProperties() == null
        || event.getRun().getFacets().getAdditionalProperties().get("airflow") == null
        || event
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get("airflow")
                .getAdditionalProperties()
                .get("dag")
            == null) {
      return null;
    }
    Map<String, Object> airflowProperties =
        event
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .get("airflow")
            .getAdditionalProperties();
    @SuppressWarnings("unchecked")
    Map<String, Object> dagProperties = (Map<String, Object>) airflowProperties.get("dag");
    if (dagProperties.get("tags") != null) {
      try {
        JSONArray arr = new JSONArray(((String) dagProperties.get("tags")).replace("'", "\""));
        LinkedList<String> tags = new LinkedList<>();
        for (int i = 0; i < arr.length(); i++) {
          tags.add(arr.getString(i));
        }
        return generateTags(tags);
      } catch (JSONException e) {
        log.warn("Unable to parse tags from airflow properties: {}", e.getMessage());
        return null;
      }
    }

    return null;
  }

  private static StringMap generateCustomProperties(
      OpenLineage.RunEvent event, boolean flowProperties) {
    StringMap customProperties = new StringMap();
    if ((event.getRun().getFacets() != null)
        && (event.getRun().getFacets().getProcessing_engine() != null)) {
      if (event.getRun().getFacets().getProcessing_engine().getName() != null) {
        customProperties.put(
            PROCESSING_ENGINE_KEY, event.getRun().getFacets().getProcessing_engine().getName());
      }

      if (event.getRun().getFacets().getProcessing_engine().getVersion() != null) {
        customProperties.put(
            PROCESSING_ENGINE_VERSION_KEY,
            event.getRun().getFacets().getProcessing_engine().getVersion());
      }
      if (event.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()
          != null) {
        customProperties.put(
            OPENLINEAGE_ADAPTER_VERSION_KEY,
            event.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion());
      }
    }

    if ((event.getRun().getFacets() == null)
        || (event.getRun().getFacets().getAdditionalProperties() == null)) {
      return customProperties;
    }

    for (Map.Entry<String, OpenLineage.RunFacet> entry :
        event.getRun().getFacets().getAdditionalProperties().entrySet()) {
      processRunFacetEntry(entry, customProperties, flowProperties);
    }
    return customProperties;
  }

  private static void processRunFacetEntry(
      Map.Entry<String, OpenLineage.RunFacet> entry,
      StringMap customProperties,
      boolean flowProperties) {
    switch (entry.getKey()) {
      case "spark_jobDetails":
        processSparkJobDetails(entry.getValue(), customProperties);
        break;
      case "processing_engine":
        processProcessingEngine(entry.getValue(), customProperties);
        break;
      case "spark_version":
        processSparkVersion(entry.getValue(), customProperties);
        break;
      case "spark_properties":
        processSparkProperties(entry.getValue(), customProperties);
        break;
      case "airflow":
        processAirflowProperties(entry.getValue(), customProperties, flowProperties);
        break;
      case "spark.logicalPlan":
        processSparkLogicalPlan(entry.getValue(), customProperties, flowProperties);
        break;
      case "unknownSourceAttribute":
        processUnknownSourceAttributes(entry.getValue(), customProperties, flowProperties);
        break;
      default:
        break;
    }
  }

  private static void processSparkJobDetails(
      OpenLineage.RunFacet facet, StringMap customProperties) {
    Map<String, Object> properties = facet.getAdditionalProperties();
    if (properties.get("jobId") != null) {
      customProperties.put(JOB_ID_KEY, properties.get("jobId").toString());
    }
    if (properties.get("jobDescription") != null) {
      customProperties.put(JOB_DESCRIPTION_KEY, (String) properties.get("jobDescription"));
    }
    if (properties.get("jobGroup") != null) {
      customProperties.put(JOB_GROUP_KEY, (String) properties.get("jobGroup"));
    }
    if (properties.get("jobCallSite") != null) {
      customProperties.put(JOB_CALL_SITE_KEY, (String) properties.get("jobCallSite"));
    }
  }

  private static void processProcessingEngine(
      OpenLineage.RunFacet facet, StringMap customProperties) {
    Map<String, Object> properties = facet.getAdditionalProperties();
    if (properties.get("name") != null) {
      customProperties.put(PROCESSING_ENGINE_KEY, (String) properties.get("name"));
    }
    if (properties.get("version") != null) {
      customProperties.put(PROCESSING_ENGINE_VERSION_KEY, (String) properties.get("version"));
    }
    if (properties.get("openlineageAdapterVersion") != null) {
      customProperties.put(
          OPENLINEAGE_ADAPTER_VERSION_KEY, (String) properties.get("openlineageAdapterVersion"));
    }
  }

  private static void processSparkVersion(OpenLineage.RunFacet facet, StringMap customProperties) {
    Map<String, Object> properties = facet.getAdditionalProperties();
    if (properties.get("spark-version") != null) {
      customProperties.put(SPARK_VERSION_KEY, (String) properties.get("spark-version"));
    }
    if (properties.get("openlineage-spark-version") != null) {
      customProperties.put(
          OPENLINEAGE_SPARK_VERSION_KEY, (String) properties.get("openlineage-spark-version"));
    }
  }

  private static void processSparkProperties(
      OpenLineage.RunFacet facet, StringMap customProperties) {
    if (facet != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> sparkProperties =
          (Map<String, Object>) facet.getAdditionalProperties().get("properties");
      log.info("Spark properties: {}, Properties: {}", facet, sparkProperties);
      if (sparkProperties != null) {
        forEachValue(sparkProperties, customProperties);
      }
    }
  }

  private static void processAirflowProperties(
      OpenLineage.RunFacet facet, StringMap customProperties, boolean flowProperties) {
    @SuppressWarnings("unchecked")
    Map<String, Object> airflowProperties =
        flowProperties
            ? (Map<String, Object>) facet.getAdditionalProperties().get("dag")
            : (Map<String, Object>) facet.getAdditionalProperties().get("task");
    if (airflowProperties != null) {
      forEachValue(airflowProperties, customProperties);
    }
  }

  private static void processSparkLogicalPlan(
      OpenLineage.RunFacet facet, StringMap customProperties, boolean flowProperties) {
    if (flowProperties) {
      JSONObject jsonObject = new JSONObject(facet.getAdditionalProperties());
      customProperties.put(SPARK_LOGICAL_PLAN_KEY, jsonObject.toString());
    }
  }

  private static void processUnknownSourceAttributes(
      OpenLineage.RunFacet facet, StringMap customProperties, boolean flowProperties) {
    if (!flowProperties) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> unknownItems =
          (List<Map<String, Object>>)
              facet.getAdditionalProperties().getOrDefault("unknownItems", Collections.emptyList());
      for (Map<String, Object> item : unknownItems) {
        forEachValue(item, customProperties);
      }
    }
  }

  // Helper method to check for RDD transformations that don't create new datasets
  private static boolean isNonMaterializingRddTransformation(String jobName) {
    // These transformations work on the same logical dataset without materializing new ones
    String[] nonMaterializingTransformations = {
      // Element-wise transformations (1-to-1 mapping)
      "map_parallel_collection",
      "map_text_file",
      "map_hadoopfile",
      "map_partitions_parallel_collection",
      "map_partitions_text_file",
      "map_partitions_hadoopfile",
      "flatmap_parallel_collection",
      "flatmap_text_file",
      "flatmap_hadoopfile",

      // Filtering operations (subset of same dataset)
      "filter_parallel_collection",
      "filter_text_file",
      "filter_hadoopfile",

      // Deduplication (subset of same dataset)
      "distinct_parallel_collection",
      "distinct_text_file",
      "distinct_hadoopfile"
    };

    for (String transformation : nonMaterializingTransformations) {
      if (jobName.endsWith(transformation)) {
        return true;
      }
    }

    return false;
  }

  private static void convertJobToDataJob(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws URISyntaxException {

    OpenLineage.Job job = event.getJob();
    DataJobInfo dji = new DataJobInfo();

    log.debug("Datahub Config: {}", datahubConf);

    // Extract job names using helper method
    JobNameResult jobNames = extractJobNames(job, event, datahubConf);

    // Set the display name
    dji.setName(jobNames.displayName);

    String jobProcessingEngine = null;
    if ((event.getRun().getFacets() != null)
        && (event.getRun().getFacets().getProcessing_engine() != null)) {
      jobProcessingEngine = event.getRun().getFacets().getProcessing_engine().getName();
    }

    DataFlowUrn flowUrn =
        getFlowUrn(
            event.getJob().getNamespace(),
            job.getName(), // Use original job name for flow URN
            jobProcessingEngine,
            event.getProducer(),
            datahubConf);

    dji.setFlowUrn(flowUrn);
    dji.setType(DataJobInfo.Type.create(flowUrn.getOrchestratorEntity()));

    // Use the jobNameForUrn (which includes table name for MERGE commands)
    DataJobUrn dataJobUrn = new DataJobUrn(flowUrn, jobNames.urnName);
    datahubJob.setJobUrn(dataJobUrn);

    StringMap customProperties = generateCustomProperties(event, false);
    dji.setCustomProperties(customProperties);

    TimeStamp timestamp = new TimeStamp();

    if (event.getEventTime() != null) {
      dji.setCreated(timestamp.setTime(event.getEventTime().toInstant().toEpochMilli()));
    }

    String description = getDescription(event);
    if (description != null) {
      dji.setDescription(description);
    }
    datahubJob.setJobInfo(dji);

    // Process inputs and outputs
    boolean inputsEqualOutputs = checkInputsEqualOutputs(event, job, datahubConf);

    processJobInputs(datahubJob, event, datahubConf);

    if (!inputsEqualOutputs) {
      processJobOutputs(datahubJob, event, datahubConf);
    }

    // Set run event and instance properties
    DataProcessInstanceRunEvent dpire = processDataProcessInstanceResult(event);
    datahubJob.setDataProcessInstanceRunEvent(dpire);

    DataProcessInstanceProperties dpiProperties = getJobDataProcessInstanceProperties(event);
    datahubJob.setDataProcessInstanceProperties(dpiProperties);

    // Create input/output edges and relationships
    DataJobInputOutput inputOutput = new DataJobInputOutput();
    processParentJob(event, job, jobNames.urnName, inputOutput, datahubConf);

    DataProcessInstanceRelationships dataProcessInstanceRelationships =
        new DataProcessInstanceRelationships();
    dataProcessInstanceRelationships.setParentTemplate(dataJobUrn);
    dataProcessInstanceRelationships.setUpstreamInstances(new UrnArray());
    datahubJob.setDataProcessInstanceRelationships(dataProcessInstanceRelationships);

    try {
      Urn dpiUrn =
          Urn.createFromString(URN_LI_DATA_PROCESS_INSTANCE + event.getRun().getRunId().toString());
      datahubJob.setDataProcessInstanceUrn(dpiUrn);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create dataprocess instance urn:" + e);
    }
  }

  private static class JobNameResult {
    final String displayName;
    final String urnName;

    JobNameResult(String displayName, String urnName) {
      this.displayName = displayName;
      this.urnName = urnName;
    }
  }

  private static JobNameResult extractJobNames(
      OpenLineage.Job job, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {

    // Check if we have a MERGE INTO command
    boolean isMergeIntoCommand = job.getName().contains(MERGE_INTO_COMMAND_PATTERN);
    String tableName = null;

    // If this is a MERGE INTO command and enhanced extraction is enabled, try to extract the target
    // table name
    if (isMergeIntoCommand && datahubConf.isEnhancedMergeIntoExtraction()) {
      log.info("Detected MERGE INTO command in job: {} - using enhanced extraction", job.getName());
      tableName = extractTableNameFromMergeCommand(job, event);
    }

    // Prepare job names - one for display and one for the URN
    String jobNameForDisplay = job.getName();
    String jobNameForUrn = job.getName();

    // If this is a merge command with an identified table, include the table name
    if (isMergeIntoCommand && tableName != null && datahubConf.isEnhancedMergeIntoExtraction()) {
      // Create modified job names that include the table name
      String tablePart = tableName.replace(".", "_").replace(" ", "_").toLowerCase();
      String enhancedJobName = job.getName() + "." + tablePart;

      log.info("Modified job name for MERGE INTO: {} -> {}", job.getName(), enhancedJobName);

      // Use the enhanced name for URN
      jobNameForUrn = enhancedJobName;

      // For display name, first add the table part, then remove everything before first dot
      jobNameForDisplay = enhancedJobName;
      if (jobNameForDisplay.contains(".")) {
        jobNameForDisplay = jobNameForDisplay.substring(jobNameForDisplay.indexOf(".") + 1);
      }
    } else if (job.getName().contains(".")) {
      // Normal case - use part after the dot for display only
      jobNameForDisplay = job.getName().substring(job.getName().indexOf(".") + 1);
    }

    return new JobNameResult(jobNameForDisplay, jobNameForUrn);
  }

  private static String extractTableNameFromMergeCommand(
      OpenLineage.Job job, OpenLineage.RunEvent event) {
    String tableName;

    // Method 1: Check for table name in the SQL facet (most reliable)
    tableName = extractTableNameFromSql(job);
    if (tableName != null) {
      return tableName;
    }

    // Method 2: Look for direct table names in the outputs
    tableName = extractTableNameFromOutputs(event);
    if (tableName != null) {
      return tableName;
    }

    // Method 3: Check for table identifiers in symlinks
    tableName = extractTableNameFromSymlinks(event);
    if (tableName != null) {
      return tableName;
    }

    // Method 4: Extract table name from warehouse paths (as a last resort)
    tableName = extractTableNameFromWarehousePaths(event);
    return tableName;
  }

  private static String extractTableNameFromSql(OpenLineage.Job job) {
    if (job.getFacets() != null && job.getFacets().getSql() != null) {
      String sqlQuery = job.getFacets().getSql().getQuery();
      if (sqlQuery != null && sqlQuery.toUpperCase().contains(MERGE_INTO_SQL_PATTERN)) {
        // Extract table name from the MERGE INTO SQL statement
        String[] lines = sqlQuery.split("\n");
        for (String line : lines) {
          line = line.trim();
          if (line.toUpperCase().startsWith(MERGE_INTO_SQL_PATTERN)) {
            // Format: MERGE INTO schema.table target
            String[] parts = line.split("\\s+");
            if (parts.length >= 3) {
              String tableName = parts[2].replace("`", "").trim();
              // If there's an alias (target/t/etc.), remove it
              int spaceIndex = tableName.indexOf(' ');
              if (spaceIndex > 0) {
                tableName = tableName.substring(0, spaceIndex);
              }
              log.info("Extracted table name from SQL: {}", tableName);
              return tableName;
            }
          }
        }
      }
    }
    return null;
  }

  private static String extractTableNameFromOutputs(OpenLineage.RunEvent event) {
    if (event.getOutputs() != null) {
      for (OpenLineage.OutputDataset output : event.getOutputs()) {
        // First check if the name itself is a table name (e.g., "delta_demo.customers")
        String name = output.getName();
        if (name != null && name.contains(".") && !name.startsWith("/")) {
          log.info("Using table name directly from output dataset name: {}", name);
          return name;
        }
      }
    }
    return null;
  }

  private static String extractTableNameFromSymlinks(OpenLineage.RunEvent event) {
    if (event.getOutputs() != null) {
      for (OpenLineage.OutputDataset output : event.getOutputs()) {
        if (output.getFacets() != null && output.getFacets().getSymlinks() != null) {
          for (OpenLineage.SymlinksDatasetFacetIdentifiers symlink :
              output.getFacets().getSymlinks().getIdentifiers()) {
            if ("TABLE".equals(symlink.getType())) {
              String name = symlink.getName();
              if (name != null) {
                // Handle table/name format
                if (name.startsWith(TABLE_PREFIX)) {
                  name = name.replaceFirst(TABLE_PREFIX, "").replace("/", ".");
                }
                log.info("Extracted table name from symlink: {}", name);
                return name;
              }
            }
          }
        }
      }
    }
    return null;
  }

  private static String extractTableNameFromWarehousePaths(OpenLineage.RunEvent event) {
    if (event.getOutputs() != null) {
      for (OpenLineage.OutputDataset output : event.getOutputs()) {
        String path = output.getName();
        if (path != null && path.contains(WAREHOUSE_PATH_PATTERN)) {
          // Extract table name from warehouse path pattern /warehouse/db.name/ or similar
          if (path.contains(DB_SUFFIX)) {
            int dbIndex = path.lastIndexOf(DB_SUFFIX);
            String tablePart = path.substring(dbIndex + 4);
            // Remove trailing slashes
            tablePart = tablePart.replaceAll("/+$", "");
            // Construct the full table name including db
            int warehouseIndex = path.lastIndexOf(WAREHOUSE_PATH_PATTERN);
            if (warehouseIndex >= 0) {
              String dbPart = path.substring(warehouseIndex + 11, dbIndex);
              String tableName = dbPart + "." + tablePart;
              log.info("Extracted table name from warehouse path: {}", tableName);
              return tableName;
            }
          }
        }
      }
    }
    return null;
  }

  private static boolean checkInputsEqualOutputs(
      OpenLineage.RunEvent event, OpenLineage.Job job, DatahubOpenlineageConfig datahubConf) {
    boolean inputsEqualOutputs = false;
    if ((datahubConf.isSpark())
        && ((event.getInputs() != null && event.getOutputs() != null)
            && (event.getInputs().size() == event.getOutputs().size()))) {
      inputsEqualOutputs =
          event.getInputs().stream()
              .map(OpenLineage.Dataset::getName)
              .collect(Collectors.toSet())
              .equals(
                  event.getOutputs().stream()
                      .map(OpenLineage.Dataset::getName)
                      .collect(Collectors.toSet()));
      if ((inputsEqualOutputs)
          && ("RDD_JOB".equals(job.getFacets().getJobType().getJobType()))
          && isNonMaterializingRddTransformation(job.getName())) {
        log.info(
            "Inputs equals Outputs: {}. This is most probably because of an rdd map operation and we only process Inputs",
            inputsEqualOutputs);
      }
    }
    return inputsEqualOutputs;
  }

  private static DataProcessInstanceProperties getJobDataProcessInstanceProperties(
      OpenLineage.RunEvent event) throws URISyntaxException {
    DataProcessInstanceProperties dpiProperties = new DataProcessInstanceProperties();
    dpiProperties.setName(event.getRun().getRunId().toString());
    AuditStamp auditStamp = new AuditStamp();
    if (event.getEventTime() != null) {
      auditStamp.setTime(event.getEventTime().toInstant().toEpochMilli());
    }
    auditStamp.setActor(Urn.createFromString(URN_LI_CORPUSER_DATAHUB));
    dpiProperties.setCreated(auditStamp);
    return dpiProperties;
  }

  public static Edge createEdge(Urn urn, ZonedDateTime eventTime) {
    Edge edge = new Edge();
    edge.setLastModified(createAuditStamp(eventTime));
    edge.setDestinationUrn(urn);
    return edge;
  }

  public static AuditStamp createAuditStamp(ZonedDateTime eventTime) {
    AuditStamp auditStamp = new AuditStamp();
    if (eventTime != null) {
      auditStamp.setTime(eventTime.toInstant().toEpochMilli());
    } else {
      auditStamp.setTime(System.currentTimeMillis());
    }
    try {
      auditStamp.setActor(Urn.createFromString(URN_LI_CORPUSER_DATAHUB));
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create actor urn:" + e);
    }
    return auditStamp;
  }

  private static void processParentJob(
      OpenLineage.RunEvent event,
      OpenLineage.Job job,
      String jobNameForUrn,
      DataJobInputOutput inputOutput,
      DatahubOpenlineageConfig datahubConf) {
    if ((event.getRun().getFacets() != null) && (event.getRun().getFacets().getParent() != null)) {
      OpenLineage.ParentRunFacetJob parentRunFacetJob =
          event.getRun().getFacets().getParent().getJob();
      DataJobUrn parentDataJobUrn =
          new DataJobUrn(
              getFlowUrn(
                  parentRunFacetJob.getNamespace(),
                  parentRunFacetJob.getName(),
                  null,
                  event.getRun().getFacets().getParent().get_producer(),
                  datahubConf),
              jobNameForUrn);

      Edge edge = createEdge(parentDataJobUrn, event.getEventTime());
      EdgeArray array = new EdgeArray();
      array.add(edge);
      inputOutput.setInputDatajobEdges(array);
    }
  }

  private static void processJobInputs(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {

    if (event.getInputs() == null) {
      return;
    }

    // Use LinkedHashSet to maintain order and remove duplicates more efficiently
    Set<OpenLineage.InputDataset> uniqueInputs = new LinkedHashSet<>(event.getInputs());

    for (OpenLineage.InputDataset input : uniqueInputs) {
      Optional<DatasetUrn> datasetUrn = convertOpenlineageDatasetToDatasetUrn(input, datahubConf);
      if (datasetUrn.isPresent()) {
        DatahubDataset.DatahubDatasetBuilder builder = DatahubDataset.builder();
        builder.urn(datasetUrn.get());
        if (datahubConf.isMaterializeDataset()) {
          builder.schemaMetadata(getSchemaMetadata(input, datahubConf));
        }
        if (datahubConf.isCaptureColumnLevelLineage()) {
          UpstreamLineage upstreamLineage = getFineGrainedLineage(input, datahubConf);
          if (upstreamLineage != null) {
            builder.lineage(upstreamLineage);
          }
        }
        datahubJob.getInSet().add(builder.build());
      }
    }
  }

  private static void processJobOutputs(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {

    if (event.getOutputs() == null) {
      return;
    }

    // Use LinkedHashSet to maintain order and remove duplicates more efficiently
    Set<OpenLineage.OutputDataset> uniqueOutputs = new LinkedHashSet<>(event.getOutputs());

    for (OpenLineage.OutputDataset output : uniqueOutputs) {
      Optional<DatasetUrn> datasetUrn = convertOpenlineageDatasetToDatasetUrn(output, datahubConf);
      if (datasetUrn.isPresent()) {
        DatahubDataset.DatahubDatasetBuilder builder = DatahubDataset.builder();
        builder.urn(datasetUrn.get());
        if (datahubConf.isMaterializeDataset()) {
          builder.schemaMetadata(getSchemaMetadata(output, datahubConf));
        }
        if (datahubConf.isCaptureColumnLevelLineage()) {
          UpstreamLineage upstreamLineage = getFineGrainedLineage(output, datahubConf);
          if (upstreamLineage != null) {
            builder.lineage(upstreamLineage);
          }
        }
        datahubJob.getOutSet().add(builder.build());
      }
    }
  }

  private static DataProcessInstanceRunEvent processDataProcessInstanceResult(
      OpenLineage.RunEvent event) {
    DataProcessInstanceRunEvent dpire = new DataProcessInstanceRunEvent();

    DataProcessInstanceRunResult result = new DataProcessInstanceRunResult();
    switch (event.getEventType()) {
      case COMPLETE:
        dpire.setStatus(DataProcessRunStatus.COMPLETE);
        result.setType(RunResultType.SUCCESS);
        result.setNativeResultType(event.getEventType().toString());
        if (event.getEventTime() != null) {
          dpire.setTimestampMillis(event.getEventTime().toInstant().toEpochMilli());
        }
        dpire.setResult(result);
        break;
      case FAIL:
      case ABORT:
        dpire.setStatus(DataProcessRunStatus.COMPLETE);
        result.setType(RunResultType.FAILURE);
        result.setNativeResultType(event.getEventType().toString());
        if (event.getEventTime() != null) {
          dpire.setTimestampMillis(event.getEventTime().toInstant().toEpochMilli());
        }
        dpire.setResult(result);
        break;
      case START:
      case RUNNING:
        dpire.setStatus(DataProcessRunStatus.STARTED);
        // result.setNativeResultType(event.getEventType().toString());
        if (event.getEventTime() != null) {
          dpire.setTimestampMillis(event.getEventTime().toInstant().toEpochMilli());
        }
        break;
      case OTHER:
      default:
        result.setNativeResultType(event.getEventType().toString());
        if (event.getEventTime() != null) {
          dpire.setTimestampMillis(event.getEventTime().toInstant().toEpochMilli());
        }
        result.setType(RunResultType.$UNKNOWN);
        dpire.setResult(result);
        break;
    }
    return dpire;
  }

  public static String getFlowName(String jobName, String flowName) {
    String[] nameSplit = jobName.split("\\.");
    if (flowName != null) {
      return flowName;
    } else {
      return nameSplit[0];
    }
  }

  public static DataFlowUrn getFlowUrn(
      String namespace,
      String jobName,
      String processingEngine,
      URI producer,
      DatahubOpenlineageConfig datahubOpenlineageConfig) {
    String producerName = null;
    if (producer != null) {
      producerName = producer.toString();
    }

    String orchestrator = getOrchestrator(processingEngine, producerName);
    String flowName = datahubOpenlineageConfig.getPipelineName();
    if (datahubOpenlineageConfig.getPlatformInstance() != null) {
      namespace = datahubOpenlineageConfig.getPlatformInstance();
    }
    return (new DataFlowUrn(orchestrator, getFlowName(jobName, flowName), namespace));
  }

  public static DataFlowInfo convertRunEventToDataFlowInfo(
      OpenLineage.RunEvent event, String flowName) {
    DataFlowInfo dataFlowInfo = new DataFlowInfo();
    dataFlowInfo.setName(getFlowName(event.getJob().getName(), flowName));
    return dataFlowInfo;
  }

  private static String getOrchestrator(String processingEngine, String producer) {
    String regex = "https://github.com/OpenLineage/OpenLineage/.*/(.*)$";
    Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
    String orchestrator = null;
    if (processingEngine != null) {
      orchestrator = processingEngine.toLowerCase();
    } else if (producer != null) {
      Matcher matcher = pattern.matcher(producer);
      if ((matcher.matches()) && ((matcher.groupCount() == 1))) {
        orchestrator = matcher.group(1);
      } else if (producer.startsWith("https://github.com/apache/airflow/")) {
        orchestrator = "airflow";
      }
    }
    if (orchestrator == null) {
      throw new RuntimeException("Unable to determine orchestrator");
    }
    return orchestrator;
  }

  public static SchemaFieldDataType.Type convertOlFieldTypeToDHFieldType(
      String openLineageFieldType) {
    switch (openLineageFieldType) {
      case "string":
        return SchemaFieldDataType.Type.create(new StringType());
      case "long":
      case "int":
        return SchemaFieldDataType.Type.create(new NumberType());
      case "timestamp":
        return SchemaFieldDataType.Type.create(new TimeType());
      case "struct":
        return SchemaFieldDataType.Type.create(new MapType());
      default:
        return SchemaFieldDataType.Type.create(new NullType());
    }
  }

  public static SchemaMetadata getSchemaMetadata(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    SchemaFieldArray schemaFieldArray = new SchemaFieldArray();
    if ((dataset.getFacets() == null) || (dataset.getFacets().getSchema() == null)) {
      return null;
    }
    dataset
        .getFacets()
        .getSchema()
        .getFields()
        .forEach(
            field -> {
              SchemaField schemaField = new SchemaField();
              schemaField.setFieldPath(field.getName());
              schemaField.setNativeDataType(field.getType());
              schemaField.setType(
                  new SchemaFieldDataType()
                      .setType(convertOlFieldTypeToDHFieldType(field.getType())));
              schemaFieldArray.add(schemaField);
            });
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setPlatformSchema(new SchemaMetadata.PlatformSchema());
    schemaMetadata.setSchemaName("");
    schemaMetadata.setVersion(1L);
    schemaMetadata.setHash("");

    MySqlDDL ddl = new MySqlDDL();
    ddl.setTableSchema(OpenLineageClientUtils.toJson(dataset.getFacets().getSchema().getFields()));
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setMySqlDDL(ddl);
    Optional<DatasetUrn> datasetUrn =
        getDatasetUrnFromOlDataset(dataset.getNamespace(), dataset.getName(), mappingConfig);

    if (!datasetUrn.isPresent()) {
      return null;
    }

    schemaMetadata.setPlatformSchema(platformSchema);

    schemaMetadata.setPlatform(datasetUrn.get().getPlatformEntity());

    schemaMetadata.setFields(schemaFieldArray);
    return schemaMetadata;
  }
}
