package io.datahubproject.openlineage.converter;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.DataTransform;
import com.linkedin.common.DataTransformArray;
import com.linkedin.common.DataTransformLogic;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.VersionInfo;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryStatement;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetContract;
import io.datahubproject.openlineage.customfacet.CustomRunFacetContributions;
import io.datahubproject.openlineage.customfacet.CustomRunFacetProcessor;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class OpenLineageJobMapper {

  static final String URN_LI_DATA_PROCESS_INSTANCE = "urn:li:dataProcessInstance:";
  static final String PROCESSING_ENGINE_KEY = "processingEngine";
  static final String PROCESSING_ENGINE_VERSION_KEY = "processingEngineVersion";
  static final String OPENLINEAGE_ADAPTER_VERSION_KEY = "openlineageAdapterVersion";
  static final String JOB_ID_KEY = "jobId";
  static final String JOB_DESCRIPTION_KEY = "jobDescription";
  static final String JOB_GROUP_KEY = "jobGroup";
  static final String JOB_CALL_SITE_KEY = "jobCallSite";
  static final String SPARK_VERSION_KEY = "spark-version";
  static final String OPENLINEAGE_SPARK_VERSION_KEY = "openlineage-spark-version";
  static final String SPARK_LOGICAL_PLAN_KEY = "spark.logicalPlan";
  static final String MERGE_INTO_COMMAND_PATTERN = "execute_merge_into_command_edge";
  static final String MERGE_INTO_SQL_PATTERN = "MERGE INTO";
  static final String TABLE_PREFIX = "table/";
  static final String WAREHOUSE_PATH_PATTERN = "/warehouse/";
  static final String DB_SUFFIX = ".db/";
  private static final CustomRunFacetProcessor CUSTOM_RUN_FACET_PROCESSOR =
      new CustomRunFacetProcessor();
  private static final Set<String> COMPATIBILITY_RUN_FACET_KEYS =
      CompatibilityFacetCatalog.contracts().stream()
          .map(CompatibilityFacetContract::key)
          .collect(Collectors.toUnmodifiableSet());

  private OpenLineageJobMapper() {}

  static DatahubJob convertJobEventToJob(
      OpenLineage.JobEvent event, DatahubOpenlineageConfig datahubConf)
      throws IOException, URISyntaxException {
    URI producer =
        event.getProducer() != null ? event.getProducer() : URI.create("https://openlineage.io");
    OpenLineage openLineage = new OpenLineage(producer);
    UUID syntheticRunId =
        UUID.nameUUIDFromBytes(
            String.format(
                    "%s:%s:%s",
                    event.getJob().getNamespace(), event.getJob().getName(), event.getEventTime())
                .getBytes(StandardCharsets.UTF_8));
    OpenLineage.RunEvent syntheticRunEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(event.getEventTime())
            .eventType(OpenLineage.RunEvent.EventType.OTHER)
            .run(openLineage.newRunBuilder().runId(syntheticRunId).build())
            .job(event.getJob())
            .inputs(event.getInputs())
            .outputs(event.getOutputs())
            .build();

    return convertRunEventToJob(syntheticRunEvent, datahubConf, false);
  }

  static DatahubJob convertRunEventToJob(
      OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws IOException, URISyntaxException {
    return convertRunEventToJob(event, datahubConf, true);
  }

  private static DatahubJob convertRunEventToJob(
      OpenLineage.RunEvent event,
      DatahubOpenlineageConfig datahubConf,
      boolean emitDataProcessInstance)
      throws IOException, URISyntaxException {
    DatahubJob.DatahubJobBuilder jobBuilder =
        DatahubJob.builder().emitDataProcessInstance(emitDataProcessInstance);

    if (event.getEventTime() != null) {
      jobBuilder.eventTime(event.getEventTime().toInstant().toEpochMilli());
    }

    log.debug("Mapping OpenLineage RunEvent for job namespace '{}'", event.getJob().getNamespace());
    logUnknownFacets(event);
    CustomRunFacetContributions customFacetContributions =
        CUSTOM_RUN_FACET_PROCESSOR.process(event.getRun().getFacets());
    DataFlowInfo dfi =
        OpenLineagePlatformResolver.convertRunEventToDataFlowInfo(
            event, datahubConf.getPipelineName());

    OpenLineage.ProcessingEngineRunFacet processingEngineFacet = getProcessingEngineFacet(event);
    String processingEngine =
        processingEngineFacet != null ? processingEngineFacet.getName() : null;

    DataFlowUrn dataFlowUrn =
        OpenLineagePlatformResolver.getFlowUrn(
            event.getJob().getNamespace(),
            event.getJob().getName(),
            processingEngine,
            getJobTypeIntegration(event.getJob().getFacets()),
            event.getProducer(),
            datahubConf);
    jobBuilder.flowUrn(dataFlowUrn);

    if (datahubConf.getPlatformInstance() != null) {
      DataPlatformInstance dpi =
          new DataPlatformInstance()
              .setPlatform(new DataPlatformUrn(dataFlowUrn.getOrchestratorEntity()))
              .setInstance(
                  OpenLineageMappingUtils.dataPlatformInstanceUrn(
                      dataFlowUrn.getOrchestratorEntity(), datahubConf.getPlatformInstance()));
      jobBuilder.flowPlatformInstance(dpi);
      // Stamp the same instance on the DataJob so the job entity carries its own
      // DataPlatformInstance aspect, not just the instance embedded in the parent DataFlow URN.
      jobBuilder.jobPlatformInstance(dpi);
    }

    VersionInfo flowVersionInfo = getProcessingEngineVersionInfo(processingEngineFacet);
    if (flowVersionInfo != null) {
      jobBuilder.flowVersionInfo(flowVersionInfo);
    }

    StringMap customProperties =
        generateCustomProperties(event, customFacetContributions.flowProperties());
    dfi.setCustomProperties(customProperties);

    jobBuilder.dataFlowInfo(dfi);

    jobBuilder.jobOwnership(generateOwnership(event.getJob().getFacets()));

    GlobalTags jobTags =
        mergeGlobalTags(
            mergeGlobalTags(
                generateJobTags(event.getJob().getFacets()),
                generateRunTags(event.getRun().getFacets())),
            customFacetContributions.jobTags());
    jobBuilder.jobGlobalTags(jobTags);
    jobBuilder.flowGlobalTags(customFacetContributions.flowTags());

    DataTransformLogic dataTransformLogic = generateDataTransformLogic(event.getJob().getFacets());
    jobBuilder.dataTransformLogic(dataTransformLogic);

    DatahubJob datahubJob = jobBuilder.build();
    convertJobToDataJob(datahubJob, event, datahubConf, customFacetContributions);
    return datahubJob;
  }

  static Ownership generateOwnership(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null || jobFacets.getOwnership() == null) {
      return null;
    }
    return OpenLineageMappingUtils.generateOwnership(
        jobFacets.getOwnership().getOwners(),
        OpenLineage.OwnershipJobFacetOwners::getName,
        OpenLineage.OwnershipJobFacetOwners::getType);
  }

  static GlobalTags generateJobTags(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null || jobFacets.getTags() == null) {
      return null;
    }
    return OpenLineageMappingUtils.generateFacetTags(
        jobFacets.getTags().getTags(),
        OpenLineage.TagsJobFacetFields::getKey,
        OpenLineage.TagsJobFacetFields::getValue);
  }

  static GlobalTags generateRunTags(OpenLineage.RunFacets runFacets) {
    if (runFacets == null || runFacets.getTags() == null) {
      return null;
    }
    return OpenLineageMappingUtils.generateFacetTags(
        runFacets.getTags().getTags(),
        OpenLineage.TagsRunFacetFields::getKey,
        OpenLineage.TagsRunFacetFields::getValue);
  }

  static GlobalTags mergeGlobalTags(GlobalTags first, GlobalTags second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    TagAssociationArray mergedTags = new TagAssociationArray();
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    Stream.of(first, second)
        .filter(tags -> tags.getTags() != null)
        .flatMap(tags -> tags.getTags().stream())
        .forEach(
            tag -> {
              if (seen.add(tag.getTag().toString())) {
                mergedTags.add(tag);
              }
            });
    return mergedTags.isEmpty() ? null : new GlobalTags().setTags(mergedTags);
  }

  static DataTransformLogic generateDataTransformLogic(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null) {
      return null;
    }

    DataTransformArray transforms = new DataTransformArray();
    if (jobFacets.getSql() != null && jobFacets.getSql().getQuery() != null) {
      transforms.add(getDataTransform(jobFacets.getSql().getQuery(), QueryLanguage.SQL));
    }
    if (jobFacets.getSourceCode() != null && jobFacets.getSourceCode().getSourceCode() != null) {
      transforms.add(
          getDataTransform(jobFacets.getSourceCode().getSourceCode(), QueryLanguage.UNKNOWN));
    }
    if (transforms.isEmpty()) {
      return null;
    }

    DataTransformLogic transformLogic = new DataTransformLogic();
    transformLogic.setTransforms(transforms);
    return transformLogic;
  }

  static DataTransform getDataTransform(String value, QueryLanguage language) {
    QueryStatement queryStatement = new QueryStatement();
    queryStatement.setValue(value);
    queryStatement.setLanguage(language);

    DataTransform transform = new DataTransform();
    transform.setQueryStatement(queryStatement);
    return transform;
  }

  static String getDescription(OpenLineage.RunEvent event) {
    if (event.getJob().getFacets() != null
        && event.getJob().getFacets().getDocumentation() != null) {
      return event.getJob().getFacets().getDocumentation().getDescription();
    }
    return null;
  }

  static String getSourceCodeLocationUrl(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null || jobFacets.getSourceCodeLocation() == null) {
      return null;
    }
    if (jobFacets.getSourceCodeLocation().getUrl() != null) {
      return jobFacets.getSourceCodeLocation().getUrl().toString();
    }
    if (jobFacets.getSourceCodeLocation().getRepoUrl() != null) {
      return jobFacets.getSourceCodeLocation().getRepoUrl();
    }
    return null;
  }

  static OpenLineage.ProcessingEngineRunFacet getProcessingEngineFacet(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() == null) {
      return null;
    }
    return event.getRun().getFacets().getProcessing_engine();
  }

  static VersionInfo getProcessingEngineVersionInfo(
      OpenLineage.ProcessingEngineRunFacet processingEngineFacet) {
    if (processingEngineFacet == null
        || processingEngineFacet.getVersion() == null
        || processingEngineFacet.getVersion().isBlank()) {
      return null;
    }
    return new VersionInfo()
        .setVersion(processingEngineFacet.getVersion())
        .setVersionType("processing_engine");
  }

  static String getJobTypeIntegration(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null || jobFacets.getJobType() == null) {
      return null;
    }
    return jobFacets.getJobType().getIntegration();
  }

  static String getJobProcessingType(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null || jobFacets.getJobType() == null) {
      return null;
    }
    return jobFacets.getJobType().getProcessingType();
  }

  static SubTypes getDataJobSubTypes(OpenLineage.JobFacets jobFacets) {
    if (jobFacets == null
        || jobFacets.getJobType() == null
        || jobFacets.getJobType().getJobType() == null
        || jobFacets.getJobType().getJobType().isBlank()) {
      return null;
    }
    return new SubTypes()
        .setTypeNames(new StringArray(List.of(jobFacets.getJobType().getJobType().trim())));
  }

  static StringMap generateCustomProperties(
      OpenLineage.RunEvent event, StringMap compatibilityProperties) {
    StringMap customProperties = new StringMap();
    customProperties.putAll(compatibilityProperties);
    OpenLineage.RunFacets facets = event.getRun().getFacets();
    if (facets == null || facets.getProcessing_engine() == null) {
      return customProperties;
    }

    OpenLineage.ProcessingEngineRunFacet processingEngine = facets.getProcessing_engine();
    if (processingEngine.getName() != null) {
      customProperties.put(PROCESSING_ENGINE_KEY, processingEngine.getName());
    }
    if (processingEngine.getVersion() != null) {
      customProperties.put(PROCESSING_ENGINE_VERSION_KEY, processingEngine.getVersion());
    }
    if (processingEngine.getOpenlineageAdapterVersion() != null) {
      customProperties.put(
          OPENLINEAGE_ADAPTER_VERSION_KEY, processingEngine.getOpenlineageAdapterVersion());
    }
    return customProperties;
  }

  static void logUnknownFacets(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() != null
        && event.getRun().getFacets().getAdditionalProperties() != null) {
      Set<String> unknownRunFacets =
          event.getRun().getFacets().getAdditionalProperties().keySet().stream()
              .filter(name -> !COMPATIBILITY_RUN_FACET_KEYS.contains(name))
              .filter(name -> !"processing_engine".equals(name))
              .collect(Collectors.toUnmodifiableSet());
      OpenLineageMappingUtils.logFacetNames("RunEvent", "run", unknownRunFacets);
    }
    if (event.getJob().getFacets() != null) {
      OpenLineageMappingUtils.logFacetNames(
          "RunEvent", "job", event.getJob().getFacets().getAdditionalProperties().keySet());
    }
    if (event.getInputs() != null) {
      for (OpenLineage.InputDataset input : event.getInputs()) {
        if (input.getFacets() != null) {
          OpenLineageMappingUtils.logFacetNames(
              "RunEvent", "input.dataset", input.getFacets().getAdditionalProperties().keySet());
        }
        if (input.getInputFacets() != null) {
          OpenLineageMappingUtils.logFacetNames(
              "RunEvent", "input", input.getInputFacets().getAdditionalProperties().keySet());
        }
      }
    }
    if (event.getOutputs() != null) {
      for (OpenLineage.OutputDataset output : event.getOutputs()) {
        if (output.getFacets() != null) {
          OpenLineageMappingUtils.logFacetNames(
              "RunEvent", "output.dataset", output.getFacets().getAdditionalProperties().keySet());
        }
        if (output.getOutputFacets() != null) {
          OpenLineageMappingUtils.logFacetNames(
              "RunEvent", "output", output.getOutputFacets().getAdditionalProperties().keySet());
        }
      }
    }
  }

  static boolean isNonMaterializingRddTransformation(String jobName) {
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

  static void convertJobToDataJob(
      DatahubJob datahubJob,
      OpenLineage.RunEvent event,
      DatahubOpenlineageConfig datahubConf,
      CustomRunFacetContributions customFacetContributions)
      throws URISyntaxException {

    OpenLineage.Job job = event.getJob();
    DataJobInfo dataJobInfo = new DataJobInfo();

    log.debug("Datahub Config: {}", datahubConf);

    // Extract job names using helper method
    JobNameResult jobNames = extractJobNames(job, event, datahubConf);

    // Set the display name
    dataJobInfo.setName(jobNames.displayName);

    OpenLineage.ProcessingEngineRunFacet processingEngineFacet = getProcessingEngineFacet(event);
    String jobProcessingEngine =
        processingEngineFacet != null ? processingEngineFacet.getName() : null;

    DataFlowUrn flowUrn =
        OpenLineagePlatformResolver.getFlowUrn(
            event.getJob().getNamespace(),
            jobNames.flowName,
            jobProcessingEngine,
            getJobTypeIntegration(job.getFacets()),
            event.getProducer(),
            datahubConf);

    dataJobInfo.setFlowUrn(flowUrn);
    String jobProcessingType = getJobProcessingType(job.getFacets());
    dataJobInfo.setType(
        DataJobInfo.Type.create(
            jobProcessingType != null && !jobProcessingType.isBlank()
                ? jobProcessingType.trim()
                : flowUrn.getOrchestratorEntity()));

    DataJobUrn dataJobUrn = new DataJobUrn(flowUrn, jobNames.taskName);
    datahubJob.setJobUrn(dataJobUrn);

    StringMap customProperties =
        generateCustomProperties(event, customFacetContributions.jobProperties());
    if (job.getFacets() != null
        && job.getFacets().getSourceCode() != null
        && job.getFacets().getSourceCode().getLanguage() != null) {
      customProperties.put(
          "openlineage.sourceCodeLanguage", job.getFacets().getSourceCode().getLanguage());
    }
    dataJobInfo.setCustomProperties(customProperties);

    TimeStamp timestamp = new TimeStamp();

    if (event.getEventTime() != null) {
      dataJobInfo.setCreated(timestamp.setTime(event.getEventTime().toInstant().toEpochMilli()));
    }

    String description = getDescription(event);
    if (description != null) {
      dataJobInfo.setDescription(description);
    }

    String sourceCodeUrl = getSourceCodeLocationUrl(job.getFacets());
    if (sourceCodeUrl != null) {
      dataJobInfo.setExternalUrl(new Url(sourceCodeUrl));
    }
    datahubJob.setJobInfo(dataJobInfo);
    datahubJob.setJobSubTypes(getDataJobSubTypes(job.getFacets()));

    // Process inputs and outputs
    boolean inputsEqualOutputs = checkInputsEqualOutputs(event, job, datahubConf);

    processJobInputs(datahubJob, event, datahubConf);

    if (!inputsEqualOutputs) {
      OpenLineageDatasetMapper.processJobOutputs(datahubJob, event, datahubConf);
    }

    // Set run event and instance properties
    DataProcessInstanceRunEvent dataProcessInstanceRunEvent =
        OpenLineageRunMapper.processDataProcessInstanceResult(event);
    datahubJob.setDataProcessInstanceRunEvent(dataProcessInstanceRunEvent);

    DataProcessInstanceProperties dpiProperties =
        OpenLineageRunMapper.getJobDataProcessInstanceProperties(event);
    datahubJob.setDataProcessInstanceProperties(dpiProperties);

    // Create input/output edges and relationships
    OpenLineageRunMapper.processParentJob(datahubJob, event, datahubConf);
    OpenLineageRunMapper.processJobDependencies(datahubJob, event, datahubConf);

    DataProcessInstanceRelationships dataProcessInstanceRelationships =
        new DataProcessInstanceRelationships();
    dataProcessInstanceRelationships.setParentTemplate(dataJobUrn);
    OpenLineageRunMapper.getParentDataProcessInstanceUrn(event)
        .ifPresent(dataProcessInstanceRelationships::setParentInstance);
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

  static class JobNameResult {
    final String flowName;
    final String taskName;
    final String displayName;

    JobNameResult(String flowName, String taskName, String displayName) {
      this.flowName = flowName;
      this.taskName = taskName;
      this.displayName = displayName;
    }
  }

  static JobNameResult extractJobNames(
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
      String tablePart = tableName.replace(".", "_").replace(" ", "_").toLowerCase(Locale.ROOT);
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

    return new JobNameResult(jobNameForUrn, jobNameForUrn, jobNameForDisplay);
  }

  static String extractTableNameFromMergeCommand(OpenLineage.Job job, OpenLineage.RunEvent event) {
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

  static String extractTableNameFromSql(OpenLineage.Job job) {
    if (job.getFacets() != null && job.getFacets().getSql() != null) {
      String sqlQuery = job.getFacets().getSql().getQuery();
      if (sqlQuery != null && sqlQuery.toUpperCase(Locale.ROOT).contains(MERGE_INTO_SQL_PATTERN)) {
        // Extract table name from the MERGE INTO SQL statement
        String[] lines = sqlQuery.split("\n");
        for (String line : lines) {
          line = line.trim();
          if (line.toUpperCase(Locale.ROOT).startsWith(MERGE_INTO_SQL_PATTERN)) {
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

  static String extractTableNameFromOutputs(OpenLineage.RunEvent event) {
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

  static String extractTableNameFromSymlinks(OpenLineage.RunEvent event) {
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

  static String extractTableNameFromWarehousePaths(OpenLineage.RunEvent event) {
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

  private static void processJobInputs(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws URISyntaxException {
    if (event.getInputs() == null) {
      return;
    }
    for (OpenLineage.InputDataset input : event.getInputs()) {
      OpenLineageDatasetMapper.InputContribution contribution =
          OpenLineageDatasetMapper.mapInput(datahubJob, event, input, datahubConf);
      if (contribution == null) {
        continue;
      }
      OpenLineageRunMapper.processDataQualityAssertions(
          datahubJob, event, contribution.input(), contribution.datasetUrn());
      OpenLineageDatasetMapper.applyInput(datahubJob, contribution);
    }
  }

  static boolean checkInputsEqualOutputs(
      OpenLineage.RunEvent event, OpenLineage.Job job, DatahubOpenlineageConfig datahubConf) {
    if (!datahubConf.isSpark()) {
      return false;
    }

    if (job.getFacets() == null
        || job.getFacets().getJobType() == null
        || !"RDD_JOB".equals(job.getFacets().getJobType().getJobType())) {
      return false;
    }

    if (!isNonMaterializingRddTransformation(job.getName())) {
      return false;
    }

    if (event.getInputs() == null
        || event.getOutputs() == null
        || event.getInputs().size() != event.getOutputs().size()) {
      return false;
    }

    boolean inputsEqualOutputs =
        event.getInputs().stream()
            .map(OpenLineage.Dataset::getName)
            .collect(Collectors.toSet())
            .equals(
                event.getOutputs().stream()
                    .map(OpenLineage.Dataset::getName)
                    .collect(Collectors.toSet()));

    if (inputsEqualOutputs) {
      log.info(
          "Inputs equals Outputs: {}. This is most probably because of an rdd map operation and we only process Inputs",
          inputsEqualOutputs);
    }

    return inputsEqualOutputs;
  }
}
