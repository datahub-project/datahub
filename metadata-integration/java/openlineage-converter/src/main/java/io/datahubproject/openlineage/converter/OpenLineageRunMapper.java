package io.datahubproject.openlineage.converter;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataprocess.DataProcessInstanceRunResult;
import com.linkedin.dataprocess.DataProcessRunStatus;
import com.linkedin.dataprocess.RunResultType;
import com.linkedin.metadata.key.AssertionKey;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

@Slf4j
final class OpenLineageRunMapper {

  static final String URN_LI_CORPUSER = "urn:li:corpuser:";
  static final String URN_LI_CORPUSER_DATAHUB = URN_LI_CORPUSER + "datahub";
  static final String URN_LI_DATA_PROCESS_INSTANCE = "urn:li:dataProcessInstance:";
  static final String URN_LI_ASSERTION = "urn:li:assertion:";
  static final String ASSERTION_ENTITY_TYPE = "assertion";
  private static final Set<String> SENSITIVE_ENV_NAME_PARTS =
      Set.of("SECRET", "TOKEN", "PASSWORD", "PASSWD", "PWD", "KEY", "CREDENTIAL", "AUTH");

  private OpenLineageRunMapper() {}

  static DataProcessInstanceProperties getJobDataProcessInstanceProperties(
      OpenLineage.RunEvent event) throws URISyntaxException {
    DataProcessInstanceProperties dpiProperties = new DataProcessInstanceProperties();
    dpiProperties.setName(event.getRun().getRunId().toString());
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(getDataProcessInstanceCreatedTime(event));
    auditStamp.setActor(Urn.createFromString(URN_LI_CORPUSER_DATAHUB));
    dpiProperties.setCreated(auditStamp);

    StringMap customProperties = getDataProcessInstanceCustomProperties(event);
    if (!customProperties.isEmpty()) {
      dpiProperties.setCustomProperties(customProperties);
    }
    return dpiProperties;
  }

  static long getDataProcessInstanceCreatedTime(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() != null
        && event.getRun().getFacets().getNominalTime() != null
        && event.getRun().getFacets().getNominalTime().getNominalStartTime() != null) {
      return event
          .getRun()
          .getFacets()
          .getNominalTime()
          .getNominalStartTime()
          .toInstant()
          .toEpochMilli();
    }
    if (event.getEventTime() != null) {
      return event.getEventTime().toInstant().toEpochMilli();
    }
    return System.currentTimeMillis();
  }

  static StringMap getDataProcessInstanceCustomProperties(OpenLineage.RunEvent event) {
    StringMap customProperties = new StringMap();
    if (event.getRun().getFacets() == null) {
      return customProperties;
    }

    OpenLineage.NominalTimeRunFacet nominalTime = event.getRun().getFacets().getNominalTime();
    if (nominalTime != null) {
      if (nominalTime.getNominalStartTime() != null) {
        customProperties.put("nominalStartTime", nominalTime.getNominalStartTime().toString());
      }
      if (nominalTime.getNominalEndTime() != null) {
        customProperties.put("nominalEndTime", nominalTime.getNominalEndTime().toString());
      }
    }

    OpenLineage.ErrorMessageRunFacet errorMessage = event.getRun().getFacets().getErrorMessage();
    if (errorMessage != null) {
      if (errorMessage.getMessage() != null) {
        customProperties.put("errorMessage", errorMessage.getMessage());
      }
      if (errorMessage.getProgrammingLanguage() != null) {
        customProperties.put("programmingLanguage", errorMessage.getProgrammingLanguage());
      }
      if (errorMessage.getStackTrace() != null) {
        customProperties.put("stackTrace", errorMessage.getStackTrace());
      }
    }

    OpenLineage.EnvironmentVariablesRunFacet environmentVariables =
        event.getRun().getFacets().getEnvironmentVariables();
    if (environmentVariables != null && environmentVariables.getEnvironmentVariables() != null) {
      for (OpenLineage.EnvironmentVariable environmentVariable :
          environmentVariables.getEnvironmentVariables()) {
        if (environmentVariable.getName() != null && environmentVariable.getValue() != null) {
          if (isSensitiveEnvironmentVariable(environmentVariable.getName())) {
            customProperties.put("env." + environmentVariable.getName(), "[REDACTED]");
          } else {
            customProperties.put(
                "env." + environmentVariable.getName(), environmentVariable.getValue());
          }
        }
      }
    }

    OpenLineage.ExtractionErrorRunFacet extractionError =
        event.getRun().getFacets().getExtractionError();
    if (extractionError != null) {
      if (extractionError.getTotalTasks() != null) {
        customProperties.put(
            "extractionError.totalTasks", extractionError.getTotalTasks().toString());
      }
      if (extractionError.getFailedTasks() != null) {
        customProperties.put(
            "extractionError.failedTasks", extractionError.getFailedTasks().toString());
      }
      if (extractionError.getErrors() != null && !extractionError.getErrors().isEmpty()) {
        customProperties.put("extractionError.errors", extractionErrorsToJson(extractionError));
      }
    }
    return customProperties;
  }

  static boolean isSensitiveEnvironmentVariable(String name) {
    String upperName = name.toUpperCase(Locale.ROOT);
    return SENSITIVE_ENV_NAME_PARTS.stream().anyMatch(upperName::contains);
  }

  static String extractionErrorsToJson(OpenLineage.ExtractionErrorRunFacet extractionError) {
    JSONArray errors = new JSONArray();
    for (OpenLineage.ExtractionErrorRunFacetErrors error : extractionError.getErrors()) {
      JSONObject errorJson = new JSONObject();
      if (error.getErrorMessage() != null) {
        errorJson.put("errorMessage", error.getErrorMessage());
      }
      if (error.getStackTrace() != null) {
        errorJson.put("stackTrace", error.getStackTrace());
      }
      if (error.getTask() != null) {
        errorJson.put("task", error.getTask());
      }
      if (error.getTaskNumber() != null) {
        errorJson.put("taskNumber", error.getTaskNumber());
      }
      errors.put(errorJson);
    }
    return errors.toString();
  }

  static Optional<Urn> getParentDataProcessInstanceUrn(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() == null
        || event.getRun().getFacets().getParent() == null
        || event.getRun().getFacets().getParent().getRun() == null
        || event.getRun().getFacets().getParent().getRun().getRunId() == null) {
      return Optional.empty();
    }
    return Optional.of(
        UrnUtils.getUrn(
            URN_LI_DATA_PROCESS_INSTANCE
                + event.getRun().getFacets().getParent().getRun().getRunId()));
  }

  static void processParentJob(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {
    if ((event.getRun().getFacets() != null) && (event.getRun().getFacets().getParent() != null)) {
      OpenLineage.ParentRunFacetJob parentRunFacetJob =
          event.getRun().getFacets().getParent().getJob();
      if (parentRunFacetJob == null) {
        return;
      }
      String parentJobName = parentRunFacetJob.getName();
      String parentNamespace =
          datahubConf.getPlatformInstance() != null
              ? datahubConf.getPlatformInstance()
              : parentRunFacetJob.getNamespace();
      DataFlowUrn parentFlowUrn =
          new DataFlowUrn(
              datahubJob.getFlowUrn().getOrchestratorEntity(),
              OpenLineagePlatformResolver.getFlowName(parentJobName, datahubConf.getPipelineName()),
              parentNamespace);
      DataJobUrn parentDataJobUrn = new DataJobUrn(parentFlowUrn, parentJobName);
      datahubJob.getParentJobs().add(parentDataJobUrn);
    }
  }

  static void processJobDependencies(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf) {
    if (event.getRun().getFacets() == null
        || event.getRun().getFacets().getJobDependencies() == null) {
      return;
    }
    OpenLineage.JobDependenciesRunFacet facet = event.getRun().getFacets().getJobDependencies();
    String orchestrator = datahubJob.getFlowUrn().getOrchestratorEntity();
    StringMap customProperties = datahubJob.getJobInfo().getCustomProperties();
    if (facet.getTrigger_rule() != null && !facet.getTrigger_rule().isBlank()) {
      customProperties.put("openlineage.jobDependencies.triggerRule", facet.getTrigger_rule());
    }
    if (facet.getUpstream() != null) {
      for (OpenLineage.JobDependency dependency : facet.getUpstream()) {
        Optional<DataJobUrn> dependencyUrn =
            getDependencyJobUrn(dependency, orchestrator, datahubConf);
        if (dependencyUrn.isPresent()) {
          datahubJob.getParentJobs().add(dependencyUrn.get());
          StringMap edgeProperties = getDependencyEdgeProperties(dependency);
          if (!edgeProperties.isEmpty()) {
            datahubJob.getParentJobProperties().put(dependencyUrn.get(), edgeProperties);
          }
        }
      }
    }
    if (facet.getDownstream() != null) {
      int index = 0;
      for (OpenLineage.JobDependency dependency : facet.getDownstream()) {
        Optional<DataJobUrn> dependencyUrn =
            getDependencyJobUrn(dependency, orchestrator, datahubConf);
        if (dependencyUrn.isPresent()) {
          storeDependencyMetadata(
              customProperties,
              "openlineage.jobDependencies.downstream." + index,
              dependency,
              dependencyUrn.get());
          index++;
        }
      }
      if (!facet.getDownstream().isEmpty()) {
        log.debug(
            "Preserving OpenLineage downstream job dependencies as DataJob custom properties; no reverse lineage edges are emitted");
      }
    }
  }

  static Optional<DataJobUrn> getDependencyJobUrn(
      OpenLineage.JobDependency dependency,
      String orchestrator,
      DatahubOpenlineageConfig datahubConf) {
    if (dependency == null
        || dependency.getJob() == null
        || dependency.getJob().getNamespace() == null
        || dependency.getJob().getName() == null
        || dependency.getJob().getNamespace().isBlank()
        || dependency.getJob().getName().isBlank()) {
      return Optional.empty();
    }
    String jobName = dependency.getJob().getName();
    String namespace =
        datahubConf.getPlatformInstance() != null
            ? datahubConf.getPlatformInstance()
            : dependency.getJob().getNamespace();
    DataFlowUrn flowUrn =
        new DataFlowUrn(
            orchestrator,
            OpenLineagePlatformResolver.getFlowName(jobName, datahubConf.getPipelineName()),
            namespace);
    return Optional.of(new DataJobUrn(flowUrn, jobName));
  }

  static StringMap getDependencyEdgeProperties(OpenLineage.JobDependency dependency) {
    StringMap properties = new StringMap();
    OpenLineageMappingUtils.putIfPresent(
        properties, "dependencyType", dependency.getDependency_type());
    OpenLineageMappingUtils.putIfPresent(
        properties, "sequenceTriggerRule", dependency.getSequence_trigger_rule());
    OpenLineageMappingUtils.putIfPresent(
        properties, "statusTriggerRule", dependency.getStatus_trigger_rule());
    if (dependency.getRun() != null && dependency.getRun().getRunId() != null) {
      properties.put("runId", dependency.getRun().getRunId().toString());
    }
    return properties;
  }

  static void storeDependencyMetadata(
      StringMap customProperties,
      String prefix,
      OpenLineage.JobDependency dependency,
      DataJobUrn dependencyUrn) {
    customProperties.put(prefix + ".job", dependencyUrn.toString());
    StringMap dependencyProperties = getDependencyEdgeProperties(dependency);
    dependencyProperties.forEach((key, value) -> customProperties.put(prefix + "." + key, value));
  }

  static void processDataQualityAssertions(
      DatahubJob datahubJob,
      OpenLineage.RunEvent event,
      OpenLineage.InputDataset input,
      DatasetUrn datasetUrn) {
    if (input.getInputFacets() == null
        || input.getInputFacets().getDataQualityAssertions() == null
        || input.getInputFacets().getDataQualityAssertions().getAssertions() == null) {
      return;
    }

    for (OpenLineage.DataQualityAssertionsDatasetFacetAssertions assertion :
        input.getInputFacets().getDataQualityAssertions().getAssertions()) {
      if (assertion.getAssertion() == null || assertion.getAssertion().isBlank()) {
        continue;
      }
      String assertionId =
          UUID.nameUUIDFromBytes(
                  String.format(
                          "%s:%s:%s",
                          datasetUrn,
                          assertion.getColumn() != null ? assertion.getColumn() : "",
                          assertion.getAssertion())
                      .getBytes(StandardCharsets.UTF_8))
              .toString();
      Urn assertionUrn = UrnUtils.getUrn(URN_LI_ASSERTION + assertionId);

      AssertionKey assertionKey = new AssertionKey().setAssertionId(assertionId);
      datahubJob
          .getExtraMcps()
          .add(OpenLineageMappingUtils.toMcp(assertionUrn, ASSERTION_ENTITY_TYPE, assertionKey));

      CustomAssertionInfo customAssertionInfo =
          new CustomAssertionInfo()
              .setType("OpenLineage Data Quality Assertion")
              .setEntity(datasetUrn)
              .setLogic(assertion.getAssertion());
      if (assertion.getColumn() != null && !assertion.getColumn().isBlank()) {
        customAssertionInfo.setField(schemaFieldUrn(datasetUrn, assertion.getColumn()));
      }

      AssertionInfo assertionInfo =
          new AssertionInfo()
              .setType(AssertionType.CUSTOM)
              .setDescription(assertion.getAssertion())
              .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL))
              .setCustomAssertion(customAssertionInfo);
      datahubJob
          .getExtraMcps()
          .add(OpenLineageMappingUtils.toMcp(assertionUrn, ASSERTION_ENTITY_TYPE, assertionInfo));

      DataPlatformInstance assertionPlatformInstance =
          new DataPlatformInstance()
              .setPlatform(new DataPlatformUrn(datahubJob.getFlowUrn().getOrchestratorEntity()));
      datahubJob
          .getExtraMcps()
          .add(
              OpenLineageMappingUtils.toMcp(
                  assertionUrn, ASSERTION_ENTITY_TYPE, assertionPlatformInstance));

      if (assertion.getSuccess() != null && datahubJob.isEmitDataProcessInstance()) {
        AssertionResult assertionResult =
            new AssertionResult()
                .setType(
                    assertion.getSuccess()
                        ? AssertionResultType.SUCCESS
                        : AssertionResultType.FAILURE);
        AssertionRunEvent assertionRunEvent =
            new AssertionRunEvent()
                .setTimestampMillis(
                    event.getEventTime() != null
                        ? event.getEventTime().toInstant().toEpochMilli()
                        : System.currentTimeMillis())
                .setRunId(event.getRun().getRunId().toString())
                .setAsserteeUrn(datasetUrn)
                .setStatus(AssertionRunStatus.COMPLETE)
                .setResult(assertionResult)
                .setAssertionUrn(assertionUrn);
        datahubJob
            .getExtraMcps()
            .add(
                OpenLineageMappingUtils.toMcp(
                    assertionUrn, ASSERTION_ENTITY_TYPE, assertionRunEvent));
      }
    }
  }

  static Urn schemaFieldUrn(DatasetUrn datasetUrn, String fieldPath) {
    return UrnUtils.getUrn("urn:li:schemaField:(" + datasetUrn + "," + fieldPath + ")");
  }

  static DataProcessInstanceRunEvent getExtractionErrorRunEvent(OpenLineage.RunEvent event) {
    if (event.getRun().getFacets() == null
        || event.getRun().getFacets().getExtractionError() == null) {
      return null;
    }
    OpenLineage.ExtractionErrorRunFacet extractionError =
        event.getRun().getFacets().getExtractionError();
    boolean hasFailures =
        (extractionError.getFailedTasks() != null && extractionError.getFailedTasks() > 0)
            || (extractionError.getErrors() != null && !extractionError.getErrors().isEmpty());
    if (!hasFailures) {
      return null;
    }

    DataProcessInstanceRunEvent dataProcessInstanceRunEvent = new DataProcessInstanceRunEvent();
    dataProcessInstanceRunEvent.setTimestampMillis(
        event.getEventTime() != null
            ? event.getEventTime().toInstant().toEpochMilli()
            : System.currentTimeMillis());
    dataProcessInstanceRunEvent.setStatus(DataProcessRunStatus.COMPLETE);
    dataProcessInstanceRunEvent.setResult(
        new DataProcessInstanceRunResult()
            .setType(RunResultType.FAILURE)
            .setNativeResultType("EXTRACTION_ERROR"));
    return dataProcessInstanceRunEvent;
  }

  static DataProcessInstanceRunEvent processDataProcessInstanceResult(OpenLineage.RunEvent event) {
    DataProcessInstanceRunEvent facetFailure = getFacetFailureRunEvent(event);
    if (facetFailure != null) {
      return facetFailure;
    }
    if (event.getEventType() == null
        || event.getEventType() == OpenLineage.RunEvent.EventType.OTHER) {
      return null;
    }

    DataProcessInstanceRunEvent dataProcessInstanceRunEvent = new DataProcessInstanceRunEvent();
    if (event.getEventTime() != null) {
      dataProcessInstanceRunEvent.setTimestampMillis(
          event.getEventTime().toInstant().toEpochMilli());
    }

    DataProcessInstanceRunResult result = new DataProcessInstanceRunResult();
    switch (event.getEventType()) {
      case COMPLETE:
        dataProcessInstanceRunEvent.setStatus(DataProcessRunStatus.COMPLETE);
        result.setType(RunResultType.SUCCESS);
        result.setNativeResultType(event.getEventType().toString());
        dataProcessInstanceRunEvent.setResult(result);
        break;
      case FAIL:
      case ABORT:
        dataProcessInstanceRunEvent.setStatus(DataProcessRunStatus.COMPLETE);
        result.setType(RunResultType.FAILURE);
        result.setNativeResultType(event.getEventType().toString());
        dataProcessInstanceRunEvent.setResult(result);
        break;
      case START:
      case RUNNING:
        dataProcessInstanceRunEvent.setStatus(DataProcessRunStatus.STARTED);
        break;
      default:
        return null;
    }
    return dataProcessInstanceRunEvent;
  }

  static DataProcessInstanceRunEvent getFacetFailureRunEvent(OpenLineage.RunEvent event) {
    DataProcessInstanceRunEvent extractionFailure = getExtractionErrorRunEvent(event);
    boolean hasErrorMessage =
        event.getRun().getFacets() != null
            && event.getRun().getFacets().getErrorMessage() != null
            && ((event.getRun().getFacets().getErrorMessage().getMessage() != null
                    && !event.getRun().getFacets().getErrorMessage().getMessage().isBlank())
                || (event.getRun().getFacets().getErrorMessage().getStackTrace() != null
                    && !event.getRun().getFacets().getErrorMessage().getStackTrace().isBlank()));
    if (extractionFailure == null && !hasErrorMessage) {
      return null;
    }
    if (extractionFailure != null) {
      return extractionFailure;
    }
    return new DataProcessInstanceRunEvent()
        .setTimestampMillis(
            event.getEventTime() != null
                ? event.getEventTime().toInstant().toEpochMilli()
                : System.currentTimeMillis())
        .setStatus(DataProcessRunStatus.COMPLETE)
        .setResult(
            new DataProcessInstanceRunResult()
                .setType(RunResultType.FAILURE)
                .setNativeResultType("ERROR_MESSAGE"));
  }
}
